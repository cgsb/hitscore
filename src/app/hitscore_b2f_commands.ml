open Core.Std



open Hitscore_app_util

open Hitscore_threaded
open Result_IO

let get_or_make_sample_sheet ~dbh ~hsc ~kind ?force_new flowcell =
  Assemble_sample_sheet.run ~configuration:hsc ~kind ~dbh ?force_new flowcell
  >>= function
  | `new_failure (_, e) ->
    printf "NEW FAILURE\n";
    error e
  | `new_success f ->
    printf "Assemble_sample_sheet.run succeeded\n";
    Layout.Function_assemble_sample_sheet.(
      get ~dbh f >>= function
      | { g_result = None; _ } -> error `assemble_sample_sheet_no_result
      | { g_result = Some r } -> return r)
  | `previous_success (f, r) ->
    printf "Assemble_sample_sheet.run hit a previously ran assembly\n";
    return r

let get_hiseq_raw ~dbh s =
  begin match String.split ~on:'_' (Filename.basename s) with
  | [_ ; _ ; _; almost_fc] ->
    Layout.Search.record_hiseq_raw_by_hiseq_dir_name ~dbh s
    >>= fun search_result ->
    return (search_result, String.drop_prefix almost_fc 1)
  | _ ->
    begin 
      try
        Layout.Record_hiseq_raw.(
          let hst = unsafe_cast (Int32.of_string s) in
          get ~dbh hst >>= fun {flowcell_name; _} ->
          return ([hst], flowcell_name))
      with
        e ->
          (Layout.Search.record_hiseq_raw_by_flowcell_name ~dbh s
           >>= fun search_result ->
           return (search_result, s))
    end
  end
  >>= fun (search_result, flowcell) ->
  begin match search_result with
  | [one] ->
    printf "Found one hiseq-raw directory\n"; return (one, flowcell)
  | [] ->
    error (`cant_find_hiseq_raw_for_flowcell flowcell)
  | more ->
    error (`found_more_than_one_hiseq_raw_for_flowcell 
              (List.length more, flowcell))
  end

let start_parse_cmdline usage_prefix args =
  let (user, queue, nodes, ppn, wall_hours, hitscore_command, pbs_options) =
    pbs_related_command_line_options ~default_wall_hours:48 () in
  let sample_sheet_kind = ref `specific_barcodes in
  let make_command = ref "make -j8" in
  let version, mismatch = ref `casava_182, ref `one in
  let tiles = ref None in
  let force_new = ref false in
  let options = pbs_options @ [
    ( "-tiles", Arg.String (fun s -> tiles := Some s),
      "<regexp list>\n\tSet the --tiles option of CASAVA (copied verbatim).");
    ( "-all-barcodes",
      Arg.Unit (fun () -> sample_sheet_kind := `all_barcodes),
      "\n\tUse/create an all-barcodes sample-sheet.");
    ( "-force-new-samplesheet",
      Arg.Unit (fun () -> force_new := true),
      "\n\tDo not check for existing sample-sheets (Default: false).");
    ( "-casava-181",
      Arg.Unit (fun () -> version := `casava_181),
      "\n\tRun with old version of CASAVA: 1.8.1.");
    ( "-mismatch-zero",
      Arg.Unit (fun s -> mismatch := `zero), "\n\tRun with mismatch 0.");
    ( "-mismatch-two",
      Arg.Unit (fun s -> mismatch := `two), "\n\tRun with mismatch 2.");
    ( "-make",
      Arg.Set_string make_command,
      sprintf "<command>\n\tSet the 'make' command used by the PBS-script \
            (default: %S)." !make_command)
  ] in
  let anon_args = ref [] in
  let anon s = anon_args := s :: !anon_args in
  let usage = 
    sprintf "Usage: %s [OPTIONS] <search-flowcell-run>\n\
        \  Where <search-flowcell-run> can be either a flowcell name, a HiSeq \n\
        \    directory path or a DB-identifier in the 'hiseq_raw' table\n\
        \  Options:" usage_prefix in
  let cmdline = Array.of_list (usage_prefix :: args) in
  begin 
    try Arg.parse_argv cmdline options anon usage;
        `go (List.rev !anon_args, !sample_sheet_kind, !force_new,
             !user, !queue, !nodes, !ppn,
             !wall_hours, !version, !mismatch,
             !hitscore_command, !make_command, !tiles)
    with
    | Arg.Bad b -> `bad b
    | Arg.Help h -> `help h
  end

let start hsc prefix cl_args =
  let open Hitscore_threaded in
  begin match (start_parse_cmdline prefix cl_args) with
  | `go (args, kind, force_new, user, queue, nodes, ppn,
         wall_hours, version, mismatch, 
         hitscore_command, make_command, tiles) ->
    db_connect hsc
    >>= fun dbh ->
    begin match args with
    | [flowcell_or_dir_or_id] ->
      get_hiseq_raw ~dbh flowcell_or_dir_or_id
      >>= fun (hiseq_dir, flowcell) ->
      get_or_make_sample_sheet ~dbh ~hsc ~kind ~force_new flowcell
      >>= fun sample_sheet ->
      Bcl_to_fastq.start ~dbh ~make_command ~configuration:hsc ?tiles
        ~sample_sheet ~hiseq_dir ~hitscore_command
        ?user ~nodes ~ppn ?queue ~wall_hours ~version ~mismatch
        (sprintf "%s_%s" flowcell Time.(now() |! to_filename_string))
      >>= fun started ->
      begin match started with
      | `success s -> 
        printf "Evaluation %ld started.\n" s.Layout.Function_bcl_to_fastq.id;
        return ()
      | `failure (s, e) ->
        printf "Evaluation %ld FAILED to START.\n" 
          s.Layout.Function_bcl_to_fastq.id;
        error e
      end
    | [] -> printf "Don't know what to do without arguments.\n"; return ()
    | l ->
      printf "Don't know what to do with %d arguments%s.\n"
        (List.length l)
        (sprintf ": [%s]" (String.concat ~sep:", " l)); return ()
    end
      |! display_errors;
    db_disconnect hsc dbh
  | `help h ->
    printf "%s" h; Ok ()
  | `bad b ->
    eprintf "%s" b; Ok ()
  end
    |! Result.ok


let register_success hsc id = 
  let open Hitscore_threaded in
  let bcl_to_fastq =
    try Some (Layout.Function_bcl_to_fastq.unsafe_cast (Int32.of_string id)) 
    with e -> None in
  begin match bcl_to_fastq with
  | Some bcl_to_fastq ->
    let work =
      db_connect hsc
      >>= fun dbh ->
      Layout.Function_bcl_to_fastq.(
        get ~dbh bcl_to_fastq >>= function
        | { g_status = `Started; } as e -> return e
        | { g_status } -> error (`not_started g_status))
      >>= fun _ ->
      Bcl_to_fastq.succeed ~dbh ~bcl_to_fastq ~configuration:hsc
    in
    begin match work with
    | Ok (`success s) ->
      printf "\nThis is a success: %ld\n\n" s.Layout.Function_bcl_to_fastq.id
    | Ok (`failure (_, e)) ->
      printf "\nThis is actually a failure:\n";
      display_errors (Error e)
    | Error  e ->
      printf "\nThere have been errors:\n";
      display_errors (Error e)          
    end;
    Some ()
  | None ->
    eprintf "ERROR: bcl-to-fastq evaluation must be an integer.\n"; None
  end

let register_failure ?reason hsc id =
  let open Hitscore_threaded in
  let bcl_to_fastq =
    try Some (Layout.Function_bcl_to_fastq.unsafe_cast (Int32.of_string id)) 
    with e -> None in
  begin match bcl_to_fastq with
  | Some bcl_to_fastq ->
    let work =
      db_connect hsc >>= fun dbh -> 
      Layout.Function_bcl_to_fastq.(
        get ~dbh bcl_to_fastq  >>= function
        | { g_status = `Started; } as e -> return e
        | { g_status } -> error (`not_started g_status))
      >>= fun _ ->
      Bcl_to_fastq.fail ~dbh ?reason bcl_to_fastq in
    display_errors work;
    Some ()
  | None ->
    eprintf "ERROR: bcl-to-fastq evaluation must be an integer.\n"; None
  end

let check_status ?(fix_it=false) hsc id =
  let open Hitscore_threaded in
  let bcl_to_fastq =
    try Some (Layout.Function_bcl_to_fastq.unsafe_cast (Int32.of_string id)) 
    with e -> None in
  begin match bcl_to_fastq with
  | Some bcl_to_fastq ->
    let work =
      db_connect hsc >>= fun dbh -> 
      Bcl_to_fastq.status ~dbh ~configuration:hsc bcl_to_fastq
      >>= function
      |  `running ->
        printf "The function is STILL RUNNING.\n"; 
        return ()
      | `started_but_not_running e -> 
        printf "The function is STARTED BUT NOT RUNNING!!!\n";
        if fix_it then (
          printf "Fixing …\n";
          Bcl_to_fastq.fail ~dbh bcl_to_fastq
            ~reason:"checking_status_reported_started_but_not_running"
          >>= fun _ -> return ())
        else 
          return ()
      | `not_started e ->
        printf "The function is NOT STARTED: %S.\n"
          (Layout.Enumeration_process_status.to_string e);
        return ()
    in
    display_errors work;
    Some ()
  | None ->
    eprintf "ERROR: bcl-to-fastq evaluation must be an integer.\n"; None
  end
    
let kill hsc id =
  let bcl_to_fastq =
    try Some (Layout.Function_bcl_to_fastq.unsafe_cast (Int32.of_string id)) 
    with e -> None in
  begin match bcl_to_fastq with
  | Some bcl_to_fastq ->
    let work =
      db_connect hsc >>= fun dbh -> 
      Layout.Function_bcl_to_fastq.(
        get ~dbh bcl_to_fastq  >>= function
        | { g_status = `Started; } as e -> return e
        | { g_status } -> error (`not_started g_status))
      >>= fun _ ->
      Bcl_to_fastq.kill ~dbh ~configuration:hsc bcl_to_fastq in
    display_errors work;
    Some ()
  | None ->
    eprintf "ERROR: bcl-to-fastq evaluation must be an integer.\n"; None
  end

let info hsc id_or_dir = 
  let dir = 
    let bcl_to_fastq =
      try Some (Layout.Function_bcl_to_fastq.unsafe_cast 
                  (Int32.of_string id_or_dir)) 
      with e -> None in
    begin match bcl_to_fastq with
    | Some bcl_to_fastq ->
      let work = 
        db_connect hsc >>= fun dbh -> 
        Layout.Function_bcl_to_fastq.(
          get ~dbh bcl_to_fastq  >>= function
          | { g_status = `Succeeded; g_result = Some s; raw_data} -> 
            return (raw_data, s)
          | { g_status } -> error (`not_started g_status))
        >>= fun (hsraw, b2fu) ->
        Layout.Record_hiseq_raw.(
          get ~dbh hsraw >>= fun { flowcell_name } -> return flowcell_name)
        >>= fun fcid ->
        Layout.Record_bcl_to_fastq_unaligned.(
          get ~dbh b2fu >>= fun { directory } -> return directory)
        >>= fun vol ->
        Common.all_paths_of_volume ~dbh ~configuration:hsc vol
        >>= (function
        | [ unaligned ] -> return  unaligned
        | l -> error (`there_is_more_than_unaligned l))
        >>= fun unaligned ->
        return (Filename.concat unaligned (sprintf "Basecall_Stats_%s" fcid))
      in
      display_errors work;
      let udir = Result.ok_exn work in
      begin match Configuration.path_of_volume_fun hsc with
      | Some f -> f udir
      | None -> failwith "Root directory not configured"
      end
    | None -> id_or_dir
    end
  in
  let dmux_sum = Filename.concat dir "Flowcell_demux_summary.xml" in
  let xml = 
    In_channel.with_file dmux_sum ~f:(fun ic ->
      XML.(make_input (`Channel ic) |! in_tree)) in
  
  let demux_summary = 
    match B2F_unaligned.flowcell_demux_summary (snd xml) with
    | Ok d -> d
    | Error (`parse_flowcell_demux_summary_error e) ->
      eprintf "Error while parsing %S: %s" dmux_sum (Exn.to_string e);
      failwith "B2F_commands.info"
  in

  Array.iteri demux_summary ~f:(fun i a ->
    if a <> [] then (
      printf "Lane %d:\n" (i + 1); 
      printf  " - Library Name -----\
               |- yield ------------\
               |- yield_q30 --------\
               |- cluster_count ----\
               |- cluster_count_m0 -\
               |- cluster_count_m1 -\
               |- quality_score_sum \
               |\n";
    );
    List.iter a (fun
      { Hitscore_interfaces.B2F_unaligned_information.name;
        yield;
        yield_q30;
        cluster_count;
        cluster_count_m0;
        cluster_count_m1;
        quality_score_sum; } ->
        let f2s o f = 
          let s = sprintf "%.0f" f in
          let rec f s =
            if String.(length s) > 3 then
              String.(f (drop_suffix s 3) ^ " " ^ suffix s 3)
            else
              s in
          fprintf o "% 18s" (f s) in
        printf "  % -18s | %a | %a | %a | %a | %a | %a |\n"
          name
          f2s yield
          f2s yield_q30
          f2s cluster_count
          f2s cluster_count_m0
          f2s cluster_count_m1
          f2s quality_score_sum 
    );
  );
  Some ()

