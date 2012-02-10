open Core.Std

module Hitscore_threaded = Hitscore.Make(Hitscore.Preemptive_threading_config)

open Hitscore_threaded
open Result_IO

open Hitscore_app_util

let get_or_make_sample_sheet ~dbh ~hsc ~kind flowcell =
  Assemble_sample_sheet.run ~configuration:hsc ~kind ~dbh flowcell
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


let display_errors = function
  | Ok _ -> ()
  | Error e ->
    begin match e with
    | `barcode_not_found (i, p) ->
      printf "ERROR: Barcode not found: %ld (%s)\n" i
        (Layout.Enumeration_barcode_provider.to_string p)
    | `fatal_error  `trees_to_unix_paths_should_return_one ->
      printf "FATAL_ERROR: trees_to_unix_paths_should_return_one\n"
    | `io_exn e ->
      printf "IO-ERROR: %s\n" (Exn.to_string e)
    | `layout_inconsistency (_, _) ->
      printf "LAYOUT-INCONSISTENCY-ERROR!\n"
    | `new_failure (_, _) ->
      printf "NEW FAILURE\n"
    | `pg_exn e ->
      printf "PGOCaml-ERROR: %s\n" (Exn.to_string e)
    | `wrong_request (`record_flowcell, `value_not_found s) ->
      printf "WRONG-REQUEST: Record 'flowcell': value not found: %s\n" s
    | `cant_find_hiseq_raw_for_flowcell flowcell ->
      printf "Cannot find a HiSeq directory for that flowcell: %S\n" flowcell
    | `found_more_than_one_hiseq_raw_for_flowcell (nb, flowcell) ->
      printf "There are %d HiSeq directories for that flowcell: %S\n" nb flowcell
    | `hiseq_dir_deleted (_, _) ->
      printf "INVALID-REQUEST: The HiSeq directory is reported 'deleted'\n"
    | `cannot_recognize_file_type t ->
      printf "LAYOUT-INCONSISTENCY-ERROR: Unknown file-type: %S\n" t
    | `empty_sample_sheet_volume (v, s) ->
      printf "LAYOUT-INCONSISTENCY-ERROR: Empty sample-sheet volume\n"
    | `inconsistency_inode_not_found i32 ->
      printf "LAYOUT-INCONSISTENCY-ERROR: Inode not found: %ld\n" i32
    | `more_than_one_file_in_sample_sheet_volume (v,s,m) ->
      printf "LAYOUT-INCONSISTENCY-ERROR: More than one file in \
                sample-sheet volume:\n%s\n" (String.concat ~sep:"\n" m)
    | `root_directory_not_configured ->
      printf "INVALID-CONFIGURATION: Root directory not set.\n"
    | `work_directory_not_configured ->
      printf "INVALID-CONFIGURATION: Work directory not set.\n"
    | `write_file_error (f,c,e) ->
      printf "SYS-FILE-ERROR: Write file error: %s\n" (Exn.to_string e)
    | `system_command_error (cmd, e) ->
      printf "SYS-CMD-ERROR: Command: %S --> %s\n" cmd (Exn.to_string e)
    | `status_parsing_error s ->
      printf "LAYOUT-INCONSISTENCY-ERROR: Cannot parse function status: %s\n" s
    | `wrong_status s ->
      printf "INVALID-REQUEST: Function has wrong status: %s\n"
        (Layout.Enumeration_process_status.to_string s)
    | `not_started e ->
      printf "INVALID-REQUEST: The function is NOT STARTED: %S.\n"
        (Layout.Enumeration_process_status.to_string e);
    | `assemble_sample_sheet_no_result ->
      printf "LAYOUT-INCONSISTENCY: The sample-sheet assembly has no result\n"
    | `there_is_more_than_unaligned l ->
      printf "UNEXPECTED-LAYOUT: there is more than one unaligned directory\n";
      List.iter l (printf " * %S\n")
    end

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
  let user = ref (Sys.getenv "LOGNAME") in
  let queue = 
    let groups =
      System.command_to_string "groups" 
      |! String.split_on_chars ~on:[ ' '; '\t'; '\n' ] in
    match List.find groups ((=) "cgsb") with
    | Some _ -> ref (Some "cgsb-s")
    | None -> ref None in
  let nodes = ref 1 in
  let ppn = ref 8 in
  let sample_sheet_kind = ref `specific_barcodes in
  let make_command = ref "make -j8" in
  let wall_hours, version, mismatch =
    ref 12, ref `casava_182, ref `one in
  let hitscore_command =
    ref (sprintf "%s %s %s" 
           Sys.executable_name Sys.argv.(1) Sys.argv.(2)) in
  let tiles = ref None in
  let options = [
    ( "-tiles", Arg.String (fun s -> tiles := Some s),
      "<regexp list>\n\tSet the --tiles option of CASAVA (copied verbatim).");
    ( "-user", 
      Arg.String (fun s -> user := Some s),
      sprintf "<login>\n\tSet the user-name (%s)."
        (Option.value_map ~default:"kind-of mandatory" !user
           ~f:(sprintf "default value inferred: %s")));
    ( "-queue", 
      Arg.String (fun s -> queue := Some s),
      sprintf "<name>\n\tSet the PBS-queue%s." 
        (Option.value_map ~default:"" !queue 
           ~f:(sprintf " (default value inferred: %s)")));
    ( "-nodes-ppn", 
      Arg.Tuple [ Arg.Set_int nodes; Arg.Set_int ppn],
      sprintf "<n> <m>\n\tSet the number of nodes and processes per node \
                  (default %d, %d)." !nodes !ppn);
    ( "-all-barcodes",
      Arg.Unit (fun () -> sample_sheet_kind := `all_barcodes),
      "\n\tUse/create an all-barcodes sample-sheet.");
    ( "-wall-hours",
      Arg.Set_int wall_hours,
      sprintf "<hours>\n\tWalltime in hours (default: %d)." !wall_hours);
    ( "-casava-181",
      Arg.Unit (fun () -> version := `casava_181),
      "\n\tRun with old version of CASAVA: 1.8.1.");
    ( "-mismatch-zero",
      Arg.Unit (fun s -> mismatch := `zero), "\n\tRun with mismatch 0.");
    ( "-mismatch-two",
      Arg.Unit (fun s -> mismatch := `two), "\n\tRun with mismatch 2.");
    ( "-hitscore-command",
      Arg.Set_string hitscore_command,
      sprintf "<command>\n\tCommand-prefix to call to register success \
                 or failure of the run (default: %S)." !hitscore_command);
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
        `go (List.rev !anon_args, !sample_sheet_kind,
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
  | `go (args, kind, user, queue, nodes, ppn,
         wall_hours, version, mismatch, 
         hitscore_command, make_command, tiles) ->
    db_connect hsc
    >>= fun dbh ->
    begin match args with
    | [flowcell_or_dir_or_id] ->
      get_hiseq_raw ~dbh flowcell_or_dir_or_id
      >>= fun (hiseq_dir, flowcell) ->
      get_or_make_sample_sheet ~dbh ~hsc ~kind flowcell
      >>= fun sample_sheet ->
      Layout.Record_inaccessible_hiseq_raw.(
        get_all ~dbh
        >>= fun all ->
        of_list_sequential all ~f:(fun ihr_t ->
          get ~dbh ihr_t >>= fun { g_last_modified; _ } ->
          begin match g_last_modified with
          | Some t -> return (ihr_t, t)
          | None -> error (`layout_inconsistency 
                              (`record_inaccessible_hiseq_raw,
                               `no_last_modified_timestamp ihr_t))
          end)
        >>| List.sort ~cmp:(fun a b -> compare (snd b) (snd a))
        >>= function
        | [] ->
          printf "There were no inaccessible_hiseq_raw => \
                       creating the empty one.\n";
          Layout.Record_inaccessible_hiseq_raw.add_value ~dbh ~deleted:[| |]
        | (h, ts) :: t -> 
          printf "Last inaccessible_hiseq_raw: %ld on %s\n"
            h.Layout.Record_inaccessible_hiseq_raw.id
            (Time.to_string ts);
          return h)
      >>= fun availability ->
      Bcl_to_fastq.start ~dbh ~make_command ~configuration:hsc ?tiles
        ~sample_sheet ~hiseq_dir ~availability ~hitscore_command
        ?user ~nodes ~ppn ?queue ~wall_hours ~version ~mismatch
        (sprintf "%s_%s" flowcell Time.(now() |! to_filename_string))
        ~write_file:(fun s file ->
          wrap_io Out_channel.(with_file 
                                 ~f:(fun o -> output_string o s)) file)
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


let register_success hsc id result_root = 
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
      Bcl_to_fastq.succeed ~dbh ~bcl_to_fastq ~result_root ~configuration:hsc
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
        Layout.File_system.(
          get_volume ~dbh vol >>= fun volume ->
          let vol_path = entry_unix_path volume.volume_entry in
          of_result (volume_trees volume) >>| trees_to_unix_paths
          >>= function
          | [ unaligned ] -> return (Filename.concat vol_path unaligned)
          | l -> error (`there_is_more_than_unaligned l))
        >>= fun unaligned ->
        return (Filename.concat unaligned (sprintf "Basecall_Stats_%s" fcid))
      in
      display_errors work;
      let udir = Result.ok_exn work in
      begin match Configuration.volume_path_fun hsc with
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

