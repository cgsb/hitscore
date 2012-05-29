open Core.Std



open Hitscore_app_util

open Hitscore
open Flow

let get_or_make_sample_sheet ~dbh ~hsc ~kind ?force_new flowcell =
  Assemble_sample_sheet.run ~configuration:hsc ~kind ~dbh ?force_new flowcell
  >>= function
  | `new_failure (_, e) ->
    printf "NEW FAILURE\n";
    error e
  | `new_success f ->
    let layout = Classy.make dbh in
    printf "Assemble_sample_sheet.run succeeded\n";
    layout#assemble_sample_sheet#get f
    >>= fun ft ->
    begin match ft#g_result with
    | None -> error `assemble_sample_sheet_no_result
    | Some r -> return r#pointer
    end
      (*get ~dbh f >>= function
      | { g_result = None; _ } -> error `assemble_sample_sheet_no_result
      | { g_result = Some r } -> return r) *)
  | `previous_success (f, r) ->
    printf "Assemble_sample_sheet.run hit a previously ran assembly\n";
    return r

let get_hiseq_raw ~dbh s =
  let layout = Classy.make dbh in
  begin match String.split ~on:'_' (Filename.basename s) with
  | [_ ; _ ; _; almost_fc] ->
    layout#hiseq_raw#all
    >>| List.filter ~f:(fun hr -> hr#hiseq_dir_name = s)
    >>= fun search_result ->
    return (search_result, String.drop_prefix almost_fc 1)
  | _ ->
    begin 
      try
        let hst = Layout.Record_hiseq_raw.unsafe_cast (Int.of_string s) in
        layout#hiseq_raw#get hst
        >>= fun found ->
        (* get ~dbh hst >>= fun {flowcell_name; _} -> *)
        return ([found], found#flowcell_name)
      with
        e ->
          (layout#hiseq_raw#all
           >>| List.filter ~f:(fun hr -> hr#flowcell_name = s)
          (* (Layout.Search.record_hiseq_raw_by_flowcell_name ~dbh s *)
           >>= fun search_result ->
           return (search_result, s))
    end
  end
  >>= fun (search_result, flowcell) ->
  begin match search_result with
  | [one] ->
    printf "Found one hiseq-raw directory\n"; return (one#g_pointer, flowcell)
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
  let bases_mask = ref None in
  let options = pbs_options @ [
    ( "-tiles", Arg.String (fun s -> tiles := Some s),
      "<regexp list>\n\tSet the --tiles option of CASAVA (copied verbatim).");
    ( "-bases-mask", Arg.String (fun s -> bases_mask := Some s),
      "<string>\n\tSet the --use-bases-mask option of CASAVA (copied verbatim).");
    ( "-all-barcodes",
      Arg.Unit (fun () -> sample_sheet_kind := `all_barcodes),
      "\n\tUse/create an all-barcodes sample-sheet.");
    ( "-no-demux",
      Arg.Unit (fun () -> sample_sheet_kind := `no_demultiplexing),
      "\n\tUse/create a 'no demultiplexing' sample-sheet.");
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
             !hitscore_command, !make_command, !tiles, !bases_mask)
    with
    | Arg.Bad b -> `bad b
    | Arg.Help h -> `help h
  end

let start hsc prefix cl_args =
  begin match (start_parse_cmdline prefix cl_args) with
  | `go (args, kind, force_new, user, queue, nodes, ppn,
         wall_hours, version, mismatch, 
         hitscore_command, make_command, tiles, bases_mask) ->
    db_connect hsc
    >>= fun dbh ->
    begin match args with
    | [flowcell_or_dir_or_id] ->
      get_hiseq_raw ~dbh flowcell_or_dir_or_id
      >>= fun (hiseq_dir, flowcell) ->
      get_or_make_sample_sheet ~dbh ~hsc ~kind ~force_new flowcell
      >>= fun sample_sheet ->
      Bcl_to_fastq.start ~dbh ~make_command ~configuration:hsc ?tiles
        ~sample_sheet ~hiseq_dir ~hitscore_command ?bases_mask
        ?user ~nodes ~ppn ?queue ~wall_hours ~version ~mismatch
        (sprintf "%s_%s" flowcell Time.(now() |! to_filename_string))
      >>= fun started ->
      begin match started with
      | `success s -> 
        printf "Evaluation %d started.\n" s.Layout.Function_bcl_to_fastq.id;
        return ()
      | `failure (s, e) ->
        printf "Evaluation %d FAILED to START.\n" 
          s.Layout.Function_bcl_to_fastq.id;
        error e
      end
    | [] -> printf "Don't know what to do without arguments.\n"; return ()
    | l ->
      printf "Don't know what to do with %d arguments%s.\n"
        (List.length l)
        (sprintf ": [%s]" (String.concat ~sep:", " l)); return ()
    end
    >>= fun () ->
    db_disconnect hsc dbh
  | `help h ->
    printf "%s" h; return ()
  | `bad b ->
    eprintf "%s" b; return ()
  end


let register_success hsc id = 
  let bcl_to_fastq =
    try Some (Layout.Function_bcl_to_fastq.unsafe_cast (Int.of_string id)) 
    with e -> None in
  begin match bcl_to_fastq with
  | Some bcl_to_fastq ->
    let work =
      with_database hsc (fun ~dbh ->
        let layout = Classy.make dbh in
        layout#bcl_to_fastq#get bcl_to_fastq
        >>= fun b2f ->
        begin
          if b2f#g_status = `Started then return ()
          else error (`bcl_to_fastq_not_started b2f#g_status)
        end
        >>= fun () ->
        Bcl_to_fastq.succeed ~dbh ~bcl_to_fastq ~configuration:hsc)
    in
    double_bind work
      ~ok:(function
      | (`success s) ->
        printf "\nThis is a success: %d\n\n" s.Layout.Function_bcl_to_fastq.id;
        return ()
      | (`failure (_, e)) ->
        printf "\nThis is actually a failure.\n";
        error e)
      ~error:(fun e ->
        printf "\nThere have been errors.\n";
        error e)          
  | None ->
    eprintf "ERROR: bcl-to-fastq evaluation must be an integer.\n";
    error (`invalid_command_line (sprintf "%S is not an integer" id))
  end

let register_failure ?reason hsc id =
  let bcl_to_fastq =
    try Some (Layout.Function_bcl_to_fastq.unsafe_cast (Int.of_string id)) 
    with e -> None in
  begin match bcl_to_fastq with
  | Some bcl_to_fastq ->
    with_database hsc (fun ~dbh ->
      let layout = Classy.make dbh in
      layout#bcl_to_fastq#get bcl_to_fastq
      >>= fun b2f ->
      begin
        if b2f#g_status = `Started then return ()
        else error (`bcl_to_fastq_not_started b2f#g_status)
      end
      >>= fun () ->
      Bcl_to_fastq.fail ~dbh ?reason bcl_to_fastq)
    >>= fun _ ->
    return ()
  | None ->
    eprintf "ERROR: bcl-to-fastq evaluation must be an integer.\n";
    error (`invalid_command_line (sprintf "%S is not an integer" id))
  end

let check_status ?(fix_it=false) hsc id =
  let bcl_to_fastq =
    try Some (Layout.Function_bcl_to_fastq.unsafe_cast (Int.of_string id)) 
    with e -> None in
  begin match bcl_to_fastq with
  | Some bcl_to_fastq ->
    with_database hsc (fun ~dbh ->
      Bcl_to_fastq.status ~dbh ~configuration:hsc bcl_to_fastq
      >>= function
      | `running ->
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
        return ())
  | None ->
    eprintf "ERROR: bcl-to-fastq evaluation must be an integer.\n";
    error (`invalid_command_line (sprintf "%S is not an integer" id))
  end
    
let kill hsc id =
  let bcl_to_fastq =
    try Some (Layout.Function_bcl_to_fastq.unsafe_cast (Int.of_string id)) 
    with e -> None in
  begin match bcl_to_fastq with
  | Some bcl_to_fastq ->
    with_database hsc (fun ~dbh ->
      let layout = Classy.make dbh in
      layout#bcl_to_fastq#get bcl_to_fastq
      >>= fun b2f ->
      begin
        if b2f#g_status = `Started then return ()
        else error (`bcl_to_fastq_not_started b2f#g_status)
      end
      >>= fun () ->
      Bcl_to_fastq.kill ~dbh ~configuration:hsc bcl_to_fastq
      >>= fun _ ->
      return ())
  | None ->
    eprintf "ERROR: bcl-to-fastq evaluation must be an integer.\n";
    error (`invalid_command_line (sprintf "%S is not an integer" id))
  end
