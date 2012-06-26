open Core.Std
  
open Hitscore
open Flow

let result_ok_exn ?(fail=Failure "Not Ok") = function
  | Ok o -> o
  | Error _ -> raise fail
module System = struct
  exception Wrong_status of [ `Exit_non_zero of int | `Signal of Signal.t ]
  let command_exn s =
    begin match Unix.system s with
    | Ok () -> ()
    | Error e -> raise (Wrong_status e)
    end
  let command s = 
    begin match Unix.system s with
    | Ok () -> Ok ()
    | Error status ->
      Error (`sys_error (`command (s, status)))
    end
  (*
    if (Unix.Process_status.is_ok status) then
      Ok ()
    else
      Error (`sys_error (`command (s, status)))

*)
  let command_to_string s =
    Unix.open_process_in s |! In_channel.input_all
        
end

module XML = struct
  include Xmlm
  let in_tree i = 
    let el tag childs = `E (tag, childs)  in
    let data d = `D d in
    input_doc_tree ~el ~data i
end
let string_of_error = function
  | `barcode_not_found (i, p) ->
    sprintf "ERROR: Barcode not found: %d (%s)\n" i
      (Layout.Enumeration_barcode_provider.to_string p)
  | `fatal_error  `trees_to_unix_paths_should_return_one ->
    sprintf "FATAL_ERROR: trees_to_unix_paths_should_return_one\n"
  | `io_exn e ->
    sprintf "IO-ERROR: %s\n" (Exn.to_string e)
  | `cannot_find_delivery (p, l) ->
    sprintf "Cannot find the delivery for %s (%d results)\n" p l
  | `lane_not_found_in_any_flowcell lanep ->
    sprintf "Cannot find lane %d in any flowcell\n" lanep.Layout.Record_lane.id
  | `pbs_qstat e ->
    sprintf "error(s) while getting qstat info: %s"
      begin match e with
      | `errors l ->
        List.map l ~f:(function `wrong_line_format s -> sprintf "wrong line: %S" s)
        |! String.concat ~sep:", "
      | `job_state_not_found -> "job_state_not_found"
      | `no_header s -> sprintf "no header in %S" s
      | `unknown_status s -> sprintf "unknown status: %S" s
      | `wrong_header_format s -> sprintf "wrong_header_format: %S" s
      end
  | `new_failure (_, _) ->
    sprintf "NEW FAILURE\n"
  | `pg_exn e ->
    sprintf "PGOCaml-ERROR: %s\n" (Exn.to_string e)
  | `wrong_request (`record_flowcell, `value_not_found s) ->
    sprintf "WRONG-REQUEST: Record 'flowcell': value not found: %s\n" s
  | `cant_find_hiseq_raw_for_flowcell flowcell ->
    sprintf "Cannot find a HiSeq directory for that flowcell: %S\n" flowcell
  | `found_more_than_one_hiseq_raw_for_flowcell (nb, flowcell) ->
    sprintf "There are %d HiSeq directories for that flowcell: %S\n" nb flowcell
  | `hiseq_dir_deleted ->
    sprintf "INVALID-REQUEST: The HiSeq directory is reported 'deleted'\n"
  | `cannot_recognize_file_type t ->
    sprintf "LAYOUT-INCONSISTENCY-ERROR: Unknown file-type: %S\n" t
  | `empty_sample_sheet_volume (v, s) ->
    sprintf "LAYOUT-INCONSISTENCY-ERROR: Empty sample-sheet volume\n"
  | `inconsistency_inode_not_found i32 ->
    sprintf "LAYOUT-INCONSISTENCY-ERROR: Inode not found: %ld\n" i32
  | `more_than_one_file_in_sample_sheet_volume (v,s,m) ->
    sprintf "LAYOUT-INCONSISTENCY-ERROR: More than one file in \
                sample-sheet volume:\n%s\n" (String.concat ~sep:"\n" m)
  | `root_directory_not_configured ->
    sprintf "INVALID-CONFIGURATION: Root directory not set.\n"
  | `raw_data_path_not_configured ->
    sprintf "INVALID-CONFIGURATION: Raw-data path not set.\n"
  | `work_directory_not_configured ->
    sprintf "INVALID-CONFIGURATION: Work directory not set.\n"
  | `write_file_error (f,e) ->
    sprintf "SYS-FILE-ERROR: Write to %S error: %s\n" f (Exn.to_string e)
  | `system_command_error (cmd, e) ->
    sprintf "SYS-CMD-ERROR: Command: %S --> %s\n" cmd
      (match e with
      | `exited i -> sprintf "Exit %d" i
      | `exn e -> Exn.to_string e
      | `signaled i -> sprintf "Signal %d" i
      | `stopped i -> sprintf "Stopped %d" i)
  | `status_parsing_error s ->
    sprintf "LAYOUT-INCONSISTENCY-ERROR: Cannot parse function status: %s\n" s
  | `wrong_status s ->
    sprintf "INVALID-REQUEST: Function has wrong status: %s\n"
      (Layout.Enumeration_process_status.to_string s)
  | `not_started e ->
    sprintf "INVALID-REQUEST: The function is NOT STARTED: %S.\n"
      (Layout.Enumeration_process_status.to_string e)
  | `assemble_sample_sheet_no_result ->
    sprintf "LAYOUT-INCONSISTENCY: The sample-sheet assembly has no result\n"
  | `there_is_more_than_unaligned l ->
    sprintf "UNEXPECTED-LAYOUT: there is more than one unaligned directory:\n%s"
      (List.map l (sprintf " * %S\n") |! String.concat ~sep:"")
  | `fatal_error (`add_volume_did_not_create_a_tree_volume v) ->
    sprintf "DATABASE-ERROR: add_volume_did_not_create_a_tree_volume!\n"
  | `cannot_recognize_fastq_format file ->
    sprintf "cannot_recognize_fastq_format of %s" file
  | `file_path_not_in_volume (file, v) ->
    sprintf "file_path_not_in_volume (%s, %d)" file v.Layout.File_system.id
  | `duplicated_barcode s ->
    sprintf "Duplicated barcode: %S" s
  | `invalid_command_line s ->
    sprintf "Invalid command-line argument(s): %s" s
  | `bcl_to_fastq_not_started status ->
    sprintf "bcl_to_fastq_not_started but %S"
      (Hitscore.Layout.Enumeration_process_status.to_string status)
  | `bcl_to_fastq_not_succeeded (pointer, status) ->
    sprintf "bcl_to_fastq %d not succeeded but %S"
      pointer.Hitscore_layout.Layout.Function_bcl_to_fastq.id
      Hitscore_layout.Layout.Enumeration_process_status.(to_string status)
  | `not_single_flowcell l ->
    sprintf "Flowcell not unique: %s" 
      (String.concat ~sep:";" (List.map l (fun (s, ll) ->
        sprintf "%S: [%s]" s (String.concat ~sep:", "
                                (List.map ll (sprintf "%d"))))))
  | `partially_found_lanes (i, s) ->
    sprintf "partially_found_lanes %d %s" i s
  | `wrong_unaligned_volume sl ->
    sprintf "wrong_unaligned_volume [%s]" (String.concat ~sep:"; " sl)
  | `pss_failure s -> sprintf "pss_failure: %S" s
  | `layout_inconsistency (where, what) ->
    sprintf "LAYOUT-INCONSISTENCY (%s): %s"
      (match where with
      | `Dump -> "Dump"
      | `File_system -> "File-system"
      | `Function f -> sprintf  "function %S" f
      | `Record r -> sprintf "record %S" r) 
      (match what with
      | `search_flowcell_by_name_not_unique (s, i) ->
        sprintf "search_flowcell_by_name_not_unique %s %d" s i
      | `successful_status_with_no_result i ->
        sprintf "successful_status_with_no_result %d" i)
  | `db_backend_error e ->
    begin match e with
    | `exn e
    | `connection e
    | `disconnection e
    | `query  (_, e) -> 
      sprintf "DB BACKEND ERROR: %s" (Exn.to_string e)
    end
  | `Layout (where, what) ->
    sprintf "LAYOUT-ERROR (%s): %s"
      (match where with
      | `Dump -> "Dump"
      | `File_system -> "File-system"
      | `Function f -> sprintf  "function %S" f
      | `Record r -> sprintf "record %S" r) 
      (match what with
      | `db_backend_error (`query (q, e)) ->
        sprintf "Query %S failed: %s" q (Exn.to_string e)
      | `db_backend_error e ->
        begin match e with
        | `exn e
        | `connection e
        | `disconnection e
        | `query  (_, e) -> 
          sprintf "DB BACKEND ERROR: %s" (Exn.to_string e)
        end
      | `parse_evaluation_error (sol, e)
      | `parse_value_error (sol, e)
      | `parse_volume_error (sol, e) ->
        sprintf "error while parsing result [%s]: %s"
          (String.concat ~sep:", "
             (List.map sol (Option.value ~default:"NONE"))) (Exn.to_string e)
      | `parse_sexp_error (sexp, e) ->
        sprintf "S-Exp parsing error: %S: %s"
          (Sexp.to_string_hum sexp) (Exn.to_string e)
      | `result_not_unique soll ->
        sprintf "result_not_unique: [%s]"
          (String.concat ~sep:", "
             (List.map soll (fun sol ->
               String.concat ~sep:", "
                 (List.map sol (Option.value ~default:"NONE")))))
      | `wrong_add_value ->
        sprintf "WRONG-ADD-VALUE"
      | `wrong_version (v1, v2) ->
        sprintf "Wrong version: %s Vs %s" v1 v2)
        

(*
let display_errors = function
  | Ok _ -> ()
  | Error e ->
    printf "ERROR:\n  %s\n" (string_of_error e)

let flow_ok_or_fail = function
  | Ok o -> o
  | Error e ->
    failwithf "%s" (string_of_error e) ()
*)
(*
let pg_raw_query  ~dbh ~query =
  let module PG = Layout.PGOCaml in
  let name = "todo_change_this" in
  wrap_io (PG.prepare ~name ~query dbh) ()
  >>= fun () ->
  wrap_io (PG.execute ~name ~params:[] dbh) ()
  >>= fun result ->
  wrap_io (PG.close_statement dbh ~name) ()
  >>= fun () ->
  return result

let print_query_result query l =
  printf "query: %s\n  --> [%s]\n" query
    (String.concat ~sep:"; " (List.map l (fun l2 ->
      sprintf "(%s)"
        (String.concat ~sep:", " (List.map l2 (Option.value ~default:"—"))))))
*)  
let pbs_related_command_line_options
    ?(default_nodes=1) ?(default_ppn=8) ?(default_wall_hours=12) () = 
  let user = ref (Sys.getenv "LOGNAME") in
  let command_to_string s =
    Unix.open_process_in s |! In_channel.input_all in
  let queue = 
    let groups =
      command_to_string "groups" 
      |! String.split_on_chars ~on:[ ' '; '\t'; '\n' ] in
    match List.find groups ((=) "cgsb") with
    | Some _ -> ref (Some "cgsb-s")
    | None -> ref None in
  let nodes = ref default_nodes in
  let ppn = ref default_ppn in
  let wall_hours = ref default_wall_hours in
  let hitscore_command =
    ref (sprintf "%s %s %s" Sys.executable_name Sys.argv.(1) Sys.argv.(2)) in
  let options = [
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
    ( "-wall-hours",
      Arg.Set_int wall_hours,
      sprintf "<hours>\n\tWalltime in hours (default: %d)." !wall_hours);
    ( "-hitscore-command",
      Arg.Set_string hitscore_command,
      sprintf "<command>\n\tCommand-prefix to call to register success \
                 or failure of the run (default: %S)." !hitscore_command);
  ] in
  (user, queue, nodes, ppn, wall_hours, hitscore_command, options)
