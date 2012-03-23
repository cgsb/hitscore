open Core.Std
  
module Hitscore_threaded = Hitscore.Make(Hitscore.Preemptive_threading_config)
open Hitscore_threaded
open Result_IO

module System = struct

  let command_exn s = 
    let status = Unix.system s in
    if not (Unix.Process_status.is_ok status) then
      ksprintf failwith "System.command_exn: %s" 
        (Unix.Process_status.to_string_hum status)
    else
      ()

  let command s =
    let status = Unix.system s in
    if (Unix.Process_status.is_ok status) then
      Ok ()
    else
      Error (`sys_error (`command (s, status)))

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
    sprintf "ERROR: Barcode not found: %ld (%s)\n" i
      (Layout.Enumeration_barcode_provider.to_string p)
  | `fatal_error  `trees_to_unix_paths_should_return_one ->
    sprintf "FATAL_ERROR: trees_to_unix_paths_should_return_one\n"
  | `io_exn e ->
    sprintf "IO-ERROR: %s\n" (Exn.to_string e)
  | `layout_inconsistency (where, what) ->
    let int32_list il = (List.map il Int32.to_string |! String.concat ~sep:", ") in
    sprintf "LAYOUT-INCONSISTENCY-ERROR for %s: %s!\n"
      (match where with
      | `Function s -> "Function " ^ s
      | `Record s -> "Record " ^ s
      | `File_system -> "File_system")
      (match what with
      | `insert_cache_did_not_return_one_id (s, il)
      | `insert_tuple_did_not_return_one_id (s, il)
      | `insert_value_did_not_return_one_id (s, il)
      | `insert_did_not_return_one_id (s, il)
      | `add_did_not_return_one             (s, il) ->
        sprintf "Wrong 'insert' did not return one only thing from the DB (%s, [%s])"
          s (int32_list il)
      | `select_did_not_return_one_tuple (s, i) ->
        sprintf "Wrong 'select' did not return one tuple: %s (%d)" s i

      | `no_last_modified_timestamp inaccessible_hiseq_raw_pointer ->
        sprintf "no_last_modified_timestamp for inaccessible_hiseq_raw_pointer: %ld"
          inaccessible_hiseq_raw_pointer.Layout.Record_inaccessible_hiseq_raw.id
      | `search_flowcell_by_name_not_unique (fcid, _) ->
        sprintf "search_flowcell_by_name_not_unique: %s" fcid
      | `successful_status_with_no_result id ->
        sprintf "successful_status_with_no_result: %ld" id)
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
  | `write_file_error (f,c,e) ->
    sprintf "SYS-FILE-ERROR: Write file error: %s\n" (Exn.to_string e)
  | `system_command_error (cmd, e) ->
    sprintf "SYS-CMD-ERROR: Command: %S --> %s\n" cmd (Exn.to_string e)
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

let display_errors = function
  | Ok _ -> ()
  | Error e ->
    printf "%s" (string_of_error e)

let result_io_ok_or_fail = function
  | Ok o -> o
  | Error e ->
    failwithf "%s" (string_of_error e) ()


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
    
let pbs_related_command_line_options () = 
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
  let wall_hours = ref 12 in
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
