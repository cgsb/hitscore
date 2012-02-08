
module type ASSEMBLE_SAMPLE_SHEET = sig

  module Configuration : Hitscore_interfaces.CONFIGURATION
  module Result_IO : Hitscore_interfaces.RESULT_IO
  module ACL : Hitscore_interfaces.ACL
    with module Result_IO = Result_IO
    with module Configuration = Configuration
  module Layout: Hitscore_layout_interface.LAYOUT
    with module Result_IO = Result_IO
    with type 'a PGOCaml.monad = 'a Result_IO.IO.t

  (** Assemble a sample-sheet:
{[
  Hitscore_lwt.Assemble_sample_sheet.run
    ~kind:`all_barcodes ~dbh
    ~note:"Documentation of Sample-sheet assembly"
    "D03M4ACXX"

    ~write_to_tmp:(fun s ->
      Lwt_io.(wrap_io 
                (with_file ~mode:output tmp_file)
                (fun chan -> fprintf chan "%s" s)))

    ~mv_from_tmp:(fun volpath filepath ->
      ksprintf shell_command "mv %s %s/%s/%s" tmp_file root volpath filepath)
]}
If any step fails (e.g. the shell command)
 but the assemble_sample_sheet evaluation has been created, it will be
 set as failed, but (for now) the file-system won't be "corrected".

  *)
  val run :
    dbh:Layout.db_handle ->
    kind:Layout.Enumeration_sample_sheet_kind.t ->
    configuration:ACL.Configuration.local_configuration ->
    ?note:string ->
    write_to_file:(file:string ->
                   content:string ->
                   (unit, [> `root_directory_not_configured ] as 'a)
                     Result_IO.monad) ->
    run_command:(string -> (unit, 'a) ACL.Result_IO.monad) ->
    string ->
    ([ `new_failure of
        [ `can_nothing ]
          Layout.Function_assemble_sample_sheet.pointer * 'a
     | `new_success of
         [ `can_get_result ]
           Layout.Function_assemble_sample_sheet.pointer
     | `previous_success of
         [ `can_get_result ]
           Layout.Function_assemble_sample_sheet.pointer *
           Layout.Record_sample_sheet.pointer ],
            [> `barcode_not_found of
                int32 * Layout.Enumeration_barcode_provider.t
            | `fatal_error of [> `trees_to_unix_paths_should_return_one ]
            | `layout_inconsistency of
                [> `file_system
                | `function_assemble_sample_sheet
                | `record_flowcell
                | `record_input_library
                | `record_lane
                | `record_log
                | `record_sample_sheet
                | `record_stock_library ] *
                  [> `add_did_not_return_one of string * int32 list
                  | `insert_did_not_return_one_id of string * int32 list
                  | `search_by_name_not_unique of
                      (int32 * int32 array) list
                  | `select_did_not_return_one_cache of string * int
                  | `select_did_not_return_one_tuple of string * int
                  | `successful_status_with_no_result of int32 ]
            | `pg_exn of exn
            | `wrong_request of
                [> `record_flowcell ] * [> `value_not_found of string ] ])
      Result_IO.monad
end
