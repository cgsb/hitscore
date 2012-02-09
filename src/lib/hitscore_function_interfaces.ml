(** The interfaces of the function-implementations.  *)


(** The sample-sheet assembly function. *)
module type ASSEMBLE_SAMPLE_SHEET = sig

(**/**)
  module Configuration : Hitscore_interfaces.CONFIGURATION
  module Result_IO : Hitscore_interfaces.RESULT_IO
  module ACL : Hitscore_acl.ACL
    with module Result_IO = Result_IO
    with module Configuration = Configuration
  module Layout: Hitscore_layout_interface.LAYOUT
    with module Result_IO = Result_IO
    with type 'a PGOCaml.monad = 'a Result_IO.IO.t
(**/**)

  (** Run the whole function to assemble a sample-sheet. *)
  val run :
    dbh:(string, bool) Batteries.Hashtbl.t Layout.PGOCaml.t ->
    kind:Layout.Enumeration_sample_sheet_kind.t ->
    configuration:Configuration.local_configuration ->
    ?note:string ->
    write_to_file:(file:string ->
                   content:string ->
                   (unit,
                    [> `layout_inconsistency of
                        [> `record_log | `record_person ] *
                          [> `insert_did_not_return_one_id of
                              string * int32 list
                          | `select_did_not_return_one_tuple of
                              string * int ]
                    | `pg_exn of exn
                    | `root_directory_not_configured ]
                      as 'a)
                     Result_IO.monad) ->
    run_command:(string -> (unit, 'a) ACL.Result_IO.monad) ->
    string ->
    ([> `new_failure of
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
               (int32 * Layout.PGOCaml.int32_array) list
           | `select_did_not_return_one_tuple of string * int
           | `successful_status_with_no_result of int32 ]
     | `pg_exn of exn
     | `wrong_request of
         [> `record_flowcell ] * [> `value_not_found of string ] ])
      Result_IO.monad

end


(** The module to run CASAVA's demultiplexer.  *)
module type BCL_TO_FASTQ = sig

(**/**)
  module Configuration : Hitscore_interfaces.CONFIGURATION
  module Result_IO : Hitscore_interfaces.RESULT_IO
  module ACL : Hitscore_acl.ACL
    with module Result_IO = Result_IO
    with module Configuration = Configuration
  module Layout: Hitscore_layout_interface.LAYOUT
    with module Result_IO = Result_IO
    with type 'a PGOCaml.monad = 'a Result_IO.IO.t
(**/**)

  (** The errors which may be 'added' by the [start] function. *)
    type 'a start_error = 
    [> `cannot_recognize_file_type of string
    | `empty_sample_sheet_volume of
        Layout.File_system.volume_pointer * Layout.Record_sample_sheet.pointer
    | `hiseq_dir_deleted of
        Layout.Record_hiseq_raw.pointer * Layout.Record_inaccessible_hiseq_raw.pointer
    | `inconsistency_inode_not_found of int32
    | `layout_inconsistency of
        [> `file_system
        | `function_bcl_to_fastq
        | `record_hiseq_raw
        | `record_inaccessible_hiseq_raw
        | `record_log
        | `record_person
        | `record_sample_sheet ] *
          [> `insert_did_not_return_one_id of
              string * int32 list
          | `select_did_not_return_one_tuple of
              string * int ]
    | `more_than_one_file_in_sample_sheet_volume of
        Layout.File_system.volume_pointer * 
          Layout.Record_sample_sheet.pointer * string list
    | `pg_exn of exn 
    | `root_directory_not_configured
    | `work_directory_not_configured
    ] as 'a

  (** Start the demultiplexer. *)
  val start :
    dbh:Layout.db_handle ->
    configuration:Configuration.local_configuration ->
    sample_sheet:Layout.Record_sample_sheet.pointer ->
    hiseq_dir:Layout.Record_hiseq_raw.pointer ->
    availability:Layout.Record_inaccessible_hiseq_raw.pointer ->
    ?tiles:string ->
    ?mismatch:[ `one | `two | `zero ] ->
    ?version:[ `casava_181 | `casava_182 ] ->
    ?user:string -> ?wall_hours:int -> ?nodes:int -> ?ppn:int -> ?queue:string ->
    ?hitscore_command:string -> ?make_command:string ->
    run_command:(string -> (unit, 'a start_error) Result_IO.monad) ->
    write_file:(string -> string -> (unit, 'a start_error) Result_IO.monad) ->
    string ->
    ([ `failure of
        [ `can_nothing ] Layout.Function_bcl_to_fastq.pointer * 'a start_error
     | `success of [ `can_complete ] Layout.Function_bcl_to_fastq.pointer ],
     'a) Result_IO.monad


  (** The errors which may be 'added' by the [succeed] function. *)
  type 'a succeed_error = 'a constraint 'a =
  [> `layout_inconsistency of
      [> `file_system
      | `record_bcl_to_fastq_unaligned
      | `record_log | `record_person] *
        [> `add_did_not_return_one of string * int32 list
        | `insert_did_not_return_one_id of string * int32 list
        | `select_did_not_return_one_tuple of string * int ]
  | `pg_exn of exn
  | `root_directory_not_configured]

  (** Create the resulting [Layout.Record_bcl_to_fastq.t] and register
      the [bcl_to_fastq] evaluation as a success. *)
  val succeed:
    dbh:Layout.db_handle ->
    configuration:Configuration.local_configuration ->
    bcl_to_fastq:[`can_complete] Layout.Function_bcl_to_fastq.pointer ->
    result_root:string ->
    run_command:(string -> (unit, 'a succeed_error) Result_IO.monad) ->
    ([ `failure of
        [ `can_nothing ] Layout.Function_bcl_to_fastq.pointer * 'a succeed_error
     | `success of [ `can_get_result ] Layout.Function_bcl_to_fastq.pointer],
     'a succeed_error) Result_IO.monad

  (** Register the evaluation as failed. *)
  val fail:
    dbh:Layout.db_handle ->
    ?reason:string ->
    [> `can_complete] Layout.Function_bcl_to_fastq.pointer ->
    ([ `can_nothing ] Layout.Function_bcl_to_fastq.pointer,
     [> `layout_inconsistency of
         [> `record_log ] * [> `insert_did_not_return_one_id of string * int32 list ]
     | `pg_exn of exn ]) Result_IO.monad

  (** Get the status of the evaluation. *)
  val status:
    dbh:Layout.db_handle ->
    configuration:Configuration.local_configuration ->
    run_command:(string -> (unit, 'a) Result_IO.monad) ->
    'b Layout.Function_bcl_to_fastq.pointer ->
    ([ `running
     | `started_but_not_running of 'a
     | `not_started of Layout.Enumeration_process_status.t ],
     [> `layout_inconsistency of
         [> `function_bcl_to_fastq ] *
           [> `select_did_not_return_one_tuple of string * int ]
     | `pg_exn of exn
     | `status_parsing_error of string
     | `work_directory_not_configured ]) Result_IO.monad


  (** The errors which may be 'added' by the [kill] function. *)
  type 'a kill_error = 'a constraint 'a =
  [> `layout_inconsistency of
      [> `function_bcl_to_fastq | `record_log ] *
        [> `insert_did_not_return_one_id of
            string * int32 list
        | `select_did_not_return_one_tuple of
            string * int ]
  | `not_started of
      Layout.Enumeration_process_status.t
  | `pg_exn of exn
  | `work_directory_not_configured ]

    
  (** Kill the evaluation ([qdel]) and set it as failed. *)
  val kill :
    dbh:Layout.db_handle ->
    configuration:Configuration.local_configuration ->
    run_command:(string -> (unit, 'a kill_error) Result_IO.monad) ->
    [> `can_complete ] Layout.Function_bcl_to_fastq.pointer ->
    ([ `can_nothing ] Layout.Function_bcl_to_fastq.pointer, 'a kill_error)
      Result_IO.monad
      
end

