
(** The module to run CASAVA's demultiplexer.  *)
module Make :
  functor (Configuration : Hitscore_interfaces.CONFIGURATION) ->
  functor (Result_IO : Hitscore_interfaces.RESULT_IO) ->
  functor (ACL : Hitscore_interfaces.ACL
           with module Result_IO = Result_IO
           with module Configuration = Configuration) ->
  functor (Layout: module type of Hitscore_db_access.Make(Result_IO)) -> 
sig

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
        | `record_sample_sheet ] *
          [> `insert_did_not_return_one_id of string * int32 list
          | `select_did_not_return_one_tuple of string * int ]
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
      | `record_log ] *
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
