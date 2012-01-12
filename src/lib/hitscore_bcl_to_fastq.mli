
(** The module to run CASAVA's demultiplexer.  *)
module Make :
  functor
    (Result_IO : Hitscore_result_IO.RESULT_IO) ->
      functor (Layout: module type of Hitscore_db_access.Make(Result_IO)) -> 
sig

  val start :
    dbh:Layout.db_handle ->
    volume_path:(string -> string) ->
    sample_sheet:Layout.Record_sample_sheet.t ->
    hiseq_dir:Layout.Record_hiseq_raw.t ->
    availability:Layout.Record_inaccessible_hiseq_raw.t ->
    ?mismatch:[< `one | `two | `zero > `one ] ->
    ?version:[< `casava_181 | `casava_182 > `casava_182 ] ->
    ?user:string ->
    ?wall_hours:int ->
    ?nodes:int ->
    ?ppn:int ->
    ?work_dir:(user:string -> unique_id:string -> string) ->
    ?queue:string ->
    ?hitscore_command:string ->
    ?make_command:string ->
    run_command:(string ->
                 (unit,
                  [> `cannot_recognize_file_type of string
                  | `empty_sample_sheet_volume of
                      Layout.File_system.volume *
                        Layout.Record_sample_sheet.t
                  | `hiseq_dir_deleted of
                      Layout.Record_hiseq_raw.t *
                        Layout.Record_inaccessible_hiseq_raw.t
                  | `inconsistency_inode_not_found of int32
                  | `layout_inconsistency of
                      [> `file_system
                      | `function_bcl_to_fastq
                      | `record_hiseq_raw
                      | `record_inaccessible_hiseq_raw
                      | `record_log
                      | `record_sample_sheet ] *
                        [> `insert_did_not_return_one_id of
                            string * int32 list
                        | `select_did_not_return_one_cache of
                            string * int ]
                  | `more_than_one_file_in_sample_sheet_volume of
                      Layout.File_system.volume *
                        Layout.Record_sample_sheet.t * string list
                  | `pg_exn of exn ]
                    as 'a)
                   Result_IO.monad) ->
    write_file:(string -> string -> (unit, 'a) Result_IO.monad) ->
    string ->
    ([ `can_complete ] Layout.Function_bcl_to_fastq.t, 'a)
      Result_IO.monad

  val succeed:
    dbh:Layout.db_handle ->
    bcl_to_fastq:[`can_complete] Layout.Function_bcl_to_fastq.t ->
    result_root:string ->
    mv_dir:(string -> string -> 
            (unit,
             [> `layout_inconsistency of
                 [> `file_system
                        | `record_bcl_to_fastq_unaligned
                        | `record_log ] *
                   [> `add_did_not_return_one of string * int32 list
                   | `insert_did_not_return_one_id of string * int32 list
                   | `select_did_not_return_one_cache of string * int ]
             | `pg_exn of exn ] as 'a) Result_IO.monad) ->
    ([ `failure of
        [ `can_nothing ] Layout.Function_bcl_to_fastq.t * 'a
     | `success of
         [ `can_get_result ] Layout.Function_bcl_to_fastq.t], 'a) Result_IO.monad

  val fail:
    dbh:Layout.db_handle ->
    ?reason:string ->
    [> `can_complete] Layout.Function_bcl_to_fastq.t ->
    ([ `can_nothing ] Layout.Function_bcl_to_fastq.t,
     [> `layout_inconsistency of
         [> `record_log ] * [> `insert_did_not_return_one_id of string * int32 list ]
     | `pg_exn of exn ]) Result_IO.monad


end
