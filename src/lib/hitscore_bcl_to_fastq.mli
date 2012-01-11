
(** The module to run CASAVA's demultiplexer.  *)
module Make :
  functor
    (Result_IO : Hitscore_result_IO.RESULT_IO) ->
      functor (Layout: module type of Hitscore_db_access.Make(Result_IO)) -> 
sig

  type 'call_error todo_list = [
  | `Run of string
  | `Save of string * string
  | `Call of string * 
      (dbh:Layout.db_handle -> (unit, 'call_error) Result_IO.monad)
  ] list 

  val start :
    dbh:Layout.db_handle -> root:string ->
    sample_sheet:Layout.Record_sample_sheet.t ->
    hiseq_dir:Layout.Record_hiseq_raw.t ->
    availability:Layout.Record_inaccessible_hiseq_raw.t ->
    ?mismatch:[`zero | `one | `two ] ->
    ?version:[`casava_182 | `casava_181] ->
    ?user:string ->
    ?wall_hours:int ->
    ?nodes:int ->
    ?ppn:int ->
    ?work_dir:(user:string -> unique_id:string -> string) ->
    ?queue:string -> string ->
    ([> `layout_inconsistency of [> `function_bcl_to_fastq | `record_log ] *
        [> `insert_did_not_return_one_id of string * int32 list ]
     | `pg_exn of exn ] 
        todo_list,
     [> `cannot_recognize_file_type of string
     | `empty_sample_sheet_volume of
         Layout.File_system.volume * Layout.Record_sample_sheet.t
     | `hiseq_dir_deleted of
         Layout.Record_hiseq_raw.t *
           Layout.Record_inaccessible_hiseq_raw.t
     | `inconsistency_inode_not_found of int32
     | `layout_inconsistency of
         [> `file_system
         | `record_hiseq_raw
         | `record_inaccessible_hiseq_raw
         | `record_sample_sheet ] *
           [> `select_did_not_return_one_cache of string * int ]
     | `more_than_one_file_in_sample_sheet_volume of
         Layout.File_system.volume * Layout.Record_sample_sheet.t *
           string list
     | `pg_exn of exn ])
      Result_IO.monad
    


end
