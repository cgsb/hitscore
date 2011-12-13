
(** The module to generate sample sheets.  *)
module Make :
  functor
    (Result_IO : Hitscore_result_IO.RESULT_IO) ->
      functor (Layout: module type of Hitscore_db_access.Make(Result_IO)) -> 
sig
  
  type sample_sheet
    
  (** Prepare a sample-sheet.  *)
  val preparation: 
    ?kind:Layout.Enumeration_sample_sheet_kind.t ->
    dbh:Layout.db_handle ->
    string ->
    (sample_sheet,
     [> `io_exn of exn
     | `layout_inconsistency of
         [> `record_flowcell
         | `record_input_library
         | `record_lane
         | `record_stock_library ] *
           [> `search_by_name_not_unique of (int32 * int32 array) list
           | `select_did_not_return_one_cache of string * int ]
     | `barcode_not_found of int32 * Layout.Enumeration_barcode_provider.t
     | `pg_exn of exn ]) Result_IO.monad
      
  val output: sample_sheet ->
    (string -> ('a, 'b) Result_IO.monad) ->
    ('a, 'b) Result_IO.monad

  val get_target_file :
    dbh:Layout.db_handle ->
    sample_sheet ->
    (Layout.File_system.volume * string * string,
     [> `fatal_error of [> `trees_to_unix_paths_should_return_one ]
     | `layout_inconsistency of
         [> `file_system ] *
           [> `add_did_not_return_one of string * int32 list
           | `select_did_not_return_one_cache of string * int ]
     | `pg_exn of exn ])
      Result_IO.monad
      
  val register_with_success :
    dbh:Layout.db_handle ->
    ?note:string ->
    file:Layout.File_system.volume ->
    sample_sheet ->
    ([ `can_get_result ] Layout.Function_assemble_sample_sheet.t,
     [> `layout_inconsistency of
         [> `function_assemble_sample_sheet | `record_sample_sheet ] *
           [> `insert_did_not_return_one_id of string * int32 list ]
     | `pg_exn of exn ])
      Result_IO.monad
      
  val run :
    dbh:(string, bool) Batteries.Hashtbl.t Layout.PGOCaml.t ->
    kind:Layout.Enumeration_sample_sheet_kind.t ->
    ?note:string ->
    write_to_tmp:(string ->
                  (unit,
                   [> `barcode_not_found of
                       int32 * Layout.Enumeration_barcode_provider.t
                   | `fatal_error of
                       [> `trees_to_unix_paths_should_return_one ]
                   | `layout_inconsistency of
                       [> `file_system
                       | `function_assemble_sample_sheet
                       | `record_flowcell
                       | `record_input_library
                       | `record_lane
                       | `record_sample_sheet
                       | `record_stock_library ] *
                         [> `add_did_not_return_one of
                             string * int32 list
                         | `insert_did_not_return_one_id of
                             string * int32 list
                         | `search_by_name_not_unique of
                             (int32 * Layout.PGOCaml.int32_array) list
                         | `select_did_not_return_one_cache of
                             string * int ]
                   | `pg_exn of exn ]
                     as 'a)
                    Result_IO.monad) ->
    mv_from_tmp:(string -> string -> (unit, 'a) Result_IO.monad) ->
    string ->
    ([ `can_get_result ] Layout.Function_assemble_sample_sheet.t, 'a)
      Result_IO.monad



end
