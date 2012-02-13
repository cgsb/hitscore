(** The interfaces of the function-implementations.  *)


(** The sample-sheet assembly function. *)
module type ASSEMBLE_SAMPLE_SHEET = sig

(**/**)
  module Configuration : Hitscore_interfaces.CONFIGURATION
  module Result_IO : Hitscore_interfaces.RESULT_IO
  module Access_rights : Hitscore_access_rights.ACCESS_RIGHTS
    with module Result_IO = Result_IO
    with module Configuration = Configuration
  module Layout: Hitscore_layout_interface.LAYOUT
    with module Result_IO = Result_IO
    with type 'a PGOCaml.monad = 'a Result_IO.IO.t
(**/**)

  (** Run the whole function to assemble a sample-sheet. Example:
      {[
run ~dbh ~kind:`specific_barcodes ~configuration "FC11IDXXX" >>= function
| `new_success pointer -> ...
| `previous_success (fun_pointer, result_pointer) -> ...
| `new_failure (failed_fun_pointer, reason) -> ...
      ]}. *)
  val run :
    dbh:(string, bool) Batteries.Hashtbl.t Layout.PGOCaml.t ->
    kind:Layout.Enumeration_sample_sheet_kind.t ->
    configuration:Configuration.local_configuration ->
    ?note:string ->
    string ->
    ([ `new_failure of
        [ `can_nothing ]
          Layout.Function_assemble_sample_sheet.pointer *
          [> `layout_inconsistency of
              [> `record_log | `record_person ] *
                [> `insert_did_not_return_one_id of string * int32 list
                | `select_did_not_return_one_tuple of string * int ]
          | `pg_exn of exn
          | `root_directory_not_configured
          | `system_command_error of string * exn
          | `write_file_error of string * string * exn ]
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
  module Access_rights : Hitscore_access_rights.ACCESS_RIGHTS
    with module Result_IO = Result_IO
    with module Configuration = Configuration
  module Layout: Hitscore_layout_interface.LAYOUT
    with module Result_IO = Result_IO
    with type 'a PGOCaml.monad = 'a Result_IO.IO.t
  (**/**)

           
  (** Start the demultiplexer. Example:
{[
start ~dbh ~configuration       (* common arguments *)
      ~sample_sheet
      ~hiseq_raw ~availability
      ~tiles:"s_1_11,s_[2-6]_1"
      ~mismatch:`zero           (* `one is the default *)
      ~version:`casava_181      (* `casava_182 is the default *)
      ~user:"ab42" 
      ~wall_hours ~nodes ~ppn ~queue             (* -> PBS options *)
      ~hitscore_command:"/usr/bin/hitscore-0.8"  (* For use inside the PBS-script *)
      ~make_command:"/opt/bin/gmake"
      "B2F_run_2012-12-21_M0"                    (* A 'name' given to the run *)
>>= function
| `success started_evaluation_pointer -> ...
| `failure failed_evaluation_pointer -> ...
]} 
*)
  val start :
    dbh:Layout.db_handle ->
    configuration:Configuration.local_configuration ->
    sample_sheet:Layout.Record_sample_sheet.pointer ->
    hiseq_dir:Layout.Record_hiseq_raw.pointer ->
    availability:Layout.Record_inaccessible_hiseq_raw.pointer ->
    ?tiles:string ->
    ?mismatch:[ `one | `two | `zero ] ->
    ?version:[`casava_181 | `casava_182 ] ->
    ?user:string ->
    ?wall_hours:int ->
    ?nodes:int ->
    ?ppn:int ->
    ?queue:string ->
    ?hitscore_command:string ->
    ?make_command:string ->
    string ->
    ([ `failure of
        [ `can_nothing ] Layout.Function_bcl_to_fastq.pointer *
          [> `layout_inconsistency of
              [> `record_log | `record_person ] *
                [> `insert_did_not_return_one_id of string * int32 list
                | `select_did_not_return_one_tuple of string * int ]
                  | `pg_exn of exn
                  | `system_command_error of string * exn
                  | `write_file_error of string * string * exn ]
     | `success of
                 [ `can_complete ] Layout.Function_bcl_to_fastq.pointer ],
     [> `cannot_recognize_file_type of string
     | `empty_sample_sheet_volume of
         Layout.File_system.volume_pointer *
           Layout.Record_sample_sheet.pointer
     | `hiseq_dir_deleted of
         Layout.Record_hiseq_raw.pointer *
           Layout.Record_inaccessible_hiseq_raw.pointer
     | `inconsistency_inode_not_found of int32
     | `layout_inconsistency of
         [> `file_system
         | `function_bcl_to_fastq
         | `record_hiseq_raw
         | `record_inaccessible_hiseq_raw
         | `record_log
         | `record_person
         | `record_sample_sheet ] *
           [> `insert_did_not_return_one_id of string * int32 list
           | `select_did_not_return_one_tuple of string * int ]
     | `more_than_one_file_in_sample_sheet_volume of
         Layout.File_system.volume_pointer *
                 Layout.Record_sample_sheet.pointer * string list
     | `pg_exn of exn
     | `root_directory_not_configured
     | `system_command_error of string * exn
     | `work_directory_not_configured ])
      Result_IO.monad

  (** Create the resulting [Layout.Record_bcl_to_fastq.t] and register
      the [bcl_to_fastq] evaluation as a success.

      This function should be called only by
      [hitscore <profile> register-success] which itself should be called
      only at the end of the PBS script generated by the {!start} function.
  *)
  val succeed :
    dbh:Layout.db_handle ->
    configuration:Configuration.local_configuration ->
    bcl_to_fastq:[> `can_complete ]
      Layout.Function_bcl_to_fastq.pointer ->
    result_root:string ->
    ([ `failure of
        [ `can_nothing ] Layout.Function_bcl_to_fastq.pointer *
                 [> `layout_inconsistency of
                     [> `record_log | `record_person ] *
                       [> `insert_did_not_return_one_id of string * int32 list
                       | `select_did_not_return_one_tuple of string * int ]
                 | `pg_exn of exn
                 | `root_directory_not_configured
                 | `system_command_error of string * exn ]
     | `success of
         [ `can_get_result ] Layout.Function_bcl_to_fastq.pointer ],
     [> `layout_inconsistency of
         [> `file_system
         | `record_bcl_to_fastq_unaligned
         | `record_log ] *
           [> `add_did_not_return_one of string * int32 list
           | `insert_did_not_return_one_id of string * int32 list
           | `select_did_not_return_one_tuple of string * int ]
     | `pg_exn of exn ]) Result_IO.monad

  (** Register the evaluation as failed with a optional reason to add
      to the [log] (record). *)
  val fail :
    dbh:Layout.db_handle ->
    ?reason:string ->
    [> `can_complete ] Layout.Function_bcl_to_fastq.pointer ->
    ([ `can_nothing ] Layout.Function_bcl_to_fastq.pointer,
     [> `layout_inconsistency of
         [> `record_log ] *
           [> `insert_did_not_return_one_id of string * int32 list ]
     | `pg_exn of exn ])
      Result_IO.monad
      
  (** Get the status of the evaluation by checking its data-base
      status and it presence in the PBS queue. *)
  val status :
           dbh:Layout.db_handle ->
           configuration:Configuration.local_configuration ->
           'a Layout.Function_bcl_to_fastq.pointer ->
    ([ `not_started of Layout.Enumeration_process_status.t
     | `running
     | `started_but_not_running of [ `system_command_error of string * exn ] ],
     [> `layout_inconsistency of
         [> `function_bcl_to_fastq ] *
           [> `select_did_not_return_one_tuple of string * int ]
     | `pg_exn of exn
     | `work_directory_not_configured ]) Result_IO.monad

                               
  (** Kill the evaluation ([qdel]) and set it as failed. *)
  val kill :
           dbh:Layout.db_handle ->
           configuration:Configuration.local_configuration ->
           [> `can_complete ] Layout.Function_bcl_to_fastq.pointer ->
           ([ `can_nothing ] Layout.Function_bcl_to_fastq.pointer,
            [> `layout_inconsistency of
                 [> `function_bcl_to_fastq | `record_log ] *
                 [> `insert_did_not_return_one_id of string * int32 list
                  | `select_did_not_return_one_tuple of string * int ]
             | `not_started of Layout.Enumeration_process_status.t
             | `pg_exn of exn
             | `system_command_error of string * exn
             | `work_directory_not_configured ])
           Result_IO.monad
                                                         
end

(** The module corresponding to [prepare_unaligned_delivery] in the {i
Layout}.  *)
module type UNALIGNED_DELIVERY = sig

(**/**)
  module Configuration : Hitscore_interfaces.CONFIGURATION
  module Result_IO : Hitscore_interfaces.RESULT_IO
  module Access_rights : Hitscore_access_rights.ACCESS_RIGHTS
    with module Result_IO = Result_IO
    with module Configuration = Configuration
  module Layout: Hitscore_layout_interface.LAYOUT
    with module Result_IO = Result_IO
    with type 'a PGOCaml.monad = 'a Result_IO.IO.t
  (**/**)

        val run :
           dbh:Layout.db_handle ->
           configuration:Configuration.local_configuration ->
           bcl_to_fastq:'a Layout.Function_bcl_to_fastq.pointer ->
           invoice:Layout.Record_invoicing.pointer ->
           destination:string ->
           ([ `can_get_result ]
            Layout.Function_prepare_unaligned_delivery.pointer,
            [> `bcl_to_fastq_not_succeeded of
                 'a Layout.Function_bcl_to_fastq.pointer *
                 Layout.Enumeration_process_status.t
             | `cannot_recognize_file_type of string
             | `inconsistency_inode_not_found of int32
             | `io_exn of exn
             | `layout_inconsistency of
                 [> `file_system
                  | `function_bcl_to_fastq
                  | `function_prepare_unaligned_delivery
                  | `record_bcl_to_fastq_unaligned
                  | `record_client_fastqs_dir
                  | `record_flowcell
                  | `record_invoicing
                  | `record_lane
                  | `record_log
                  | `record_person ] *
                 [> `insert_did_not_return_one_id of string * int32 list
                  | `select_did_not_return_one_tuple of string * int ]
             | `not_single_flowcell of
                 (string * int Hitscore_std.List.t) Hitscore_std.List.t
             | `partially_found_lanes of
                 int32 * string *
                 Layout.Record_lane.pointer Hitscore_std.Array.container *
                 int option list
             | `pg_exn of exn
             | `system_command_error of string * exn
             | `work_directory_not_configured
             | `wrong_unaligned_volume of string Hitscore_std.List.t ])
           Result_IO.monad



end
