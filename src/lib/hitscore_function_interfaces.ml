(** The interfaces of the function-implementations.  *)


(** The sample-sheet assembly function. *)
module type ASSEMBLE_SAMPLE_SHEET = sig

(**/**)
  module Common : Hitscore_common.COMMON
  open Common
(**/**)

  (** Run the whole function to assemble a sample-sheet. Example:
      {[
      run ~dbh ~kind:`specific_barcodes ~configuration "FC11IDXXX" >>= function
      | `new_success pointer -> ...
      | `previous_success (fun_pointer, result_pointer) -> ...
      | `new_failure (failed_fun_pointer, reason) -> ...
      ]}.
      If [~force_new] is [true] the run does not check existing assemblies.
  *)

  val run :
    dbh:(string, bool) Batteries.Hashtbl.t Common.Layout.PGOCaml.t ->
    kind:Common.Layout.Enumeration_sample_sheet_kind.t ->
    configuration:Common.Configuration.local_configuration ->
    ?force_new:bool ->
    ?note:string ->
    string ->
    ([> `new_failure of
        [ `can_nothing ]
          Common.Layout.Function_assemble_sample_sheet.pointer *
          [> `layout_inconsistency of
              [> `File_system
              | `Function of string
              | `Record of string ] *
                [> `insert_did_not_return_one_id of string * int32 list
                | `select_did_not_return_one_tuple of string * int ]
          | `pg_exn of exn
          | `root_directory_not_configured
          | `system_command_error of string * exn
          | `write_file_error of string * string * exn ]
     | `new_success of
         [ `can_get_result ]
           Common.Layout.Function_assemble_sample_sheet.pointer
     | `previous_success of
         [ `can_get_result ]
           Common.Layout.Function_assemble_sample_sheet.pointer *
           Common.Layout.Record_sample_sheet.pointer ],
     [> `barcode_not_found of
         int32 * Common.Layout.Enumeration_barcode_provider.t
     | `fatal_error of
         [> `add_volume_did_not_create_a_tree_volume of
             Common.Layout.File_system.pointer
         | `trees_to_unix_paths_should_return_one ]
     | `layout_inconsistency of
         [> `File_system | `Function of string | `Record of string ] *
           [> `add_did_not_return_one of string * int32 list
           | `insert_did_not_return_one_id of string * int32 list
           | `search_flowcell_by_name_not_unique of
               (string * (int32 * Common.Layout.PGOCaml.int32_array) list)
           | `select_did_not_return_one_tuple of string * int
           | `successful_status_with_no_result of int32 ]
     | `pg_exn of exn
     | `wrong_request of
         [> `record_flowcell ] * [> `value_not_found of string ] ])
      Common.Result_IO.monad

end


(** The module to run CASAVA's demultiplexer.  *)
module type BCL_TO_FASTQ = sig

(**/**)
  module Common : Hitscore_common.COMMON
  open Common
(**/**)

           
  (** Start the demultiplexer. Example:
{[
start ~dbh ~configuration       (* common arguments *)
      ~sample_sheet
      ~hiseq_raw
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
| `failure (failed_evaluation_pointer, reason) -> ...
]} 

     There, the return type can be seen as
     [ ([ `success | `failure_after ], `failure_before) monad]
     [`failure (_, _)] represents failures caught nicely
     (the function has been set as failed), and the error part of the
     monad are other other errors (before starting the function, or
     while setting it as failed -- unlikely).
*)
  val start :
    dbh:Common.Layout.db_handle ->
    configuration:Common.Configuration.local_configuration ->
    sample_sheet:Common.Layout.Record_sample_sheet.pointer ->
    hiseq_dir:Common.Layout.Record_hiseq_raw.pointer ->
    ?tiles:string ->
    ?mismatch:[< `one | `two | `zero > `one ] ->
    ?version:[< `casava_181 | `casava_182 > `casava_182 ] ->
    ?user:string ->
    ?wall_hours:int ->
    ?nodes:int ->
    ?ppn:int ->
    ?queue:string ->
    ?hitscore_command:string ->
    ?make_command:string ->
    string ->
    ([> `failure of
        [ `can_nothing ] Common.Layout.Function_bcl_to_fastq.pointer *
          [> `layout_inconsistency of
              [> `File_system
              | `Function of string
              | `Record of string ] *
                [> `insert_did_not_return_one_id of string * int32 list
                | `select_did_not_return_one_tuple of string * int ]
          | `pg_exn of exn
          | `system_command_error of string * exn
          | `work_directory_not_configured
          | `write_file_error of string * string * exn ]
     | `success of
         [ `can_complete ]
           Common.Layout.Function_bcl_to_fastq.pointer ],
     [> `cannot_recognize_file_type of string
     | `empty_sample_sheet_volume of
         Common.Layout.File_system.pointer *
           Common.Layout.Record_sample_sheet.pointer
     | `hiseq_dir_deleted
     | `inconsistency_inode_not_found of int32
     | `layout_inconsistency of
         [> `File_system | `Function of string | `Record of string ] *
           [> `insert_did_not_return_one_id of string * int32 list
           | `no_last_modified_timestamp of
               Common.Layout.Record_inaccessible_hiseq_raw.pointer
           | `select_did_not_return_one_tuple of string * int ]
     | `more_than_one_file_in_sample_sheet_volume of
         Common.Layout.File_system.pointer *
           Common.Layout.Record_sample_sheet.pointer * string list
     | `pg_exn of exn
     | `raw_data_path_not_configured
     | `root_directory_not_configured ])
      Common.Result_IO.monad

  (** Create the resulting [Layout.Record_bcl_to_fastq.t] and register
      the [bcl_to_fastq] evaluation as a success.

      This function should be called only by
      [hitscore <profile> register-success] which itself should be called
      only at the end of the PBS script generated by the {!start} function.
  *)
  val succeed :
    dbh:Common.Layout.db_handle ->
    configuration:Common.Configuration.local_configuration ->
    bcl_to_fastq:[> `can_complete ]
      Common.Layout.Function_bcl_to_fastq.pointer ->
    ([> `failure of
        [ `can_nothing ] Common.Layout.Function_bcl_to_fastq.pointer *
          [> `layout_inconsistency of
              [> `File_system
              | `Function of string
              | `Record of string ] *
                [> `insert_did_not_return_one_id of string * int32 list
                | `select_did_not_return_one_tuple of string * int ]
          | `pg_exn of exn
          | `system_command_error of string * exn ]
     | `success of
         [ `can_get_result ]
           Common.Layout.Function_bcl_to_fastq.pointer ],
     [> `cannot_recognize_file_type of string
     | `inconsistency_inode_not_found of int32
     | `layout_inconsistency of
         [> `File_system | `Function of string | `Record of string ] *
           [> `add_did_not_return_one of string * int32 list
           | `insert_did_not_return_one_id of string * int32 list
           | `select_did_not_return_one_tuple of string * int ]
     | `pg_exn of exn
     | `root_directory_not_configured
     | `system_command_error of string * exn
     | `work_directory_not_configured ])
      Common.Result_IO.monad

  (** Register the evaluation as failed with a optional reason to add
      to the [log] (record). *)
  val fail :
    dbh:Common.Layout.db_handle ->
    ?reason:string ->
    [> `can_complete ] Common.Layout.Function_bcl_to_fastq.pointer ->
    ([ `can_nothing ] Common.Layout.Function_bcl_to_fastq.pointer,
     [> `layout_inconsistency of
         [> `File_system | `Function of string | `Record of string ] *
           [> `insert_did_not_return_one_id of string * int32 list ]
     | `pg_exn of exn ])
      Common.Result_IO.monad
      
  (** Get the status of the evaluation by checking its data-base
      status and it presence in the PBS queue. *)
  val status :
    dbh:Common.Layout.db_handle ->
    configuration:Common.Configuration.local_configuration ->
    'a Common.Layout.Function_bcl_to_fastq.pointer ->
    ([ `not_started of Common.Layout.Enumeration_process_status.t
     | `running
     | `started_but_not_running of
         [ `system_command_error of string * exn ] ],
     [> `layout_inconsistency of
         [> `File_system | `Function of string | `Record of string ] *
           [> `select_did_not_return_one_tuple of string * int ]
     | `pg_exn of exn
     | `work_directory_not_configured ])
      Common.Result_IO.monad
      
  (** Kill the evaluation ([qdel]) and set it as failed. *)
  val kill :
    dbh:Common.Layout.db_handle ->
    configuration:Common.Configuration.local_configuration ->
    [> `can_complete ] Common.Layout.Function_bcl_to_fastq.pointer ->
    ([ `can_nothing ] Common.Layout.Function_bcl_to_fastq.pointer,
     [> `layout_inconsistency of
         [> `File_system | `Function of string | `Record of string ] *
           [> `insert_did_not_return_one_id of string * int32 list
           | `select_did_not_return_one_tuple of string * int ]
     | `not_started of Common.Layout.Enumeration_process_status.t
     | `pg_exn of exn
     | `system_command_error of string * exn
     | `work_directory_not_configured ])
      Common.Result_IO.monad

end

(** The module corresponding to [prepare_unaligned_delivery] in the {i
Layout}.  *)
module type UNALIGNED_DELIVERY = sig

(**/**)
  module Common : Hitscore_common.COMMON
  open Common
(**/**)

  (** Do the actual preparation of the delivery. We pass the
      [~bcl_to_fastq] function for practical reasons (it the id used
      everywhere else); the [?directory_tag] is the prefix of the
      directory name (the default begin the data of the current day; and
      [~destination] is a directory path. *)
  val run :
    dbh:Layout.db_handle ->
    configuration:Configuration.local_configuration ->
    ?directory_tag:string ->
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
                 [> `File_system | `Record of string | `Function of string ] *
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
             | `root_directory_not_configured
             | `wrong_unaligned_volume of string Hitscore_std.List.t ])
           Result_IO.monad



end
  
(** Deletion of intensity files. *)
module type DELETE_INTENSITIES = sig
    
(**/**)
  module Common : Hitscore_common.COMMON
  open Common
(**/**)

  (** Register a successful deletion of intensity files and create the
      new Hiseq_raw record. *)
  val register :
    dbh:Common.Layout.db_handle ->
    hiseq_raw:Common.Layout.Record_hiseq_raw.pointer ->
    ([ `can_get_result ]
        Common.Layout.Function_delete_intensities.pointer,
     [> `hiseq_dir_deleted
     | `layout_inconsistency of
         [> `File_system | `Function of string | `Record of string ] *
           [> `insert_did_not_return_one_id of string * int32 list
           | `no_last_modified_timestamp of
               Common.Layout.Record_inaccessible_hiseq_raw.pointer
           | `select_did_not_return_one_tuple of string * int ]
     | `pg_exn of exn ])
      Common.Result_IO.monad

end


(** The module to create “untyped” fastq directories.  *)
module type COERCE_B2F_UNALIGNED = sig

(**/**)
  module Common : Hitscore_common.COMMON
  open Common
(**/**)

  
  (** Start *)
  val run :
    dbh:Layout.db_handle ->
    configuration:Configuration.local_configuration ->
    input:Layout.Record_bcl_to_fastq_unaligned.pointer ->
    ([ `can_get_result ]
        Common.Layout.Function_coerce_b2f_unaligned.pointer,
     [> `layout_inconsistency of
         [> `File_system | `Record of string | `Function of string ] *
           [> `add_did_not_return_one of string * int32 list
           | `insert_did_not_return_one_id of string * int32 list
           | `select_did_not_return_one_tuple of string * int ]
     | `pg_exn of exn ])
      Common.Result_IO.monad

end


(** The function to run fastx on a given set of fastq files.  *)
module type FASTX_QUALITY_STATS = sig

(**/**)
  module Common : Hitscore_common.COMMON
  open Common
(**/**)

  
  (** Start *)
  val start :
    dbh:Layout.db_handle ->
    configuration:Configuration.local_configuration ->
    ?option_Q:int ->
    ?filter_names:string ->
    ?user:string ->
    ?wall_hours:int ->
    ?nodes:int ->
    ?ppn:int ->
    ?queue:string ->
    ?hitscore_command:string ->
    ?make_command:string ->
    input_dir:Layout.Record_generic_fastqs.pointer ->
    ([ `can_complete ]
        Common.Layout.Function_fastx_quality_stats.pointer,
     [> `pg_exn of exn ])
      Common.Result_IO.monad


end
