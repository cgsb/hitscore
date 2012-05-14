(** The interfaces of the function-implementations.  *)

open Hitscore_std
open Hitscore_layout
open Hitscore_access_rights
open Hitscore_db_backend
open Hitscore_common
open Hitscore_configuration

(** The sample-sheet assembly function. *)
module type ASSEMBLE_SAMPLE_SHEET = sig

  val illumina_barcodes : (int * string) list
  val bioo_barcodes : (int * string) list
  
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
    dbh:Hitscore_db_backend.Backend.db_handle ->
    kind:Layout.Enumeration_sample_sheet_kind.t ->
    configuration:Configuration.local_configuration ->
    ?force_new:bool ->
    ?note:string ->
    string ->
    ([> `new_failure of
        Layout.Function_assemble_sample_sheet.pointer *
          [> `Layout of
              [> `Record of string ] *
                [> `db_backend_error of
                    [> `query of
                        Hitscore_db_backend.Sql_query.t * exn ]
                | `parse_sexp_error of Hitscore_std.Sexp.t * exn
                | `parse_value_error of string option list * exn
                | `wrong_add_value ]
          | `root_directory_not_configured
          | `system_command_error of string * exn
          | `write_file_error of string * string * exn ]
     | `new_success of
         Layout.Function_assemble_sample_sheet.pointer
     | `previous_success of
         Layout.Function_assemble_sample_sheet.pointer *
           Layout.Record_sample_sheet.pointer ],
     [> `Layout of
         [> `File_system | `Function of string | `Record of string ] *
           [> `db_backend_error of
               [> `query of Hitscore_db_backend.Sql_query.t * exn ]
           | `parse_evaluation_error of
               string Hitscore_std.Option.t list * exn
           | `parse_sexp_error of Hitscore_std.Sexp.t * exn
           | `parse_value_error of string option list * exn
           | `parse_volume_error of string option list * exn
           | `result_not_unique of
               Hitscore_db_backend.Backend.result
           | `wrong_add_value ]
     | `barcode_not_found of
         int *
           Layout.Enumeration_barcode_provider.t
               | `duplicated_barcode of string
               | `fatal_error of
                   [> `add_volume_did_not_create_a_tree_volume of
                        Layout.File_system.pointer
                    | `trees_to_unix_paths_should_return_one ]
               | `layout_inconsistency of
                   [> `Function of string | `Record of string ] *
                   [> `search_flowcell_by_name_not_unique of string * int
                    | `successful_status_with_no_result of int ]
               | `wrong_request of
                   [> `record_flowcell ] * [> `value_not_found of string ] ])
      Flow.monad
end


(** The module to run CASAVA's demultiplexer.  *)
module type BCL_TO_FASTQ = sig

           
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
             dbh:Hitscore_db_backend.Backend.db_handle ->
             configuration:Configuration.local_configuration ->
             sample_sheet:Layout.Record_sample_sheet.pointer ->
             hiseq_dir:Layout.Record_hiseq_raw.pointer ->
             ?tiles:string ->
             ?bases_mask:string ->
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
                   Layout.Function_bcl_to_fastq.pointer *
                   [> `Layout of
                        [> `Record of string ] *
                        [> `db_backend_error of
                             [> `query of
                                  Hitscore_db_backend.Sql_query.t * exn ]
                         | `parse_sexp_error of Hitscore_std.Sexp.t * exn
                         | `parse_value_error of string option list * exn
                         | `wrong_add_value ]
                    | `system_command_error of string * exn
                    | `work_directory_not_configured
                    | `write_file_error of string * string * exn ]
               | `success of
                   Layout.Function_bcl_to_fastq.pointer ],
              [> `Layout of
                   [> `File_system | `Function of string | `Record of string ] *
                   [> `db_backend_error of
                        [> `query of Hitscore_db_backend.Sql_query.t * exn ]
                    | `parse_sexp_error of Hitscore_std.Sexp.t * exn
                    | `parse_value_error of string option list * exn
                    | `parse_volume_error of string option list * exn
                    | `result_not_unique of
                        Hitscore_db_backend.Backend.result
                    | `wrong_add_value ]
               | `empty_sample_sheet_volume of
                   Layout.File_system.pointer *
                   Layout.Record_sample_sheet.pointer
               | `hiseq_dir_deleted
               | `more_than_one_file_in_sample_sheet_volume of
                   Layout.File_system.pointer *
                   Layout.Record_sample_sheet.pointer *
                   string List.t
               | `raw_data_path_not_configured
               | `root_directory_not_configured ])
             t

  (** Create the resulting [Layout.Record_bcl_to_fastq.t] and register
      the [bcl_to_fastq] evaluation as a success.

      This function should be called only by
      [hitscore <profile> register-success] which itself should be called
      only at the end of the PBS script generated by the {!start} function.
  *)
           val succeed :
             dbh:Hitscore_db_backend.Backend.db_handle ->
             configuration:Configuration.local_configuration ->
             bcl_to_fastq:Layout.Function_bcl_to_fastq.pointer ->
             ([> `failure of
                   Layout.Function_bcl_to_fastq.pointer *
                   [> `Layout of
                        [> `Record of string ] *
                        [> `db_backend_error of
                             [> `query of
                                  Hitscore_db_backend.Sql_query.t * exn ]
                         | `parse_sexp_error of Hitscore_std.Sexp.t * exn
                         | `parse_value_error of string option list * exn
                         | `wrong_add_value ]
                    | `system_command_error of string * exn ]
               | `success of
                   Layout.Function_bcl_to_fastq.pointer ],
              [> `Layout of
                   [> `File_system | `Function of string | `Record of string ] *
                   [> `db_backend_error of
                        [> `query of Hitscore_db_backend.Sql_query.t * exn ]
                    | `parse_evaluation_error of
                        string Hitscore_std.Option.t list * exn
                    | `parse_sexp_error of Hitscore_std.Sexp.t * exn
                    | `parse_value_error of string option list * exn
                    | `parse_volume_error of string option list * exn
                    | `result_not_unique of
                        Hitscore_db_backend.Backend.result
                    | `wrong_add_value ]
               | `root_directory_not_configured
               | `system_command_error of string * exn
               | `work_directory_not_configured ])
             t

  (** Register the evaluation as failed with a optional reason to add
      to the [log] (record). *)
           val fail :
             dbh:Hitscore_db_backend.Backend.db_handle ->
             ?reason:string ->
             Hitscore_layout.Layout.Function_bcl_to_fastq.pointer ->
             (Hitscore_layout.Layout.Function_bcl_to_fastq.pointer,
              [> `Layout of
                  [> `Function of string | `Record of string ] *
                    [> `db_backend_error of
                        [> `query of Hitscore_db_backend.Sql_query.t * exn ]
                    | `wrong_add_value ] ])
               t

  (** Get the status of the evaluation by checking its data-base
      status and it presence in the PBS queue. *)
           val status :
             dbh:Hitscore_db_backend.Backend.db_handle ->
             configuration:Configuration.local_configuration ->
             Hitscore_layout.Layout.Function_bcl_to_fastq.pointer ->
             ([> `not_started of
                   Layout.Enumeration_process_status.t
               | `running
               | `started_but_not_running of
                   [> `system_command_error of string * exn ] ],
              [> `Layout of
                   [> `Function of string ] *
                   [> `db_backend_error of
                        [> `query of Hitscore_db_backend.Sql_query.t * exn ]
                    | `parse_evaluation_error of
                        string Hitscore_std.Option.t list * exn
                    | `parse_sexp_error of Hitscore_std.Sexp.t * exn
                    | `result_not_unique of
                        Hitscore_db_backend.Backend.result ]
               | `work_directory_not_configured ])
             t

               
  (** Kill the evaluation ([qdel]) and set it as failed. *)
           val kill :
             dbh:Hitscore_db_backend.Backend.db_handle ->
             configuration:Configuration.local_configuration ->
             Hitscore_layout.Layout.Function_bcl_to_fastq.pointer ->
             (Hitscore_layout.Layout.Function_bcl_to_fastq.pointer,
              [> `Layout of
                   [> `Function of string | `Record of string ] *
                   [> `db_backend_error of
                        [> `query of Hitscore_db_backend.Sql_query.t * exn ]
                    | `parse_evaluation_error of
                        string Hitscore_std.Option.t list * exn
                    | `parse_sexp_error of Hitscore_std.Sexp.t * exn
                    | `result_not_unique of
                        Hitscore_db_backend.Backend.result
                    | `wrong_add_value ]
               | `not_started of
                   Layout.Enumeration_process_status.t
               | `system_command_error of string * exn
               | `work_directory_not_configured ])
             t

end

(** The module corresponding to [prepare_unaligned_delivery] in the {i
Layout}.  *)
module type UNALIGNED_DELIVERY = sig


  (** Do the actual preparation of the delivery. We pass the
      [~bcl_to_fastq] function for practical reasons (it the id used
      everywhere else); the [?directory_tag] is the prefix of the
      directory name (the default begin the data of the current day; and
      [~destination] is a directory path. *)
  val run :
    dbh:Hitscore_db_backend.Backend.db_handle ->
    configuration:Configuration.local_configuration ->
    ?directory_tag:string ->
    bcl_to_fastq:Layout.Function_bcl_to_fastq.pointer ->
    invoice:Layout.Record_invoicing.pointer ->
    destination:string ->
    (unit,
     [> `Layout of
         [> `File_system | `Function of string | `Record of string ] *
           [> `db_backend_error of
               [> `query of Hitscore_db_backend.Sql_query.t * exn ]
           | `parse_evaluation_error of
               string Hitscore_std.Option.t list * exn
           | `parse_sexp_error of Hitscore_std.Sexp.t * exn
           | `parse_value_error of string option list * exn
           | `parse_volume_error of string option list * exn
           | `result_not_unique of
               Hitscore_db_backend.Backend.result
           | `wrong_add_value ]
     | `bcl_to_fastq_not_succeeded of
         Layout.Function_bcl_to_fastq.pointer *
           Layout.Enumeration_process_status.t
     | `io_exn of exn
     | `not_single_flowcell of
         (string * int List.t)
           List.t
     | `partially_found_lanes of int * string
     | `root_directory_not_configured
     | `system_command_error of string * exn
     | `wrong_unaligned_volume of string List.t ])
      Flow.monad


end
  
(** Deletion of intensity files. *)
module type DELETE_INTENSITIES = sig
    
  (** Register a successful deletion of intensity files and create the
      new Hiseq_raw record. *)
  val register :
    dbh:Hitscore_db_backend.Backend.db_handle ->
    hiseq_raw:Layout.Record_hiseq_raw.pointer ->
    (Layout.Function_delete_intensities.pointer,
     [> `Layout of
         [> `Function of string | `Record of string ] *
           [> `db_backend_error of
               [> `query of Hitscore_db_backend.Sql_query.t * exn ]
           | `parse_sexp_error of Hitscore_std.Sexp.t * exn
           | `parse_value_error of string option list * exn
           | `result_not_unique of
               Hitscore_db_backend.Backend.result
           | `wrong_add_value ]
     | `hiseq_dir_deleted ])
      t

end


(** The module to create “untyped” fastq directories.  *)
module type COERCE_B2F_UNALIGNED = sig

  
  val run :
    dbh:Hitscore_db_backend.Backend.db_handle ->
    configuration:'a ->
    input:Layout.Record_bcl_to_fastq_unaligned.pointer ->
    (unit,
     [> `Layout of
         [> `File_system | `Function of string | `Record of string ] *
           [> `db_backend_error of
               [> `query of Hitscore_db_backend.Sql_query.t * exn ]
           | `parse_evaluation_error of
               string Hitscore_std.Option.t list * exn
           | `parse_sexp_error of Hitscore_std.Sexp.t * exn
           | `parse_value_error of string option list * exn
           | `result_not_unique of
               Hitscore_db_backend.Backend.result
           | `wrong_add_value ] ])
      t
end


(** The function to run fastx on a given set of fastq files.  *)
module type FASTX_QUALITY_STATS = sig


  (** Call fastx_quality_stats the “right” way. *)
           val call_fastx :
             dbh:Hitscore_db_backend.Backend.db_handle ->
             configuration:Configuration.local_configuration ->
             volume:Layout.File_system.pointer ->
             option_Q:int ->
             String.t ->
             string ->
             (unit,
              [> `Layout of
                   [> `File_system ] *
                   [> `db_backend_error of
                        [> `query of Hitscore_db_backend.Sql_query.t * exn ]
                    | `parse_sexp_error of Hitscore_std.Sexp.t * exn
                    | `parse_value_error of string option list * exn
                    | `parse_volume_error of string option list * exn
                    | `result_not_unique of
                        Hitscore_db_backend.Backend.result ]
               | `cannot_recognize_fastq_format of String.t
               | `file_path_not_in_volume of
                   String.t *
                   Layout.File_system.pointer
               | `root_directory_not_configured
               | `system_command_error of string * exn ])
             t
  
  (** Start *)
           val start :
             dbh:Hitscore_db_backend.Backend.db_handle ->
             configuration:Configuration.local_configuration ->
             ?option_Q:int ->
             ?filter_names:String.t List.t ->
             ?user:string ->
             ?wall_hours:int ->
             ?nodes:int ->
             ?ppn:int ->
             ?queue:string ->
             ?hitscore_command:string ->
             Layout.Record_generic_fastqs.pointer ->
             (Layout.Function_fastx_quality_stats.pointer,
              [> `Layout of
                   [> `File_system | `Function of string | `Record of string ] *
                   [> `db_backend_error of
                        [> `query of Hitscore_db_backend.Sql_query.t * exn ]
                    | `parse_sexp_error of Hitscore_std.Sexp.t * exn
                    | `parse_value_error of string option list * exn
                    | `parse_volume_error of string option list * exn
                    | `result_not_unique of
                        Hitscore_db_backend.Backend.result
                    | `wrong_add_value ]
               | `root_directory_not_configured
               | `system_command_error of string * exn
               | `work_directory_not_configured
               | `write_file_error of string * string * exn ])
             t


           val succeed :
             dbh:Hitscore_db_backend.Backend.db_handle ->
             configuration:Configuration.local_configuration ->
             Layout.Function_fastx_quality_stats.pointer ->
             ([> `failure of
                   Layout.Function_fastx_quality_stats.pointer *
                   [> `Layout of
                        [> `Record of string ] *
                        [> `db_backend_error of
                             [> `query of
                                  Hitscore_db_backend.Sql_query.t * exn ]
                         | `parse_sexp_error of Hitscore_std.Sexp.t * exn
                         | `parse_value_error of string option list * exn
                         | `wrong_add_value ]
                    | `system_command_error of string * exn ]
               | `success of
                   Layout.Function_fastx_quality_stats.pointer ],
              [> `Layout of
                   [> `File_system | `Function of string | `Record of string ] *
                   [> `db_backend_error of
                        [> `query of Hitscore_db_backend.Sql_query.t * exn ]
                    | `parse_evaluation_error of
                        string Hitscore_std.Option.t list * exn
                    | `parse_sexp_error of Hitscore_std.Sexp.t * exn
                    | `parse_value_error of string option list * exn
                    | `parse_volume_error of string option list * exn
                    | `result_not_unique of
                        Hitscore_db_backend.Backend.result
                    | `wrong_add_value ]
               | `root_directory_not_configured
               | `system_command_error of string * exn
               | `work_directory_not_configured ])
             t
      
  (** Register the evaluation as failed with a optional reason to add
      to the [log] (record). *)
           val fail :
             dbh:Hitscore_db_backend.Backend.db_handle ->
             ?reason:string ->
             Hitscore_layout.Layout.Function_fastx_quality_stats.pointer ->
             (Hitscore_layout.Layout.Function_fastx_quality_stats.pointer,
              [> `Layout of
                   [> `Function of string | `Record of string ] *
                   [> `db_backend_error of
                        [> `query of Hitscore_db_backend.Sql_query.t * exn ]
                    | `wrong_add_value ] ])
             t
      
  (** Get the status of the evaluation by checking its data-base
      status and it presence in the PBS queue. *)
           val status :
             dbh:Hitscore_db_backend.Backend.db_handle ->
             configuration:Configuration.local_configuration ->
             Hitscore_layout.Layout.Function_fastx_quality_stats.pointer ->
             ([> `not_started of
                   Layout.Enumeration_process_status.t
               | `running
               | `started_but_not_running of
                   [> `system_command_error of string * exn ] ],
              [> `Layout of
                   [> `Function of string ] *
                   [> `db_backend_error of
                        [> `query of Hitscore_db_backend.Sql_query.t * exn ]
                    | `parse_evaluation_error of
                        string Hitscore_std.Option.t list * exn
                    | `parse_sexp_error of Hitscore_std.Sexp.t * exn
                    | `result_not_unique of
                        Hitscore_db_backend.Backend.result ]
               | `work_directory_not_configured ])
             t
      
  (** Kill the evaluation ([qdel]) and set it as failed. *)
           val kill :
             dbh:Hitscore_db_backend.Backend.db_handle ->
             configuration:Configuration.local_configuration ->
             Hitscore_layout.Layout.Function_fastx_quality_stats.pointer ->
             (Hitscore_layout.Layout.Function_fastx_quality_stats.pointer,
              [> `Layout of
                   [> `Function of string | `Record of string ] *
                   [> `db_backend_error of
                        [> `query of Hitscore_db_backend.Sql_query.t * exn ]
                    | `parse_evaluation_error of
                        string Hitscore_std.Option.t list * exn
                    | `parse_sexp_error of Hitscore_std.Sexp.t * exn
                    | `result_not_unique of
                        Hitscore_db_backend.Backend.result
                    | `wrong_add_value ]
               | `not_started of
                   Layout.Enumeration_process_status.t
               | `system_command_error of string * exn
               | `work_directory_not_configured ])
             t

end
