(** Main entry-point of the library (functor). *)

open Hitscore_std


(** Make the Hitscore library for a given I/O model. *)

  (** The configuration information. *)
module Configuration: Hitscore_interfaces.CONFIGURATION
  
module Flow: Sequme_flow_monad.FLOW_MONAD
  with type 'a IO.t = 'a Lwt.t
  
include module type of Hitscore_layout
  
  (** The Access-Rights management.  *)
module Access_rights:  Hitscore_access_rights.ACCESS_RIGHTS
  
  (** Module containing “common” utilities, it eases the creation of
      function functors. *)
module Common : Hitscore_common.COMMON
           
module Backend : Hitscore_db_backend.BACKEND
  
  (** Sample-sheet assembly. *) 
module Assemble_sample_sheet: Hitscore_function_interfaces.ASSEMBLE_SAMPLE_SHEET

  (** Demultiplexing. *)
module Bcl_to_fastq: Hitscore_function_interfaces.BCL_TO_FASTQ

  (** Information about the HiSeq raw directories. *)
module Hiseq_raw: Hitscore_interfaces.HISEQ_RAW

  (** Information about CASAVA Unaligned directories. *)
module B2F_unaligned: Hitscore_interfaces.B2F_UNALIGNED

  (** Prepare delivery of FASTQ files. *)
module Unaligned_delivery: Hitscore_function_interfaces.UNALIGNED_DELIVERY

  (** Delete intensity files (for now just registration). *)
module Delete_intensities: Hitscore_function_interfaces.DELETE_INTENSITIES

  (** Coerce an Unaligned directory to a generic fastqs directory. *)
module Coerce_b2f_unaligned: Hitscore_function_interfaces.COERCE_B2F_UNALIGNED

  (** Run Fastx quality stats on a given bunch of FASTQ files. *)
module Fastx_quality_stats: Hitscore_function_interfaces.FASTX_QUALITY_STATS

  (** The Query/Data/Message Broker. *)
module Broker: Hitscore_broker.BROKER

  (** Attempt to connect to the database. *)
val db_connect :
  Configuration.local_configuration ->
  (Backend.db_handle, [> `db_backend_error of [> Backend.error ] ])
    Flow.monad

  (** Close a data-base handle. *)
val db_disconnect : Configuration.local_configuration -> 
  Backend.db_handle -> 
  (unit, [> `db_backend_error of [> `disconnection of exn ] ]) Flow.monad
    
    
  (** Connect and disconnect whatever happens in [~f].  *)
val with_database :
  configuration:Configuration.local_configuration ->
  f:(dbh:Backend.db_handle ->
     ('a, [> `db_backend_error of [> Backend.error ] ] as 'b)
       Hitscore_std.Flow.monad) ->
  ('a, 'b) Hitscore_std.Flow.t

