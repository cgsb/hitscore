(** Main entry-point of the library (functor). *)

open Hitscore_std


(** Make the Hitscore library for a given I/O model. *)
module Make (IO_configuration : Sequme_flow_monad.IO_CONFIGURATION) : sig

  (** A double monad: [Result.t] and [IO_configuration.t]. *)
  module Flow : Sequme_flow_monad.FLOW_MONAD
    with type 'a IO.t = 'a IO_configuration.t

  (** The Layout is the thing defined by the layout DSL.  *)
  module Layout: Hitscore_layout_interface.LAYOUT
    with module Flow = Flow
    with type 'a PGOCaml.monad = 'a Flow.IO.t

  (**  *)
  module Classy_layout: Hitscore_layout_interface.CLASSY
    with module Layout = Layout

  (** The configuration information. *)
  module Configuration: Hitscore_interfaces.CONFIGURATION

  (** The Access-Rights management.  *)
  module Access_rights:  Hitscore_access_rights.ACCESS_RIGHTS
    with module Flow = Flow
    with module Configuration = Configuration
    with module Layout = Layout

  (** Module containing “common” utilities, it eases the creation of
      function functors. *)
  module Common : Hitscore_common.COMMON
    with module Configuration = Configuration
    with module Flow = Flow
    with module Layout = Layout
    with module Access_rights = Access_rights
           
  (** Sample-sheet assembly. *) 
  module Assemble_sample_sheet: Hitscore_function_interfaces.ASSEMBLE_SAMPLE_SHEET
    with module Common = Common

  (** Demultiplexing. *)
  module Bcl_to_fastq: Hitscore_function_interfaces.BCL_TO_FASTQ
    with module Common = Common

  (** Information about the HiSeq raw directories. *)
  module Hiseq_raw: Hitscore_interfaces.HISEQ_RAW

  (** Information about CASAVA Unaligned directories. *)
  module B2F_unaligned: Hitscore_interfaces.B2F_UNALIGNED

  (** Prepare delivery of FASTQ files. *)
  module Unaligned_delivery: Hitscore_function_interfaces.UNALIGNED_DELIVERY
    with module Common = Common

  (** Delete intensity files (for now just registration). *)
  module Delete_intensities: Hitscore_function_interfaces.DELETE_INTENSITIES
    with module Common = Common

  (** Coerce an Unaligned directory to a generic fastqs directory. *)
  module Coerce_b2f_unaligned: Hitscore_function_interfaces.COERCE_B2F_UNALIGNED
    with module Common = Common

  (** Run Fastx quality stats on a given bunch of FASTQ files. *)
  module Fastx_quality_stats: Hitscore_function_interfaces.FASTX_QUALITY_STATS
    with module Common = Common

  (** The Query/Data/Message Broker. *)
  module Broker: Hitscore_broker.BROKER with module Common = Common

  (** Attempt to connect to the database. *)
  val db_connect : Configuration.local_configuration -> 
    (Layout.db_handle, [> `pg_exn of exn]) Flow.monad
      
  (** Close a data-base handle. *)
  val db_disconnect : Configuration.local_configuration -> 
    Layout.db_handle -> 
    (unit, [> `pg_exn of exn]) Flow.monad

    
  (** Connect and disconnect whatever happens in [~f].  *)
  val with_database: 
    configuration:Configuration.local_configuration ->
    f:(dbh:Layout.db_handle -> ('a, [> `pg_exn of exn] as 'b) Flow.monad) ->
    ('a, [> `pg_exn of exn] as 'b) Flow.monad

end 
