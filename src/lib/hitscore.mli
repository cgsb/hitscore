(** Main entry-point of the library (functor). *)

open Hitscore_std


(** The "default" configuration (with OCaml's preemptive threads, and
    weaker typing). *)
module Preemptive_threading_config : 
  Hitscore_interfaces.IO_CONFIGURATION with type 'a t = 'a

(** Make the Hitscore library for a given I/O model. *)
module Make (IO_configuration : Hitscore_interfaces.IO_CONFIGURATION) : sig

  (** A double monad: [Result.t] and [IO_configuration.t]. *)
  module Result_IO : Hitscore_interfaces.RESULT_IO
    with type 'a IO.t = 'a IO_configuration.t

  (** The Layout is the thing defined by the layout DSL.  *)
  module Layout: Hitscore_layout_interface.LAYOUT
    with module Result_IO = Result_IO
    with type 'a PGOCaml.monad = 'a Result_IO.IO.t

  (** The configuration information. *)
  module Configuration: Hitscore_interfaces.CONFIGURATION

  (** The Access-Rights management.  *)
  module Access_rights:  Hitscore_access_rights.ACCESS_RIGHTS
    with module Result_IO = Result_IO
    with module Configuration = Configuration
    with module Layout = Layout

  module Common : Hitscore_common.COMMON
    with module Configuration = Configuration
    with module Result_IO = Result_IO
    with module Access_rights = Access_rights
    with module Layout = Layout
           
  module Assemble_sample_sheet: Hitscore_function_interfaces.ASSEMBLE_SAMPLE_SHEET
    with module Configuration = Configuration
    with module Result_IO = Result_IO
    with module Access_rights = Access_rights
    with module Layout = Layout

  module Bcl_to_fastq: Hitscore_function_interfaces.BCL_TO_FASTQ
    with module Configuration = Configuration
    with module Result_IO = Result_IO
    with module Access_rights = Access_rights
    with module Layout = Layout

  module Hiseq_raw: Hitscore_interfaces.HISEQ_RAW

  module B2F_unaligned: Hitscore_interfaces.B2F_UNALIGNED

  module Unaligned_delivery: Hitscore_function_interfaces.UNALIGNED_DELIVERY
    with module Configuration = Configuration
    with module Result_IO = Result_IO
    with module Access_rights = Access_rights
    with module Layout = Layout

  module Delete_intensities: Hitscore_function_interfaces.DELETE_INTENSITIES
    with module Configuration = Configuration
    with module Result_IO = Result_IO
    with module Access_rights = Access_rights
    with module Layout = Layout


  (** Attempt to connect to the database. *)
  val db_connect : Configuration.local_configuration -> 
    (Layout.db_handle, [> `pg_exn of exn]) Result_IO.monad
      
  (** Close a data-base handle. *)
  val db_disconnect : Configuration.local_configuration -> 
    Layout.db_handle -> 
    (unit, [> `pg_exn of exn]) Result_IO.monad

    
  (** Connect and disconnect whatever happens in [~f].  *)
  val with_database: 
    configuration:Configuration.local_configuration ->
    f:(dbh:Layout.db_handle -> ('a, [> `pg_exn of exn] as 'b) Result_IO.monad) ->
    ('a, [> `pg_exn of exn] as 'b) Result_IO.monad

end 
