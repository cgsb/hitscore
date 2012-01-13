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
  module Layout: module type of Hitscore_db_access.Make(Result_IO)

  (** The configuration information. *)
  module Configuration: Hitscore_interfaces.CONFIGURATION

  module Assemble_sample_sheet: 
  module type of Hitscore_assemble_sample_sheet.Make (Result_IO) (Layout)

  module Bcl_to_fastq: 
  module type of Hitscore_bcl_to_fastq.Make (Result_IO) (Layout)

  (** Attempt to connect to the database. *)
  val db_connect : Configuration.local_configuration -> 
    (Layout.db_handle, [> `pg_exn of exn]) Result_IO.monad
      
  (** Close a data-base handle. *)
  val db_disconnect : Configuration.local_configuration -> 
    Layout.db_handle -> 
    (unit, [> `pg_exn of exn]) Result_IO.monad

    
end 
