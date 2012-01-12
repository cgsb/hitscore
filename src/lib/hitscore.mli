(** Main entry-point of the library (functor). *)

open Hitscore_std


(** The "default" configuration (with OCaml's preemptive threads, and
    weaker typing). *)
module Preemptive_threading_config : 
  Hitscore_interfaces.IO_CONFIGURATION with type 'a t = 'a

(** Make the Hitscore library for a given I/O model. *)
module Make (IO_configuration : Hitscore_interfaces.IO_CONFIGURATION) : sig

  (** A double monad: [Result.t] and [IO_configuration.t]. *)
  module Result_IO : Hitscore_result_IO.RESULT_IO
    with type 'a IO.t = 'a IO_configuration.t

  (** The Layout is the thing defined by the layout DSL.  *)
  module Layout: module type of Hitscore_db_access.Make(Result_IO)

  module Assemble_sample_sheet: 
  module type of Hitscore_assemble_sample_sheet.Make (Result_IO) (Layout)

  module Bcl_to_fastq: 
  module type of Hitscore_bcl_to_fastq.Make (Result_IO) (Layout)

  (** [db_configuration] keeps track of the db-connection parameters. *)
  type db_configuration

  (** Build a data-base configuration. *)
  val db_configuration : host:string ->
    port:int ->
    database:string ->
    username:string -> password:string -> db_configuration

  (** [local_configuration] contains a whole configuration instance. *)
  type local_configuration

  (** Create a [local_configuration], if no [db_configuration] is given,
      the default values will be used (i.e. PGOCaml will try to connect to
      the local database). *)
  val configure : ?root_directory:string ->
    ?root_writers:string list -> ?root_group:string ->
    ?vol:string ->
    ?db_configuration:db_configuration ->
    unit -> local_configuration
    
  (** Get the current root directory (if set).  *)
  val root_directory : local_configuration -> string option
    
  (** Get the root writers' logins. *)
  val root_writers: local_configuration -> string list

  (** Get the root owner group. *)
  val root_group: local_configuration -> string option

  (** Get the current path to the volumes (i.e. kind-of [$ROOT/vol/]). *)
  val volumes_directory: local_configuration -> string option

  (** Make a path to a VFS volume. *)
  val volume_path: local_configuration -> string -> string option

  (** Make a path-making function for VFS volumes. *)
  val volume_path_fun: local_configuration -> (string -> string) option

  val db_host     : local_configuration -> string   option 
  val db_port     : local_configuration -> int      option 
  val db_database : local_configuration -> string   option 
  val db_username : local_configuration -> string   option 
  val db_password : local_configuration -> string   option 

  (** Attempt to connect to the database. *)
  val db_connect : local_configuration -> 
    (Layout.db_handle, [> `pg_exn of exn]) Result_IO.monad
      
  (** Close a data-base handle. *)
  val db_disconnect : local_configuration -> Layout.db_handle -> 
    (unit, [> `pg_exn of exn]) Result_IO.monad

    
end 
