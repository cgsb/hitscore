(** Main entry-point of the library (functor). *)

open Hitscore_std

(** The "default" configuration (with OCaml's preemptive threads, and
    weaker typing). *)
module Preemptive_threading_config : 
  Hitscore_config.IO_CONFIGURATION with type 'a t = 'a

(** Make the Hitscore library for a given I/O model. *)
module Make (IO_configuration : Hitscore_config.IO_CONFIGURATION) : sig

  (** A double monad: [Result.t] and [IO_configuration.t]. *)
  module Result_IO : sig

    (** It a {{:http://www.janestreet.com/ocaml/doc/core/Monad.S2.html}Monad.S2}
        from {i Core}. {[
type ('a, 'b) monad 
val (>>=) : ('a, 'b) monad ->
       ('a -> ('c, 'b) monad) -> ('c, 'b) monad
val (>>|) : ('a, 'b) monad -> ('a -> 'c) -> ('c, 'b) monad
val bind : ('a, 'b) monad -> ('a -> ('c, 'b) monad) -> ('c, 'b) monad
val return : 'a -> ('a, 'b) monad
val map : ('a, 'b) monad -> f:('a -> 'c) -> ('c, 'b) monad
val join : (('a, 'b) monad, 'b) monad -> ('a, 'b) monad
val ignore : ('a, 'b) monad -> (unit, 'b) monad
]}
  *)
    include Monad.S2 with type ('a, 'b) monad =
                       ('a, 'b) Result.t IO_configuration.t

    (** [catch_io f x] uses [IO_configuration.catch] to catch all the
        exceptions in the result. *)
    val catch_io : f:('b -> 'a IO_configuration.t) -> 'b -> ('a, exn) monad

  end

  (** The Layout is the thing defined by the layout DSL.  *)
  module Layout: module type of Hitscore_db_access.Make(IO_configuration)

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
    ?db_configuration:db_configuration ->
    unit -> local_configuration
    
  (** Get the current root directory (if set).  *)
  val root_directory : local_configuration -> string option
    
  (** Attempt to connect to the database. *)
  val db_connect : local_configuration -> 
    (Layout.db_handle, exn) Result_IO.monad
      
  (** Close a data-base handle. *)
  val db_disconnect : 
    local_configuration -> Layout.db_handle -> (unit, exn) Result_IO.monad
    
end 
