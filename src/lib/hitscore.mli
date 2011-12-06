(** Main entry-point of the library (functor). *)



(** Make the Hitscore library for a given I/O model. *)
module Make (IO_configuration : Hitscore_config.IO_CONFIGURATION) : sig

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
  val db_connect : local_configuration -> Layout.db_handle IO_configuration.t
      
  (** Close a data-base handle. *)
  val db_disconnect : 
    local_configuration -> Layout.db_handle -> unit IO_configuration.t
    
end 
