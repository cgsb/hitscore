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

  (** Create a [local_configuration], if no [db_configuration] is given *)
  val configure : ?root_directory:string ->
    ?db_configuration:db_configuration ->
    unit -> local_configuration
    
  val root_directory : local_configuration -> string option
    
  val db_connect : local_configuration -> Layout.db_handle IO_configuration.t
      
  val db_disconnect : 
    local_configuration -> Layout.db_handle -> unit IO_configuration.t
    
end = struct

  module Layout = Hitscore_db_access.Make(IO_configuration)

  type db_configuration = {
    db_host     : string;
    db_port     : int;
    db_database : string;
    db_username : string;
    db_password : string
  }

  let db_configuration ~host ~port ~database ~username ~password =
    {db_host = host; db_port = port; db_database = database;
     db_username = username; db_password = password}

  type local_configuration = {
    root_directory: string option;
    db_configuration: db_configuration option;
  }

  let configure ?root_directory ?db_configuration () =
    { root_directory; db_configuration; }

  let root_directory t = t.root_directory

  let db_connect t =
    match t.db_configuration with
    | None -> Layout.PGOCaml.connect ()
    | Some {
      db_host    ; 
      db_port    ; 
      db_database; 
      db_username; 
      db_password; } ->
      Layout.PGOCaml.connect ()
        ~host:db_host
        ~port:db_port
        ~database:db_database
        ~user:db_username
        ~password:db_password

  let db_disconnect t dbh = Layout.PGOCaml.close dbh 


end

