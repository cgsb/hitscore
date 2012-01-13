
(** Module for run-time configuration of the library.  *)

(** It contains only non-I/O functions.  *)


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

(** Get the Data-base configuration *)
val db: local_configuration -> db_configuration option
  
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
