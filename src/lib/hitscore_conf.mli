(** Sequme configuration. *)
open Batteries_uni

exception Invalid of string

type t = string Map.StringMap.t

val read : string -> t
  (** [read sequme_root] reads the configuration file located within
      the given [sequme_root] directory. Raise [Invalid] if any
      errors. *)

type dbconf = {
  db_engine : string;
  db_name : string; (** if engine=sqlite, this is absolute path to database file *)
  db_user : string;
  db_password : string;
  db_host : string;
  db_port : string
}

val get_dbconf : t -> dbconf
  (** Return dbconf in [t]. Raise [Invalid] if [t] does not fully
      specify a database configuration. *)

val sqlite_exec : t -> ?cb:(Sqlite3.row -> Sqlite3.headers -> unit) -> string -> unit
  (** [sqlite_exec conf stmt] executes [stmt] on the sqlite database
      specified in [conf]. Raise [Invalid] if [conf] does not specify
      a sqlite database or in case of any other errors. *)

val log : t -> string -> unit
  (** [log conf msg] inserts [msg] into the log table. *)
