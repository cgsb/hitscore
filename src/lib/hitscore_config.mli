open Hitscore_std

type t = private {
  root : string;
  db_hostname : string;
  db_port : int;
  db_database : string;
  db_username : string;
  db_password : string
}

val make : root:string
  -> db_hostname:string
  -> db_port:int
  -> db_database:string
  -> db_username:string
  -> db_password:string
  -> t
