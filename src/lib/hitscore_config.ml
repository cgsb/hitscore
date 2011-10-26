open Hitscore_std

type t = {
  root : string;
  db_hostname : string;
  db_port : int;
  db_database : string;
  db_username : string;
  db_password : string
}

let make ~root ~db_hostname ~db_port ~db_database ~db_username ~db_password =
  {root; db_hostname; db_port; db_database; db_username; db_password}
