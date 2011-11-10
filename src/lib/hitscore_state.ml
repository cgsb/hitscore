module Result = Core.Std.Result
open Result
type dbh = (string, bool) Hashtbl.t PGOCaml.t

module Config = Hitscore_config
module Person = Hitscore_person

type t = {
  config : Config.t;
  dbh : dbh
}

let get x =
  let open Config in
  try
    let dbh = PGOCaml.connect
      ~host:x.db_hostname
      ~port:x.db_port
      ~user:x.db_username
      ~password:x.db_password
      ~database:x.db_database
      ()
    in
    Ok {config=x; dbh}
  with e -> Error e


type action =
    | AddPerson of Person.t

let update_exn t = function
  | AddPerson x -> ()
(*
      let open Person in
      let f = x.first in
      let m = match x.middle_initial with Some m -> Some (String.make 1 m) | None -> None in
      let l = x.last in
      let e = x.email in
      PGSQL(t.dbh)
        "INSERT INTO person (first_name,middle_initial,last_name,email)
         VALUES ($f, $?m, $l, $e)"
*)
