open Hitscore_std

module Config = Hitscore_config

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
