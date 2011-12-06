

module Make (Config : module type of Hitscore_config) = struct

  module Layout = Hitscore_db_access.Make(Config)

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

