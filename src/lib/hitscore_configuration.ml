open Hitscore_std

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
  volumes_directory: string;
  root_writers: string list;
  root_group: string option;
  db_configuration: db_configuration option;
}

let configure ?root_directory ?(root_writers=[]) ?root_group
    ?(vol="vol") ?db_configuration () =
  { root_directory; root_writers; root_group; 
    volumes_directory = vol; db_configuration; }

let db t = t.db_configuration

let root_directory t = t.root_directory

let volumes_directory t =
  Option.(t.root_directory >>| fun r -> sprintf "%s/%s" r t.volumes_directory)

let volume_path t volume =
  Option.(volumes_directory t >>| fun t -> sprintf "%s/%s" t volume)

let volume_path_fun t =
  Option.(volumes_directory t >>| fun t -> (sprintf "%s/%s" t))

let root_writers t = t.root_writers
let root_group t = t.root_group

let db_host     t = Option.map t.db_configuration (fun dbc -> dbc.db_host    ) 
let db_port     t = Option.map t.db_configuration (fun dbc -> dbc.db_port    ) 
let db_database t = Option.map t.db_configuration (fun dbc -> dbc.db_database) 
let db_username t = Option.map t.db_configuration (fun dbc -> dbc.db_username) 
let db_password t = Option.map t.db_configuration (fun dbc -> dbc.db_password) 


