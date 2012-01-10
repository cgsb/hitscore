open Hitscore_std


module Preemptive_threading_config : 
  Hitscore_interfaces.IO_CONFIGURATION with type 'a t = 'a = struct
    include PGOCaml.Simple_thread
    let map_sequential l ~f = List.map ~f l
    let log_error = eprintf "%s"
    let catch f e =
      try f () with ex -> e ex
end



module Make (IO_configuration : Hitscore_interfaces.IO_CONFIGURATION) = struct

  module Result_IO = Hitscore_result_IO.Make(IO_configuration)

  module Layout = Hitscore_db_access.Make(Result_IO)

  module Assemble_sample_sheet = 
    Hitscore_assemble_sample_sheet.Make (Result_IO) (Layout)

  module Bcl_to_fastq =
    Hitscore_bcl_to_fastq.Make (Result_IO) (Layout)

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


  let db_host     t = Option.map t.db_configuration (fun dbc -> dbc.db_host    ) 
  let db_port     t = Option.map t.db_configuration (fun dbc -> dbc.db_port    ) 
  let db_database t = Option.map t.db_configuration (fun dbc -> dbc.db_database) 
  let db_username t = Option.map t.db_configuration (fun dbc -> dbc.db_username) 
  let db_password t = Option.map t.db_configuration (fun dbc -> dbc.db_password) 
    
  let db_connect t =
    match t.db_configuration with
    | None -> 
      Result_IO.(bind_on_error
                   (catch_io Layout.PGOCaml.connect ())
                   (fun e -> error (`pg_exn e)))
    | Some {db_host; db_port; db_database; db_username; db_password} ->
      Result_IO.(bind_on_error
                   (catch_io (Layout.PGOCaml.connect
                                ~host:db_host
                                ~port:db_port
                                ~database:db_database
                                ~user:db_username
                                ~password:db_password) ())
                   (fun e -> error (`pg_exn e)))        
        
  let db_disconnect t dbh = 
    Result_IO.(bind_on_error
                 (catch_io Layout.PGOCaml.close dbh )
                 (fun e -> error (`pg_exn e)))

end

