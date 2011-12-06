open Hitscore_std


module Preemptive_threading_config : 
  Hitscore_config.IO_CONFIGURATION with type 'a t = 'a = struct
    include PGOCaml.Simple_thread
    let map_sequential l ~f = List.map ~f l
    let log_error = eprintf "%s"
    let catch f e =
      try f () with ex -> e ex
end


module Make (IO_configuration : Hitscore_config.IO_CONFIGURATION) = struct

  module Layout = Hitscore_db_access.Make(IO_configuration)

  module Result_IO = struct

    include  Monad.Make2(struct 
      type ('a, 'b) t = ('a, 'b) Result.t IO_configuration.t

      let return x = IO_configuration.return (Ok x) 
      let bind x f = 
        IO_configuration.(>>=) x (function
          | Error e -> IO_configuration.return (Error e)
          | Ok o -> f o)
    end)
    
    let catch_io ~f x =
      IO_configuration.catch 
        (fun () -> 
          let a_exn_m : 'a IO_configuration.t = f x in
          IO_configuration.(>>=) a_exn_m
            (fun x -> IO_configuration.return (Ok x)))
        (fun e -> IO_configuration.return (Error e))

  end

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
    | None -> 
      Result_IO.catch_io Layout.PGOCaml.connect ()
    | Some {db_host; db_port; db_database; db_username; db_password} ->
      Result_IO.catch_io (Layout.PGOCaml.connect
                            ~host:db_host
                            ~port:db_port
                            ~database:db_database
                            ~user:db_username
                            ~password:db_password) ()
        
  let db_disconnect t dbh = Result_IO.catch_io Layout.PGOCaml.close dbh 


end

