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

  module Result_IO = struct

    include  Monad.Make2(struct 
      type ('a, 'b) t = ('a, 'b) Result.t IO_configuration.t

      let return x = IO_configuration.return (Ok x) 
      let bind x f = 
        IO_configuration.(>>=) x (function
          | Error e -> IO_configuration.return (Error e)
          | Ok o -> f o)
    end)

    let error e = IO_configuration.return (Error e)
    
    let bind_on_error m ~f = IO_configuration.(>>=) m (function
      | Ok o -> IO_configuration.return (Ok o)
      | Error e -> (f e))

    let double_bind m ~ok ~error = 
      IO_configuration.(>>=) m (function
        | Ok o -> ok o
        | Error e -> error e)

    let catch_io ~f x =
      IO_configuration.catch 
        (fun () -> 
          let a_exn_m : 'a IO_configuration.t = f x in
          IO_configuration.(>>=) a_exn_m
            (fun x -> IO_configuration.return (Ok x)))
        (fun e -> IO_configuration.return (Error e))

    let map_sequential (type b) (l: ('a, b) monad list) ~f =
      let module Map_sequential = struct
        exception Local_exception of b
        let ms l f =
          bind_on_error 
            (catch_io
               (IO_configuration.map_sequential ~f:(fun m ->
                 IO_configuration.(>>=) m (function
                   | Ok o -> 
                     IO_configuration.(>>=) (f o) (function
                       | Ok oo -> IO_configuration.return oo
                       | Error ee -> IO_configuration.fail (Local_exception ee))
                   | Error e -> IO_configuration.fail (Local_exception e))))
               l)
            (function Local_exception e -> error e 
              | e -> failwithf "Expecting only Local_exception, but got: %s"
                (Exn.to_string e) ())
      end in
      Map_sequential.ms l f

  end

  module Layout = Hitscore_db_access.Make(struct
    include IO_configuration
    module Result_IO = Result_IO 
  end)

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

