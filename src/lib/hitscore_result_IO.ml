open Hitscore_interfaces
open Core.Std

module Make (IO_configuration : IO_CONFIGURATION): 
  RESULT_IO with type 'a IO.t = 'a IO_configuration.t = struct

  module IO = IO_configuration
 
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

    let of_list_sequential l ~f = map_sequential (List.map l return) f

    let debug s = 
      double_bind (catch_io IO_configuration.log_error s)
        ~ok:(return)
        ~error:(fun exn -> error (`io_exn exn))

    let wrap_pgocaml ~query ~on_result =        
      let caught = catch_io query () in
      double_bind caught
        ~ok:on_result
        ~error:(fun exn -> error (`pg_exn exn)) 

    let wrap_io ?(on_exn=fun e -> `io_exn e) f x =        
      let caught = catch_io f x in
      double_bind caught
        ~ok:return
        ~error:(fun exn -> error (on_exn exn)) 

    let of_option o ~f =
      of_list_sequential (List.filter_opt [o]) ~f
      >>= function
      | [one] -> return (Some one)
      | _ -> return None

    let of_result r = IO_configuration.return r 


    let system_command s =
      let caught = catch_io IO.system_command s in
      double_bind caught
        ~ok:return
        ~error:(fun exn -> error (`system_command_error (s, exn)))


    (* Wrapped version of [IO.write_string_to_file]. *)
    let  write_file ~file ~content =
      let caught = catch_io (IO.write_string_to_file content) file in
      double_bind caught
        ~ok:return
        ~error:(fun exn -> error (`write_file_error (file, content, exn)))


  end
