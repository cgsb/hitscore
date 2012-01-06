open Hitscore_interfaces
open Core.Std

module type RESULT_IO = sig
  module IO : Hitscore_interfaces.IO_CONFIGURATION
  include Monad.S2 with type ('a, 'b) monad = ('a, 'b) Result.t IO.t
  val catch_io : f:('b -> 'a IO.t) -> 'b -> ('a, exn) monad
  val error: 'a -> ('any, 'a) monad
  val bind_on_error: ('a, 'err) monad -> f:('err -> ('a, 'b) monad) -> 
    ('a, 'b) monad
  val double_bind: ('a, 'b) monad ->
    ok:('a -> ('c, 'd) monad) ->
    error:('b -> ('c, 'd) monad) -> ('c, 'd) monad
  val map_sequential: ('a, 'b) monad list -> f:('a -> ('c, 'b) monad) ->
    ('c list, 'b) monad
  val of_list_sequential: 'a list -> f:('a -> ('c, 'b) monad) ->
    ('c list, 'b) monad
  val debug: string -> (unit, [> `io_exn of exn ]) monad
  val wrap_pgocaml: 
    query:(unit -> 'a IO.t) ->
    on_result:('a -> ('b, [> `pg_exn of exn ] as 'c) monad) ->
    ('b, 'c) monad
  val wrap_io: ('a -> 'b IO.t) -> 'a -> ('b, [> `io_exn of exn ]) monad
  val of_option: 'a option -> f:('a -> ('c, 'b) monad) -> ('c option, 'b) monad 

end



module Make (IO_configuration : IO_CONFIGURATION) = struct

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

    let wrap_io f x =        
      let caught = catch_io f x in
      double_bind caught
        ~ok:return
        ~error:(fun exn -> error (`io_exn exn)) 

    let of_option o ~f =
      of_list_sequential (List.filter_opt [o]) ~f
      >>= function
      | [one] -> return (Some one)
      | _ -> return None


  end
