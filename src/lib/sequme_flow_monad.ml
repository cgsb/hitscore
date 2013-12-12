(** The Old Functorial Flow-monad
    @deprecated
    Use [Sequme_flow]
*)

(** The I/O configuration module type is the main parameter of [Hitscore.Make].  *)
module type IO_CONFIGURATION = sig


  (** {4 I/O Compatible with PG'OCaml} *)
  (** The threading and I/O (for now) follows PG'OCaml's conventions: *)

  (** The IO monad *)
  type 'a t
  val return : 'a -> 'a t
  val (>>=) : 'a t -> ('a -> 'b t) -> 'b t
  val fail : exn -> 'a t
    
  type in_channel
  type out_channel
  val open_connection : Unix.sockaddr -> (in_channel * out_channel) t
  val output_char : out_channel -> char -> unit t
  val output_binary_int : out_channel -> int -> unit t
  val output_string : out_channel -> string -> unit t
  val flush : out_channel -> unit t
  val input_char : in_channel -> char t
  val input_binary_int : in_channel -> int t
  val really_input : in_channel -> string -> int -> int -> unit t
  val close_in : in_channel -> unit t

(** {4 Additional Requirements} *)
    
  (** Report errors (as text, temporary).  *)
  val log_error: string -> unit t

  (** This is the sequential threaded-map over lists, i.e.
    {i {{:http://ocsigen.org/lwt/api/Lwt_list}Lwt_list}.map_s} but with a {i
    Core-like} API. *)
  val map_sequential : 'a list -> f:('a -> 'b t) -> 'b list t

  (** This is like {i {{:http://ocsigen.org/lwt/api/Lwt}Lwt}.catch}. *)
  val catch : (unit -> 'a t) -> (exn -> 'a t) -> 'a t

  (** Run a unix command (in a shell, like [Unix.system]).  *)
  val system_command: string -> unit t

  (** [write_string_to_file string file] should write a string to a file.  *)
  val write_string_to_file: string -> string -> unit t

end

open Core.Std


  
(** The "default" configuration (with OCaml's preemptive threads, and
    weaker typing). *)
module Preemptive_threading_config : 
  IO_CONFIGURATION with type 'a t = 'a = struct

    type 'a t = 'a
    let return x = x
    let (>>=) v f =  f v
    let fail = raise

    type in_channel = Pervasives.in_channel
    type out_channel = Pervasives.out_channel
    let open_connection = Unix.open_connection
    let output_char = output_char
    let output_binary_int = output_binary_int
    let output_string = output_string
    let flush = flush
    let input_char = input_char
    let input_binary_int = input_binary_int
    let really_input = really_input
    let close_in = In_channel.close

    let map_sequential l ~f = List.map ~f l
    let log_error = eprintf "%s"
    let catch f e =
      try f () with ex -> e ex

    exception Wrong_status of [ `Exit_non_zero of int | `Signal of Signal.t ]
    let system_command s =
      begin match Unix.system s with
      | Ok () -> ()
      | Error e -> raise (Wrong_status e)
      end

    let write_string_to_file content file =
      Out_channel.(with_file file ~f:(fun o ->
        output_string o content))
      

end



  
(** A double monad: [Result.t] and [IO_configuration.t]. *)
module type FLOW_MONAD = sig

  module IO : IO_CONFIGURATION

  (** It is 
      a {{:http://www.janestreet.com/ocaml/doc/core/Monad.S2.html}Monad.S2}
      from {i Core}. {[
      type ('a, 'b) monad 
      val (>>=) : ('a, 'b) monad ->
      ('a -> ('c, 'b) monad) -> ('c, 'b) monad
      val (>>|) : ('a, 'b) monad -> ('a -> 'c) -> ('c, 'b) monad
      val bind : ('a, 'b) monad -> ('a -> ('c, 'b) monad) -> ('c, 'b) monad
      val return : 'a -> ('a, 'b) monad
      val map : ('a, 'b) monad -> f:('a -> 'c) -> ('c, 'b) monad
      val join : (('a, 'b) monad, 'b) monad -> ('a, 'b) monad
      val ignore : ('a, 'b) monad -> (unit, 'b) monad
      ]}
  *)
  include Monad.S2 with type ('a, 'b) t = ('a, 'b) Result.t IO.t
  type ('a, 'b) monad = ('a, 'b) t
    
  (** [catch_io f x] uses [IO_configuration.catch] to catch all the
      exceptions in the result. *)
  val catch_io : f:('b -> 'a IO.t) -> 'b -> ('a, exn) monad
    
  (** Use anything as an Error thread.  *)
  val error: 'a -> ('any, 'a) monad

  (** Do something on errors (generally augment them). *)
  val bind_on_error: ('a, 'err) monad -> f:('err -> ('a, 'b) monad) -> 
    ('a, 'b) monad
      
  (** Do something on both sides. *)
  val double_bind: ('a, 'b) monad ->
    ok:('a -> ('c, 'd) monad) ->
    error:('b -> ('c, 'd) monad) -> ('c, 'd) monad

  (** Apply [f] to all the [Ok]'s or propagate an error. *)
  val map_sequential: ('a, 'b) monad list -> f:('a -> ('c, 'b) monad) ->
    ('c list, 'b) monad

  (** [of_list_sequential l f] is equivalent to
      [map_sequential (List.map l return) f]. *)
  val of_list_sequential: 'a list -> f:('a -> ('c, 'b) monad) ->
    ('c list, 'b) monad

  (** Print debug information in some "channel" provided by 
      [IO_configuration.log_error]. *)
  val debug: string -> (unit, [> `io_exn of exn ]) monad
    
  (** Put a PGOCaml query into the monad in an extensibility-compliant way. *)
  val wrap_pgocaml: 
    query:(unit -> 'a IO.t) ->
    on_result:('a -> ('b, [> `pg_exn of exn ] as 'c) monad) ->
    ('b, 'c) monad

  (** Put any IO.t in a monad like [catch_io] but put the exception
      in a polymorphic variant (the default being [`io_exn e]).  *)
  val wrap_io: ?on_exn:(exn -> ([> `io_exn of exn ] as 'c)) ->
    ('a -> 'b IO.t) -> 'a -> ('b, 'c) monad

  (** [of_option] allows to put options {i inside} the monad. *)
  val of_option: 'a option -> f:('a -> ('c, 'b) monad) -> ('c option, 'b) monad 

  (** [of_result] adds threading to a [Result.t]. *)
  val of_result: ('a, 'b) Result.t -> ('a, 'b) monad

  (** Wrapped version of [IO.system_command]. *)
  val system_command: string -> 
    (unit, [> `system_command_error of (string * exn) ]) monad

  (** Wrapped version of [IO.write_string_to_file]. *)
  val write_file: file:string -> content:string ->
    (unit, [> `write_file_error of (string * string * exn)]) monad

end


module Make (IO_configuration : IO_CONFIGURATION): 
  FLOW_MONAD with type 'a IO.t = 'a IO_configuration.t = struct

    module IO = IO_configuration
 
    type ('a, 'b) t = ('a, 'b) Result.t IO_configuration.t
    include  Monad.Make2(struct 
      type ('a, 'b) t = ('a, 'b) Result.t IO_configuration.t

      let return x = IO_configuration.return (Ok x) 
      let bind x f = 
        IO_configuration.(>>=) x (function
          | Error e -> IO_configuration.return (Error e)
          | Ok o -> f o)
    end)

    type ('a, 'b) monad = ('a, 'b) t

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

