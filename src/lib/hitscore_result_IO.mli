open Core.Std
open Hitscore_config

(** A double monad: [Result.t] and [IO_configuration.t]. *)
module type RESULT_IO = sig

  type 'a io

  (** It a {{:http://www.janestreet.com/ocaml/doc/core/Monad.S2.html}Monad.S2}
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
  include Monad.S2 with type ('a, 'b) monad = ('a, 'b) Result.t io
    
  (** [catch_io f x] uses [IO_configuration.catch] to catch all the
      exceptions in the result. *)
  val catch_io : f:('b -> 'a io) -> 'b -> ('a, exn) monad
    
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

  end

module Make (IO : IO_CONFIGURATION) : RESULT_IO with type 'a io = 'a IO.t

