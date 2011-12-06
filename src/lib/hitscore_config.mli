(** The configuration module types (parameter(s) of [Hitscore.Make]).  *)

(** The I/O configuration module type is the main parameter of [Hitscore.Make].  *)
module type IO_CONFIGURATION = sig

  (** The threading and I/O (for now) follows PG'OCaml's conventions:
    {[
    module type THREAD = sig
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
    end
    ]} *)
  include PGOCaml_generic.THREAD

  (** Report errors (as text, temporary).  *)
  val log_error: string -> unit t

  (** This is the sequential threaded-map over lists, i.e.
    {i {{:http://ocsigen.org/lwt/api/Lwt_list}Lwt_list}.map_s} but with a {i
    Core-like} API. *)
  val map_sequential : 'a list -> f:('a -> 'b t) -> 'b list t

  val catch : (unit -> 'a t) -> (exn -> 'a t) -> 'a t

end


