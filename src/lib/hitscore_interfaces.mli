(** Some shared module types.  *)

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

  (** This is like {i {{:http://ocsigen.org/lwt/api/Lwt}Lwt}.catch}. *)
  val catch : (unit -> 'a t) -> (exn -> 'a t) -> 'a t


end

open Core.Std

(** A double monad: [Result.t] and [IO_configuration.t]. *)
module type RESULT_IO = sig

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
  include Monad.S2 with type ('a, 'b) monad = ('a, 'b) Result.t IO.t
    
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
      in a polymorphic variant.  *)
  val wrap_io: ('a -> 'b IO.t) -> 'a -> ('b, [> `io_exn of exn ]) monad


  (** [of_option] allows to put options {i inside} the monad. *)
  val of_option: 'a option -> f:('a -> ('c, 'b) monad) -> ('c option, 'b) monad 

end

(** Non-I/O Runtime configuration of the library. *)
module type CONFIGURATION = sig

  (** [db_configuration] keeps track of the db-connection parameters. *)
  type db_configuration

  (** Build a data-base configuration. *)
  val db_configuration : host:string ->
    port:int ->
    database:string ->
    username:string -> password:string -> db_configuration

  (** [local_configuration] contains a whole configuration instance. *)
  type local_configuration

  (** Create a [local_configuration], if no [db_configuration] is given,
      the default values will be used (i.e. PGOCaml will try to connect to
      the local database). *)
  val configure : ?root_directory:string ->
    ?root_writers:string list -> ?root_group:string ->
    ?vol:string ->
    ?db_configuration:db_configuration ->
    ?work_directory:string ->
    unit -> local_configuration

  (** Get the Data-base configuration *)
  val db: local_configuration -> db_configuration option
    
  val db_host     : local_configuration -> string   option 
  val db_port     : local_configuration -> int      option 
  val db_database : local_configuration -> string   option 
  val db_username : local_configuration -> string   option 
  val db_password : local_configuration -> string   option 

  (** Get the current root directory (if set).  *)
  val root_directory : local_configuration -> string option
    
  (** Get the root writers' logins. *)
  val root_writers: local_configuration -> string list

  (** Get the root owner group. *)
  val root_group: local_configuration -> string option

  (** Get the current path to the volumes (i.e. kind-of [$ROOT/vol/]). *)
  val volumes_directory: local_configuration -> string option

  (** Make a path to a VFS volume. *)
  val volume_path: local_configuration -> string -> string option

  (** Make a path-making function for VFS volumes. *)
  val volume_path_fun: local_configuration -> (string -> string) option

  (** Get (if configured) the current work-directory (the one used for
  short-term storage and computations). *)
  val work_directory: local_configuration -> string option

  (** Set of available profiles. *)
  type profile_set

  (** Parse a list of S-Expressions representing configuration profiles. *)
  val parse_str: string -> 
    (profile_set, [> `configuration_parsing_error of exn ]) Result.t

  (** Get the list of available profile names.  *)
  val profile_names: profile_set -> string list

  (** Select a profile and build a configuration with it. *)
  val use_profile: profile_set -> string -> 
    (local_configuration, [> `profile_not_found of string]) Result.t


end


