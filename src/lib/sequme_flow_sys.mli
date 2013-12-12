(** Basic system access functions. *)

open Core.Std
open Sequme_flow

module Timeout: sig
    
  type t = [
  | `global_default
  | `seconds of float
  | `none
  ]

  val set_global_default: float -> unit
  val get_global_default: unit -> float

  val do_io:
    t -> 
    f:(unit ->
       ('a, [> `io_exn of exn | `timeout of float ] as 'b) Sequme_flow.t) ->
    ('a, 'b) Sequme_flow.t
end

(** Write a string to a file. *)
val write_file: string -> content:string ->
  (unit, [> `write_file_error of (string * exn)]) t

(** Read a string from a file. *)
val read_file: ?timeout:Timeout.t -> string -> 
  (string,
   [> `read_file_error of string * exn
   | `read_file_timeout of string * float ]) Sequme_flow.t
    

(** Block for a given amount of seconds ([Lwt_unix.sleep]). *)
val sleep: float -> (unit, [> `io_exn of exn ]) t    

  
(** Make [/bin/sh] execute a command. *)
val system_command: string ->
  (unit,
   [> `system_command_error of string *
       [> `exited of int | `exn of exn | `signaled of int | `stopped of int ]
   ]) t

(** Execute a shell command and return its [stdout, stderr]. *)
val get_system_command_output: string ->
  (string * string,
   [> `system_command_error of string *
       [> `exited of int | `exn of exn | `signaled of int | `stopped of int ]
   ]) t

