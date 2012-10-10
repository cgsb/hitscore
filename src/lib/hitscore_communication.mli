(** Client/Server communication utilities. *)

(** The definition of the messages and their serialization. *)
module Protocol :
sig

  (** Messages from the client to the server. *)
  type up = [
  | `log of string (** Ask the server to keep something in the logs. *)
  ]

  (** Messages from the server to the client. *)
  type down = [
  | `user_message of string (** Ask the client to display a message to
                                the user *)
  ]

  type serialization_mode = [ `binary | `s_expression ] with sexp
  type serialization_error = 
  [ `message_serialization of serialization_mode * string * exn ]
    
  val string_of_up: mode:serialization_mode -> up -> string 
  val hum_string_of_up: up -> string 
  val up_of_string_exn: mode:serialization_mode -> string -> up 
  val up_of_string: mode:serialization_mode -> string -> (up, [> serialization_error]) Core.Result.t
  val string_of_down: mode:serialization_mode -> down -> string 
  val hum_string_of_down: down -> string 
  val down_of_string_exn: mode:serialization_mode -> string -> down 
  val down_of_string: mode:serialization_mode -> string -> (down, [> serialization_error]) Core.Result.t 

  (** Add a ["-sexp-messages"] flag that adds the [~mode] argument to the
  command-line specification. *)
  val serialization_mode_flag: unit ->
    (mode:[> `binary | `s_expression ] -> 'a, 'a)
      Sequme_flow_app_util.Command_line.Spec.t
end

(** Utility functions related to authentication with passwords and
    “tokens”. *)
module Authentication : sig

  (** Hash a password with a [Person.t] id using the “official” function. *)
  val hash_password : int -> string -> string

  (** Do a chained user/password check for a given person: first
      against a potential password stored in the layout, then again the
      PAM service [pam_service] (default [""]). *)
  val check_chained :
    ?pam_service:string ->
    person:< g_id : int; login : string option;
             password_hash : string option; .. > ->
    password:string -> unit -> (bool, 'a) Sequme_flow.t
end
