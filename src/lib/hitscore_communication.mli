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

  type serialization_mode = [ `binary | `s_expression ]

  val string_of_up: mode:serialization_mode -> up -> string 
  val up_of_string_exn: mode:serialization_mode -> string -> up 
  val string_of_down: mode:serialization_mode -> down -> string 
  val down_of_string_exn: mode:serialization_mode -> string -> down 
end

(** Utility functions related to authentication with passwords and
    “tokens”. *)
module Authentication : sig

  (** Hash a password with a [Person.t] id using the “official” function. *)
  val hash_password : int -> string -> string

end
