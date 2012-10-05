
module Protocol :
sig

  type up = [
  | `log of string
  ]

  type down = [
  | `user_message of string
  ]

end

module Authentication : sig

  val hash_password : int -> string -> string

end
