open Hitscore_std
  
type t

val to_string: t -> string
val of_string: string -> (t, [> `sexp_parsing_error of exn]) Result.t

