open Hitscore_std

type t
val get : Hitscore_config.t -> (t,exn) Result.t
