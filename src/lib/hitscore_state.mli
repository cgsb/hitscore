open Hitscore_std

type t
val get : Hitscore_config.t -> (t,exn) Result.t

type action =
    | AddPerson of Hitscore_person.t

val update_exn : t -> action -> unit
