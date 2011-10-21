open Batteries_uni

exception Error of string

type record = {
  fcid : string;
  lane : int;
  library : Hitscore_library.t
}

module Database : sig
  exception Error of string
  type t = record list
  val of_file : Hitscore_library.Database.t -> string -> t
end
