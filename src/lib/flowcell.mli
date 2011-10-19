open Batteries_uni

exception Error of string

type record = {
  fcid : string;
  lane : int;
  library : Library.t
}

module Database : sig
  exception Error of string
  type t = record list
  val of_file : Library.Database.t -> string -> t
end
