open Batteries_uni

exception Error of string

type t = {
  fcid : string;
  mismatch : int;
  sample_sheet_path : string;
}

module Database : sig
  exception Error of string
  type s = t
  type t = s Map.IntMap.t
  val of_file : string -> t
end
