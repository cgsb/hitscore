open Batteries_uni

exception Error of string

type t = {
  sample : Sample.t;
  indexes : Sequme.Illumina.Barcode.t list;
  read_type : Sequme.Read_type.t;
  read_length : int; (** For paired-end, this is length of each end. *)
}

module Database : sig
  exception Error of string
  type s = t
  type t = s Map.IntMap.t
  val of_file : string -> t
end
