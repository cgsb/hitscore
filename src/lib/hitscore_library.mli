
exception Error of string

module Sample: module type of Hitscore_sample

type t = {
  sample : Sample.t;
  indexes : Sequme.Illumina.Barcode.t list;
  read_type : Sequme.Read_type.t;
  read_length : int; (** For paired-end, this is length of each end. *)
}

module Database : sig
  exception Error of string
  type s = t
  type t = s BatMap.IntMap.t
  val of_file : string -> t
end
