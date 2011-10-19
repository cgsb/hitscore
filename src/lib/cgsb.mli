(** CGSB Core Facility. *)
open Batteries_uni

module Sample : sig
  exception Error of string

  type t = {
    name1 : string;
    investigator : string option;
  }
end

module Library : sig
  exception Error of string

  type t = {
    sample : Sample.t;
    indexes : Sequme.Illumina.Barcode.t list;
    read_type : Sequme.ReadType.t;
    read_length : int; (** For paired-end, this is length of each end. *)
  }
end

module LibraryDB : sig
  exception Error of string

  type t = Library.t Map.IntMap.t

  val of_file : string -> t

end

module Flowcell : sig
  exception Error of string

  type record = {
    fcid : string;
    lane : int;
    library : Library.t
  }
end

module FlowcellDB : sig
  exception Error of string
  type t = Flowcell.record list
  val of_file : LibraryDB.t -> string -> t
end

module BclToFastq : sig
  exception Error of string

  type t = {
    fcid : string;
    mismatch : int;
    sample_sheet_path : string;
  }
end

module BclToFastqDB : sig
  exception Error of string
  type t = BclToFastq.t Map.IntMap.t
  val of_file : string -> t
end
