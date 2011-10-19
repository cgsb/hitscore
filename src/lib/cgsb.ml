open Batteries_uni;; open Printf;; open Biocaml;; open Sequme
module IntMap = Map.IntMap
module StringSet = Set.StringSet

module Sample = struct
  exception Error of string

  type t = {
    name1 : string;
    investigator : string option;
  }

  let of_row (get : Table.getter) (row : Table.row) =
    {
      name1 = get row "Sample Name1";
      investigator = (
        match get row "investigator" with "" -> None | x -> Some x
      )
    }
end

module Library = struct
  exception Error of string

  type t = {
    sample : Sample.t;
    indexes : Illumina.Barcode.t list;
    read_type : ReadType.t;
    read_length : int;
  }

  let parse_read_length = function
    | "50x50" -> 50
    | "100x100" -> 100
    | x -> Error (sprintf "unknown read length %s" x) |> raise
          
  let of_row (get : Table.getter) (row : Table.row) =
    {
      sample = Sample.of_row get row;
      indexes =
        get row "indexes"
        |> flip String.nsplit ","
        |> List.map (String.strip |- Illumina.Barcode.of_ad_code)
      ;
      read_type = get row "Read Type" |> ReadType.of_string;
      read_length = get row "Read Length" |> parse_read_length;
    }

end

module LibraryDB = struct
  exception Error of string

  type t = Library.t IntMap.t

  let columns = [
    "ID"; "Sample Name1"; "Sample Name2"; "organism"; "investigator";
    "application"; "stranded"; "control_type"; "truseq_control_used";
    "indexes"; "library_submitted"; "Read Type"; "Read Length"
  ]

  let of_file file =
    let add_row get ans row =
      let lib = Library.of_row get row in
      let id = get row "ID" |> int_of_string in
      IntMap.add id lib ans
    in
    let inp = open_in file in
    let _,cols,get,e = Table.of_input ~itags:"table,header,separator=\\t" inp in
    let ans =
      if (cols |> List.enum |> StringSet.of_enum) <> (columns |> List.enum |> StringSet.of_enum) then
        Error "unexpected columns" |> raise
      else
        Enum.fold (add_row get) IntMap.empty e
    in
    close_in inp;
    ans

end

module Flowcell = struct
  exception Error of string

  type record = {
    fcid : string;
    lane : int;
    library : Library.t
  }

  let of_row libdb (get : Table.getter) (row : Table.row) =
    let ans =
      {
        fcid = get row "FCID";
        lane = get row "Lane" |> int_of_string;
        library = IntMap.find (get row "Library ID" |> int_of_string) libdb;
      }
    in
    let s_expected = ans.library.Library.sample.Sample.name1 in
    let s_is = get row "Sample Name1" in
    if s_expected <> s_is then
      Error (sprintf "Sample Name1 %s should be %s" s_is s_expected) |> raise
    else
      ans

end

module FlowcellDB = struct
  exception Error of string

  type t = Flowcell.record list

  let columns = ["FCID"; "Lane"; "Library ID"; "Sample Name1"]

  let of_file libdb file =
    let inp = open_in file in
    let _,cols,get,e = Table.of_input ~itags:"table,header,separator=\\t" inp in
    let ans =
      if (cols |> List.enum |> StringSet.of_enum) <> (columns |> List.enum |> StringSet.of_enum) then
        Error "unexpected columns" |> raise
      else
        Enum.fold (fun ans row -> (Flowcell.of_row libdb get row)::ans) [] e
    in
    close_in inp;
    List.rev ans

end

module BclToFastq = struct
  exception Error of string

  type t = {
    fcid : string;
    mismatch : int;
    sample_sheet_path : string;
  }

  let of_row get dir row =
    {
      fcid = get row "FCID";
      mismatch = get row "mismatch" |> int_of_string;
      sample_sheet_path = Filename.concat dir "SampleSheet.csv"
    }

end

module BclToFastqDB = struct
  exception Error of string

  type t = BclToFastq.t IntMap.t

  let columns = ["ID"; "FCID"; "mismatch"]

  (* TO DO: change this, hard-coded path is BAD! *)
  let dir = "/data/hiseq"

  let of_file file =
    let add_row get ans row =
      let id = get row "ID" |> int_of_string in
      let dir = Filename.concat dir (sprintf "%d" id) in
      let bq = BclToFastq.of_row get dir row in
      IntMap.add id bq ans
    in
    let inp = open_in file in
    let _,cols,get,e = Table.of_input ~itags:"table,header,separator=\\t" inp in
    let ans =
      if (cols |> List.enum |> StringSet.of_enum) <> (columns |> List.enum |> StringSet.of_enum) then
        Error "unexpected columns" |> raise
      else
        Enum.fold (add_row get) IntMap.empty e
    in
    close_in inp;
    ans

end
