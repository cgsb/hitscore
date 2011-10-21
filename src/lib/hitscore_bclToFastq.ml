open Batteries_uni;; open Printf;; open Biocaml
module IntMap = Map.IntMap
module StringSet = Set.StringSet

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

module Database = struct
  exception Error of string

  type s = t
  type t = s IntMap.t

  let columns = ["ID"; "FCID"; "mismatch"]

  (* TO DO: change this, hard-coded path is BAD! *)
  let dir = "/data/hiseq"

  let of_file file =
    let add_row get ans row =
      let id = get row "ID" |> int_of_string in
      let dir = Filename.concat dir (sprintf "%d" id) in
      let bq = of_row get dir row in
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
