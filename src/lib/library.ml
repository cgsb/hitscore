open Batteries_uni;; open Printf;; open Biocaml;; open Sequme
module IntMap = Map.IntMap
module StringSet = Set.StringSet

exception Error of string

type t = {
  sample : Sample.t;
  indexes : Illumina.Barcode.t list;
  read_type : Read_type.t;
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
    read_type = get row "Read Type" |> Read_type.of_string;
    read_length = get row "Read Length" |> parse_read_length;
  }


module Database = struct
  exception Error of string

  type s = t
  type t = s IntMap.t

  let columns = [
    "ID"; "Sample Name1"; "Sample Name2"; "organism"; "investigator";
    "application"; "stranded"; "control_type"; "truseq_control_used";
    "indexes"; "library_submitted"; "Read Type"; "Read Length"
  ]

  let of_file file =
    let add_row get ans row =
      let lib = of_row get row in
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
