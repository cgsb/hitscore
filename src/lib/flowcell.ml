open Batteries_uni;; open Printf;; open Biocaml
module IntMap = Map.IntMap
module StringSet = Set.StringSet

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

module Database = struct
  exception Error of string

  type t = record list

  let columns = ["FCID"; "Lane"; "Library ID"; "Sample Name1"]

  let of_file libdb file =
    let inp = open_in file in
    let _,cols,get,e = Table.of_input ~itags:"table,header,separator=\\t" inp in
    let ans =
      if (cols |> List.enum |> StringSet.of_enum) <> (columns |> List.enum |> StringSet.of_enum) then
        Error "unexpected columns" |> raise
      else
        Enum.fold (fun ans row -> (of_row libdb get row)::ans) [] e
    in
    close_in inp;
    List.rev ans

end
