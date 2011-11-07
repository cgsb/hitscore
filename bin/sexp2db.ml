#!/usr/bin/env ocamlscript
Ocaml.ocamlflags := [ "-thread"];;
Ocaml.packs := [ "core";];;
--

open Core.Std
let (|>) x f = f x


module Sx = Sexplib.Sexp

exception Parse_error of string

type basic_type =
  | Timestamp
  | Identifier
  | Text
  | Pointer of string * string
  | Integer
  | Real
  | Bool
type property =
  | Array
  | Not_null
  | Unique
type properties = property list

type table = {
  name: string;
  fields: (string * basic_type * properties) list;
}
type db = table list

let parse_sexp sexp =
  let fail msg =
    raise (Parse_error (sprintf "Syntax Error: %s" msg)) in
  let fail_atom s = fail (sprintf "Unexpected atom: %s" s) in
  let basic_type_of_string = function
    | "timestamp" -> Timestamp
    | "identifier" -> Identifier
    | "text" -> Text
    | "integer" -> Integer
    | "real" -> Real
    | "bool" -> Bool
    | s -> 
      begin match String.split s ~on:':' with
      | [ table; field ] -> Pointer (table, field)
      | _ ->
        fail (sprintf "Unknown type: %s" s)
      end
  in
  let parse_props = function
    | Sx.Atom "not_null" -> Not_null
    | Sx.Atom "unique" -> Unique
    | Sx.Atom "array" -> Array
    | sx -> fail  (sprintf "I'm lost parsing properties with: %s\n" (Sx.to_string sx))
  in
  let parse_field fl =
    match fl with
    | Sx.List ((Sx.Atom name) :: (Sx.Atom typ) :: props) ->
      (name, basic_type_of_string typ, List.map props parse_props)
    | l ->
      fail (sprintf "I'm lost parsing fields with: %s\n" (Sx.to_string l))

  in
  let parse_entry entry =
    match entry with
    | (Sx.Atom "table") :: (Sx.Atom name) :: l ->
      let fields = List.map ~f:parse_field l in
      {name; fields}
    | s ->
      fail (sprintf "I'm lost while parsing entry with: %s\n"
              (Sx.to_string (Sx.List s)))
  in
  match sexp with
  | Sx.Atom s -> fail_atom s
  | Sx.List l ->
    List.map l
      ~f:(function
        | Sx.Atom s -> fail_atom s
        | Sx.List l -> parse_entry l)



let parse_str str =
  let sexp = 
    try Sx.of_string (sprintf "(%s)" str) 
    with Failure msg ->
      raise (Parse_error (sprintf "Syntax Error (sexplib): %s" msg))
  in
  (parse_sexp sexp)
  

let init_db_postgres db output_string =
  List.iter db (fun table ->
    output_string "CREATE TABLE ";
    output_string table.name;
    output_string "\n  (";
    output_string
      (String.concat ~sep:",\n  "
         (List.map table.fields (fun (n, t, pl) ->
           sprintf "%s %s%s" 
             n 
             (match t with
             | Timestamp -> "timestamptz"
             | Identifier -> "serial PRIMARY KEY"
             | Text -> "text"
             | Pointer (t, f) -> sprintf "integer"
             | Integer -> "integer"
             | Bool -> "bool"
             | Real -> "real") 
             (String.concat ~sep:""
                (List.map pl ~f:(function
                  | Not_null -> " NOT NULL"
                  | Array -> " ARRAY"
                  | Unique -> " UNIQUE"))))));
    output_string ");\n";
  )


let clear_db_postgres db output_string =
  List.iter db (fun table ->
    output_string (sprintf "DROP TABLE %s;\n" table.name))

let verify db output_string =
  List.iter db (fun table ->
    begin match List.find_all table.fields 
        ~f:(fun (_, t, _) -> t = Identifier) with
    | [ one ] -> ()
    | [] ->
      sprintf "table %s has no identifier.\n" table.name |> output_string
    | l ->
      sprintf "table %s has %d identifiers.\n" table.name (List.length l) |>
          output_string
    end;
    List.iter table.fields (function 
      | (n, Pointer (tab, fi), _) ->
        begin match List.find db ~f:(fun t -> t.name = tab) with
        | Some t ->
          begin match List.find
              t.fields ~f:(fun (n, t, _) -> n = fi && t = Identifier) with
              | Some t ->
              (* OK Nothing To do *) ()
              | None ->
                output_string 
                  (sprintf "%s(%s): pointer to wrong field %s(%s).\n"
                     table.name n tab fi)
          end
        | None ->
          output_string 
            (sprintf "%s(%s): pointer to wrong table %s(%s).\n"
               table.name n tab fi)
        end
      | _ -> ())
  )

let digraph db ?(name="db") output_string =
  sprintf "digraph %s {\n" name |> output_string;
  List.iter db (fun table ->
    sprintf "  %s [shape=record, label=\
        <<table border=\"0\"><tr><td border=\"1\">%s</td></tr><tr><td>"
      table.name table.name |> output_string;
    let links = ref [] in
    List.iter table.fields (fun (n, t, al) ->
      sprintf "%s " n |> output_string;
      begin match t with
      | Identifier -> output_string "*"
      | Pointer (t, f) -> 
        output_string "&amp;";
        links := t :: !links
      | _ -> ()
      end;
      List.iter al (function
        | Not_null -> output_string ""
        | Array -> output_string "[]"
        | Unique -> output_string "!");
      output_string "<br/>";
    );
    output_string "</td></tr></table>>];\n";
    List.iter !links (fun t ->
      sprintf "%s -> %s;\n" table.name t |> output_string
    );
  );
  output_string "}\n"



let () =
  match Sys.argv |> Array.to_list with
  | exec :: "verify" :: file :: [] ->
    In_channel.(with_file file
                  ~f:(fun i -> verify (input_all i |> parse_str) (eprintf "%s")))
  | exec :: "postgres" :: file :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let db = parse_str (In_channel.input_all i) in
        Out_channel.(with_file 
                       ((Filename.basename file) ^ "_init.psql") 
                       ~f:(fun o -> init_db_postgres db (output_string o)));
        Out_channel.(with_file (Filename.basename file ^ "_clear.psql") 
                       ~f:(fun o -> clear_db_postgres db (output_string o))))
  | exec :: "draw" :: file :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let db = parse_str (In_channel.input_all i) in
        Out_channel.(with_file 
                       ((Filename.basename file) ^ "_digraph.dot") 
                       ~f:(fun o -> digraph db (output_string o))))
  | _ ->
    eprintf "usage: sexp2db {verify, postgres, draw} <sexp_file>\n";
    ()


