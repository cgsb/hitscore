#!/usr/bin/env ocamlscript
Ocaml.ocamlflags := [ "-thread"];;
Ocaml.packs := [ "core"];;
--

open Core.Std
let (|>) x f = f x


module Sx = Sexplib.Sexp

module DB_dsl = struct
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
    sprintf "digraph %s {
        graph [fontsize=20 labelloc=\"t\" label=\"\" \
            splines=true overlap=false rankdir = \"LR\"];\n"
      name |> output_string;
    List.iter db (fun table ->
      sprintf "  %s [shape=Mrecord, label=\
        <<table border=\"0\" ><tr><td border=\"1\">%s</td></tr>"
        table.name table.name |> output_string;
      let links = ref [] in
      List.iter table.fields (fun (n, t, al) ->
        sprintf "<tr><td align=\"left\">%s " n |> output_string;
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
        output_string "</td></tr>";
      );
      output_string "</table>>];\n";
      List.iter !links (fun t ->
        sprintf "%s -> %s;\n" table.name t |> output_string
      );
    );
    output_string "}\n"
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
      | sx -> fail  (sprintf "I'm lost parsing properties with: %s\n" 
                       (Sx.to_string sx))
    in
    let parse_field fl =
      match fl with
      | Sx.List ((Sx.Atom name) :: (Sx.Atom typ) :: props) ->
        (name, basic_type_of_string typ, List.map props parse_props)
      | l ->
        fail (sprintf "I'm lost parsing fields with: %s\n" (Sx.to_string l))
          
    in
    let templates = ref [] in
    let parse_entry entry =
      match entry with
      | (Sx.Atom "template") :: (Sx.Atom name) :: l ->
        let fields = List.map ~f:parse_field l in
        templates := (name, fields) :: !templates;
        None
      | (Sx.Atom "table") :: (Sx.Atom name) :: l ->
        let fields = List.map ~f:parse_field l in
        Some {name; fields}
      | (Sx.Atom template) :: (Sx.Atom name) :: l ->
        begin match List.Assoc.find !templates template with
        | Some fs ->
          let fields = fs @ (List.map ~f:parse_field l) in
          Some {name; fields}
        | None ->
          fail (sprintf "Unknown template or command: %s" name)
        end
      | s ->
        fail (sprintf "I'm lost while parsing entry with: %s\n"
                (Sx.to_string (Sx.List s)))
    in
    match sexp with
    | Sx.Atom s -> fail_atom s
    | Sx.List l ->
      List.filter_map l
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
      
end

exception Parse_error of string

type dsl_type =
  | Bool
  | Timestamp
  | Int
  | Real
  | String
  | Option of dsl_type
  | Array of dsl_type 
  | Pointer of string

type typed_value = string * dsl_type

type dsl_runtime_item =
  | Atom of string * typed_value list
  | Function of string * (string * string) list * string

type dsl_runtime = dsl_runtime_item list


let rec string_of_dsl_type = function
  | Bool        -> "Bool"
  | Timestamp   -> "Timestamp"
  | Int         -> "Int"
  | Real        -> "Real"
  | String      -> "String"    
  | Option o    -> sprintf "%s option" (string_of_dsl_type o)
  | Array a     -> sprintf "%s array" (string_of_dsl_type a)
  | Pointer p   -> sprintf "%s ref" p

let parse_sexp sexp =
  let fail msg =
    raise (Parse_error (sprintf "Syntax Error: %s" msg)) in
  let fail_atom s = fail (sprintf "Unexpected atom: %s" s) in
  let existing_types = ref [] in
  let check_type t =
    match List.find !existing_types ~f:((=) t) with
    | Some _ -> (Pointer t)
    | None -> fail (sprintf "Unknown type: %s" t)
  in
  let type_of_string = function
    | "timestamp" -> Timestamp
    | "string" -> String
    | "int" -> Int
    | "real" -> Real
    | "bool" -> Bool
    | s -> check_type s
  in
  let rec parse_2nd_type fst = function
    | [] -> fst
    | Sx.Atom "option" :: l -> parse_2nd_type (Option fst) l
    | Sx.Atom "array" :: l -> parse_2nd_type (Array fst) l
    | sx -> fail  (sprintf "I'm lost parsing types with: %s\n"
                     (Sx.to_string (Sx.List sx)))
  in
  let parse_field fl =
    match fl with
    | Sx.List ((Sx.Atom name) :: (Sx.Atom typ) :: props) ->
      (name, parse_2nd_type (type_of_string typ) props)
    | l ->
      fail (sprintf "I'm lost parsing fields with: %s\n" (Sx.to_string l))

  in
  let parse_entry entry =
    match entry with
    | (Sx.Atom "atom") :: (Sx.Atom name) :: l ->
      let fields = List.map ~f:parse_field l in
      existing_types := name :: !existing_types;
      Atom (name, fields)
    | (Sx.Atom "function") :: (Sx.Atom lout) :: (Sx.Atom name) :: lin ->
      let fields = 
        List.map ~f:(function
          | Sx.List [Sx.Atom name; Sx.Atom typ] ->
            ignore (check_type typ);
            (name, typ)
          | s ->
            fail (sprintf "I'm lost while parsing fun-args with: %s\n"
                    (Sx.to_string s))
        ) lin in
      ignore (check_type lout);
      Function (name, fields, lout)
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
  
let to_db dsl =
  let module DB = DB_dsl in
  List.map dsl (function
    | Atom (name, record) ->
      let user_fields =
        List.map record (fun (n, t) ->
          let typ, props = 
            let props = ref [ DB.Not_null] in
            let rec convert t =
              match t with
              | Bool        -> DB.Bool      
              | Timestamp   -> DB.Timestamp 
              | Int         -> DB.Integer       
              | Real        -> DB.Real      
              | String      -> DB.Text   
              | Option t2 -> 
                props := List.filter !props ~f:((=) DB.Not_null);
                convert t2
              | Array t2 ->
                props := DB.Array :: !props;
                convert t2
              | Pointer s -> DB.Pointer (s, "g_id")
            in
            let converted = convert t in
            (converted, !props)
          in
          (n,  typ, props)) in
      let fields = 
        ("g_id", DB.Identifier, [DB.Not_null]) ::
          ("g_last_accessed", DB.Timestamp, []) ::
          user_fields
      in
      { DB.name ; DB.fields }
    | Function (name, args, result) ->
      let arg_fields =
        List.map args (fun (n, t) ->
          (n, DB.Pointer (t, "g_id"), [DB.Not_null]))
      in
      let fields =
        ("g_id", DB.Identifier, [DB.Not_null]) ::
          ("g_result", DB.Pointer (result, "id"), []) ::
          ("g_recomputable", DB.Bool, []) ::
          ("g_recompute_penalty", DB.Real, []) ::
          ("g_inserted", DB.Timestamp, []) ::
          ("g_started", DB.Timestamp, []) ::
          ("g_completed", DB.Timestamp, []) ::
          ("g_status", DB.Text, []) ::
          arg_fields in
      { DB.name; DB.fields }
  )

      
let digraph dsl ?(name="dsl") output_string =
  sprintf "digraph %s {
        graph [fontsize=20 labelloc=\"t\" label=\"\" \
            splines=true overlap=false rankdir = \"LR\"];\n"
    name |> output_string;
  List.iter dsl (function
    | Atom (name, fields) ->
      sprintf "  %s [shape=record, label=\
        <<table border=\"0\" ><tr><td border=\"1\">%s</td></tr>"
        name name |> output_string;
      let links = ref [] in
      List.iter fields (fun (n, t) ->
        sprintf "<tr><td align=\"left\">%s: %s</td></tr>" n (string_of_dsl_type t) 
                           |> output_string;
        begin match t with
        | Pointer t -> 
          links := t :: !links
        | _ -> ()
        end;
      );
      output_string "</table>>];\n";
      List.iter !links (fun t ->
        sprintf "%s -> %s;\n" name t |> output_string
      );
    | Function (name, args, result) ->
      sprintf "  %s [shape=Mrecord, label=\
        <<table border=\"0\" ><tr><td border=\"4\">%s</td></tr>"
        name name |> output_string;
      let links = ref [] in
      List.iter args (fun (n, t) ->
        sprintf "<tr><td align=\"left\">%s: %s</td></tr>" n t |> output_string;
        links := t :: !links
      );
      sprintf "<tr><td align=\"left\">%s</td></tr>" result |> output_string;
      output_string "</table>>];\n";
      List.iter !links (fun t ->
        sprintf "%s -> %s;\n" name t |> output_string
      );
    );
    output_string "}\n"


let () =
  match Sys.argv |> Array.to_list with
  | exec :: "dbverify" :: file :: [] ->
    In_channel.(with_file file
                  ~f:(fun i -> DB_dsl.verify
                    (input_all i |> DB_dsl.parse_str) (eprintf "%s")))
  | exec :: "dbpostgres" :: file :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let db = DB_dsl.parse_str (In_channel.input_all i) in
        Out_channel.(with_file 
                       ((Filename.basename file) ^ "_init.psql") 
                       ~f:(fun o -> DB_dsl.init_db_postgres db (output_string o)));
        Out_channel.(with_file (Filename.basename file ^ "_clear.psql") 
                       ~f:(fun o -> DB_dsl.clear_db_postgres db (output_string o))))
  | exec :: "dbdraw" :: file :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let db = DB_dsl.parse_str (In_channel.input_all i) in
        Out_channel.(with_file 
                       ((Filename.basename file) ^ "_digraph.dot") 
                       ~f:(fun o -> DB_dsl.digraph db (output_string o))))
  | exec :: "dsldbdraw" :: file :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let db = to_db (parse_str (In_channel.input_all i)) in
        Out_channel.(with_file 
                       ((Filename.basename file) ^ "_digraph.dot") 
                       ~f:(fun o -> DB_dsl.digraph db (output_string o))))
  | exec :: "dsldbpostgres" :: file :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let db = to_db (parse_str (In_channel.input_all i)) in
        Out_channel.(with_file 
                       ((Filename.basename file) ^ "_init.psql") 
                       ~f:(fun o -> DB_dsl.init_db_postgres db (output_string o)));
        Out_channel.(with_file (Filename.basename file ^ "_clear.psql") 
                       ~f:(fun o -> DB_dsl.clear_db_postgres db (output_string o))))
  | exec :: "dsldraw" :: file :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let dsl = parse_str (In_channel.input_all i) in
        Out_channel.(with_file 
                       ((Filename.basename file) ^ "_digraph.dot") 
                       ~f:(fun o -> digraph dsl (output_string o))))
  | _ ->
    eprintf "usage: sexp2db {dbverify, dbpostgres, dbdraw} <sexp_file>\n";
    ()


