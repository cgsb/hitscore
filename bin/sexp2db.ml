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
        <<table border=\"0\" ><tr><td border=\"1\" colspan=\"2\">%s</td></tr>"
        table.name table.name |> output_string;
      let links = ref [] in
      List.iter table.fields (fun (n, t, al) ->
        sprintf "<tr><td align=\"left\">%s </td><td align=\"left\">: " n
                                 |> output_string;
        begin match t with
        | Timestamp      -> "Timestamp"      
        | Identifier     -> "Identifier"     
        | Text           -> "Text"           
        | Pointer (t, f) -> links := t :: !links; sprintf "%s:%s" t f
        | Integer        -> "Integer"        
        | Real           -> "Real"           
        | Bool           -> "Bool"
        end |> output_string;
        List.map al (function
          | Not_null ->  " non null" 
          | Array ->  " array"
          | Unique -> " unique") |> String.concat ~sep:""
                                 |> output_string;
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
  | Value of string * typed_value list
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

let rec type_is_pointer = function
  | Bool         -> `no
  | Timestamp    -> `no
  | Int          -> `no
  | Real         -> `no
  | String       -> `no
  | Option o     -> type_is_pointer o
  | Array a      -> type_is_pointer a
  | Pointer p -> `yes p

let rec type_is_option = function
  | Bool         -> `no
  | Timestamp    -> `no
  | Int          -> `no
  | Real         -> `no
  | String       -> `no
  | Option o     -> `yes
  | Array a      -> type_is_option a
  | Pointer p -> `no

let sanitize s =
  String.iter s ~f:(function
    | 'a' .. 'z' | '_' | '0' .. '9' | 'A' .. 'Z' -> ()
    | c -> 
      sprintf "In name %S, char %C is forbidden" s c |> failwith
  );
  begin match String.get s 0 with
  | 'a' .. 'z' -> ()
  | _ -> 
    sprintf "%S should start with a lowercase letter" s |> failwith
  end;
  if String.is_prefix s ~prefix:"g_" then
    sprintf "%S should not start with \"g_\"" s |> failwith;
  begin match s with
  | "type" | "val" | "let" | "in" | "with" | "match" | "null" ->
    sprintf "%S is not a good name …" s |> failwith
  | _ -> ()
  end
 
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
      sanitize name;
      sanitize typ;
      (name, parse_2nd_type (type_of_string typ) props)
    | l ->
      fail (sprintf "I'm lost parsing fields with: %s\n" (Sx.to_string l))

  in
  let parse_entry entry =
    match entry with
    | (Sx.Atom "value") :: (Sx.Atom name) :: l ->
      sanitize name;
      let fields = List.map ~f:parse_field l in
      existing_types := name :: !existing_types;
      Value (name, fields)
    | (Sx.Atom "function") :: (Sx.Atom lout) :: (Sx.Atom name) :: lin ->
      sanitize name;
      sanitize lout;
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
    | Value (name, record) ->
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
          ("g_result", DB.Pointer (result, "g_id"), []) ::
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
    | Value (name, fields) ->
      sprintf "  %s [shape=record, label=\
        <<table border=\"0\" ><tr><td border=\"1\">%s</td></tr>"
        name name |> output_string;
      let links = ref [] in
      List.iter fields (fun (n, t) ->
        sprintf "<tr><td align=\"left\">%s: %s</td></tr>" n (string_of_dsl_type t) 
                           |> output_string;
        begin match type_is_pointer t with
        | `yes p -> links := p :: !links
        | `no -> ()
        end
      );
      output_string "</table>>];\n";
      List.iter !links (fun t ->
        sprintf "%s -> %s [color=\"#888888\"];\n" name t |> output_string
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
      sprintf "<tr><td align=\"left\"><i>%s</i></td></tr>" result |> output_string;
      output_string "</table>>];\n";
      List.iter !links (fun t ->
        sprintf "%s -> %s [style=dashed, color=\"#880000\"];\n" name t |> 
            output_string
      );
      sprintf "%s -> %s [color=\"#008800\"];\n" name result |> output_string
    );
    output_string "}\n"


let ocaml_code dsl output_string =
  List.iter dsl (function
    | Value (name, fields) ->
      (* Function to add a new value: *)
      sprintf "let add_value_%s\n" name |> output_string;
      List.iter fields (fun (n, t) ->
        let kind_of_arg =
          match type_is_option t with `yes -> "?" | `no -> "~" in
        let want_id =
          match type_is_pointer t with `yes _ -> "_id" | `no -> "" in
        sprintf "    %s%s%s\n" kind_of_arg n want_id |> output_string;
      );
      let intos =
        "g_last_accessed" :: List.map fields fst |> String.concat ~sep:", " in
      let values =
        let prefix (n, t) =
          (match type_is_option t with `yes -> "$?" | `no -> "$") ^
            n ^
            (match type_is_pointer t with `yes _ -> "_id" | `no -> "")
        in
        "now()" :: List.map fields prefix |> String.concat ~sep:", " in
      sprintf "    db_handle =\n  PGSQL (db_handle)\n" |> output_string;
      sprintf "    \"INSERT INTO %s (%s)\\\n     VALUES (%s)\\\n\
                   \     RETURNING g_id \"\n\n" 
        name intos values |> output_string;
    | Function (name, args, result) ->
      (* Function to insert a new function evaluation: *)
      sprintf "let add_evaluation_of_%s\n" name |> output_string;
      List.iter args (fun (n, t) -> output_string ("    ~" ^ n ^ "_id\n"));
      output_string "    ?(recomputable=false)\n";
      output_string "    ?(recompute_penalty=0.)\n";
      output_string "    db_handle =\n  PGSQL (db_handle)\n";
      let intos =
        "g_recomputable" :: "g_recompute_penalty" :: "g_inserted" :: "g_status" :: 
          (List.map args fst) |> String.concat ~sep:", " in
      let values =
        "$recomputable" :: "$recompute_penalty" :: "now ()" :: "'INSERTED'" ::
          (List.map args (fun (n, t) -> "$" ^ n ^ "_id")) |>
              String.concat ~sep:", " in
      sprintf "    \"INSERT INTO %s (%s)\\\n     VALUES (%s)\\\n\
                   \     RETURNING g_id \"\n\n" 
        name intos values |> output_string;
      (* Function to set the state of a function evaluation to 'STARTED': *)
      sprintf "let set_%s_started ~id db_handle =\n" name |> output_string;
      output_string "  PGSQL (db_handle)\n";
      sprintf "    \"UPDATE %s SET g_status = 'STARTED', g_started = now ()\n\
              \    WHERE g_id = $id\"\n\n" name |> output_string

  );
  ()


let () =
  match Sys.argv |> Array.to_list with
  | exec :: "all" :: file :: prefix :: [] ->
    let dsl =
      In_channel.(with_file file ~f:(fun i -> parse_str (input_all i))) in
    Out_channel.(
      with_file (prefix ^ (Filename.basename file) ^ "_digraph.dot") 
        ~f:(fun o -> digraph dsl (output_string o))
    );
    let db = to_db dsl in
    eprintf "Verification:\n";
    DB_dsl.verify db (eprintf "%s");
    eprintf "Done.\n";
    let open Out_channel in
    with_file (prefix ^ (Filename.basename file) ^ "_dsl_digraph.dot") 
      ~f:(fun o -> DB_dsl.digraph db (output_string o));
      
    with_file (prefix ^ (Filename.basename file) ^ "_init.psql") 
      ~f:(fun o -> DB_dsl.init_db_postgres db (output_string o));
    
    with_file (prefix ^ (Filename.basename file) ^ "_clear.psql") 
      ~f:(fun o -> DB_dsl.clear_db_postgres db (output_string o));
    
    with_file (prefix ^ (Filename.basename file) ^ "_code.ml") 
      ~f:(fun o -> ocaml_code dsl (output_string o));


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
    eprintf "usage: \n\
       sexp2db {dbverify, dbpostgres, dbdraw, ...} <sexp_file>\n\
       sexp2db all <sexp_file> <outprefix>\n";
    ()


