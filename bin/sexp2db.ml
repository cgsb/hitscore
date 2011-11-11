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
            splines=true overlap=false ];\n"
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
  | Record_name of string
  | Enumeration_name of string
  | Function_name of string

type typed_value = string * dsl_type

type dsl_runtime_type =
  | Enumeration of string * string list
  | Record of string * typed_value list
  | Function of string * typed_value list * string
      
type dsl_runtime_description = {
  nodes: dsl_runtime_type list;
  types: dsl_type list; (* TODO clarify what's in there! *)
}

let built_in_complex_types = [
  Enumeration ("process_status",
               [ "Started"; "Inserted"; "Failed"; "Succeeded" ]);
]

let type_name = function
  | Enumeration (n, _) -> Enumeration_name n
  | Record (n, _) -> Record_name n
  | Function (n, _, _) -> Function_name n

let built_in_types =
  List.map built_in_complex_types ~f:type_name

let rec string_of_dsl_type = function
  | Bool        -> "Bool"
  | Timestamp   -> "Timestamp"
  | Int         -> "Int"
  | Real        -> "Real"
  | String      -> "String"    
  | Option o    -> sprintf "%s option" (string_of_dsl_type o)
  | Array a     -> sprintf "%s array" (string_of_dsl_type a)
  | Pointer p   -> sprintf "%s ref" p
  | Record_name       n -> n |> sprintf "%s:rec" 
  | Enumeration_name  n -> n |> sprintf "%s:enum" 
  | Function_name     n -> n |> sprintf "%s:fun" 

let rec type_is_pointer = function
  | Bool         -> `no
  | Timestamp    -> `no
  | Int          -> `no
  | Real         -> `no
  | String       -> `no
  | Option o     -> type_is_pointer o
  | Array a      -> type_is_pointer a
  | Pointer p -> `yes p
  | Record_name       n -> `yes n
  | Enumeration_name  n -> `no
  | Function_name     n -> `yes n

(* A link is semantic: a pointer, or a enum-name, etc. *)
let rec type_is_link = function
  | Bool      
  | Timestamp 
  | Int       
  | Real      
  | String       -> `no
  | Option o     -> type_is_link o
  | Array a      -> type_is_link a
  | Pointer           n
  | Record_name       n
  | Enumeration_name  n
  | Function_name     n -> `yes n

let rec type_is_option = function
  | Bool         -> `no
  | Timestamp    -> `no
  | Int          -> `no
  | Real         -> `no
  | String       -> `no
  | Option o     -> `yes
  | Array a      -> type_is_option a
  | Pointer p -> `no
  | Record_name       n -> `no
  | Enumeration_name  n -> `no
  | Function_name     n -> `no

let find_type l t =
  List.find l ~f:(function
    | Record_name       n -> n = t
    | Enumeration_name  n -> n = t
    | Function_name     n -> n = t
    | _ -> false)



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
  let subrecord_macros = ref [] in
  let check_type t =
    match find_type !existing_types t with
    | Some t -> t
    | _ -> fail (sprintf "Unknown type: %s" t)
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
    | Sx.Atom "array" :: l ->
      begin match fst with 
      | Int | Record_name _ ->
        parse_2nd_type (Array fst) l
      | _ ->
        sprintf "PGOCaml will only accept arrays of ints or records, \
                 so I stop there (%s array)." (string_of_dsl_type fst)
            |> fail
      end
    | sx -> fail  (sprintf "I'm lost parsing types with: %s\n"
                     (Sx.to_string (Sx.List sx)))
  in
  let parse_field fl =
    match fl with
    | Sx.List ((Sx.Atom name) :: (Sx.Atom typ) :: props) ->
      sanitize name;
      sanitize typ;
      [(name, parse_2nd_type (type_of_string typ) props)]
    | Sx.Atom n ->
      begin match List.find !subrecord_macros (fun (nn, _) -> n = nn) with
      | Some tv -> snd tv
      | None ->
        sprintf "Unknown sub-record-macro: %S" n |> fail
      end
    | l ->
      fail (sprintf "I'm lost parsing fields with: %s\n" (Sx.to_string l))

  in
  let parse_entry entry =
    match entry with
    | (Sx.Atom "subrecord") :: (Sx.Atom name) :: l ->
      sanitize name;
      let fields = List.map ~f:parse_field l |> List.flatten in
      subrecord_macros := (name, fields) :: !subrecord_macros;
      None
    | (Sx.Atom "record") :: (Sx.Atom name) :: l ->
      sanitize name;
      let fields = List.map ~f:parse_field l |> List.flatten in
      existing_types := Record_name name :: !existing_types;
      Some (Record (name, fields))
    | (Sx.Atom "function") :: (Sx.Atom lout) :: (Sx.Atom name) :: lin ->
      sanitize name;
      sanitize lout;
      let fields = List.map ~f:parse_field lin |> List.flatten in
      begin match check_type lout with
      | Record_name r -> ()
      | t -> 
        fail (sprintf "A function can only return a record, not %S like %s does."
                (string_of_dsl_type t) name)
      end;
      existing_types := Function_name name :: !existing_types;
      Some (Function (name, fields, lout))
    | (Sx.Atom "enumeration") :: (Sx.Atom name) :: lin ->
      existing_types := Enumeration_name name :: !existing_types;
      Some (Enumeration (name,
                         List.map lin
                           (function Sx.Atom a -> a 
                             | _ -> 
                               fail (sprintf "Enumeration %s has wrong format" name))))
    | s ->
      fail (sprintf "I'm lost while parsing entry with: %s\n"
              (Sx.to_string (Sx.List s)))
  in
  match sexp with
  | Sx.Atom s -> fail_atom s
  | Sx.List l ->
    let user_nodes =
      List.filter_map l
        ~f:(function
          | Sx.Atom s -> fail_atom s
          | Sx.List l -> parse_entry l) in
    { nodes = built_in_complex_types @ user_nodes; 
      types = List.rev !existing_types }
    

let parse_str str =
  let sexp = 
    try Sx.of_string (sprintf "(%s)" str) 
    with Failure msg ->
      raise (Parse_error (sprintf "Syntax Error (sexplib): %s" msg))
  in
  (parse_sexp sexp)

module DB = DB_dsl
  
let dsl_type_to_db t =
  let props = ref [ DB.Not_null] in
  let rec convert t =
    match t with
    | Bool        -> DB.Bool      
    | Timestamp   -> DB.Timestamp 
    | Int         -> DB.Integer       
    | Real        -> DB.Real      
    | String      -> DB.Text   
    | Option t2 -> 
      props := List.filter !props ~f:((<>) DB.Not_null);
      convert t2
    | Array t2 ->
      props := DB.Array :: !props;
      convert t2
    | Pointer s -> DB.Pointer (s, "g_id")
    | Function_name s -> DB.Pointer (s, "g_id")
    | Enumeration_name s -> DB.Text
    | Record_name s -> DB.Pointer (s, "g_id")
  in
  let converted = convert t in
  (converted, !props)

let db_record_standard_fields = [
  ("g_id", DB.Identifier, [DB.Not_null]);
  ("g_last_accessed", DB.Timestamp, []);
]
let db_function_standard_fields result = [   
  ("g_id", DB.Identifier, [DB.Not_null]);
  ("g_result", DB.Pointer (result, "g_id"), []);
  ("g_recomputable", DB.Bool, [DB.Not_null]);
  ("g_recompute_penalty", DB.Real, []);
  ("g_inserted", DB.Timestamp, []);
  ("g_started", DB.Timestamp, []);
  ("g_completed", DB.Timestamp, []);
  ("g_status", DB.Text, [DB.Not_null]);
]

let to_db dsl =
  List.map dsl.nodes (function
    | Record (name, record) ->
      let user_fields =
        List.map record (fun (n, t) ->
          let typ, props = dsl_type_to_db t in
          (n,  typ, props)) in
      let fields = db_record_standard_fields @ user_fields
      in
      [{ DB.name ; DB.fields }]
    | Function (name, args, result) ->
      let arg_fields =
        List.map args (fun (n, t) ->
          let typ, props = dsl_type_to_db t in
          (n,  typ, props)) in
      let fields = (db_function_standard_fields result) @ arg_fields in
      [ { DB.name; DB.fields } ]
    | Enumeration _ -> []
  ) |> List.flatten

      
let digraph dsl ?(name="dsl") output_string =
  sprintf "digraph %s {
        graph [fontsize=20 labelloc=\"t\" label=\"\" \
            splines=true overlap=false ];\n"
    name |> output_string;
  List.iter dsl.nodes (function
    | Record (name, fields) ->
      sprintf "  %s [shape=record, label=\
        <<table border=\"0\" ><tr><td border=\"2\">%s</td></tr>"
        name name |> output_string;
      let links = ref [] in
      List.iter fields (fun (n, t) ->
        sprintf "<tr><td align=\"left\">%s: %s</td></tr>" n (string_of_dsl_type t) 
                           |> output_string;
        begin match type_is_link t with
        | `yes p -> links := p :: !links
        | `no -> ()
        end
      );
      output_string "</table>>];\n";
      List.iter !links (fun t ->
        sprintf "%s -> %s [style=\"setlinewidth(3)\",color=\"#888888\"];\n"
          name t |> output_string
      );
    | Function (name, args, result) ->
      sprintf "  %s [shape=Mrecord,color=blue,label=\
        <<table border=\"0\" ><tr><td border=\"2\">%s</td></tr>"
        name name |> output_string;
      let links = ref [] in
      List.iter args (fun (n, t) ->
        sprintf "<tr><td align=\"left\">%s: %s</td></tr>" 
          n (string_of_dsl_type t) |> output_string;
        begin match type_is_link t with
        | `yes p -> links := p :: !links
        | `no -> ()
        end
      );
      sprintf "<tr><td align=\"left\"><i>%s</i></td></tr>" result |> output_string;
      output_string "</table>>];\n";
      List.iter !links (fun t ->
        sprintf "%s -> %s [penwidth=3,style=dashed,\
                 color=\"#880000\"];\n" t name |> 
            output_string
      );
      sprintf "%s -> %s [penwidth=3,color=\"#008800\"];\n" name result |> output_string
    | Enumeration (name, items) ->
      sprintf "%s [label=\"%s =\\l  | %s\\l\"];\n\n"
        name name 
        (String.concat ~sep:"\\l  | " items) |> output_string
    );
    output_string "}\n"


let pgocaml_type_of_field =
  let rec props  = function
    | [ DB.Array    ; DB.Not_null] -> sprintf "PGOCaml.%s_array"
    | [ DB.Not_null ] -> sprintf "%s"
    | _ -> sprintf "%s option" in
  function
    | (_, DB.Timestamp          , p) -> (props p) "PGOCaml.timestamptz"
    | (_, DB.Identifier         , p) -> (props p) "int32"              
    | (_, DB.Text               , p) -> (props p) "string"             
    | (_, DB.Pointer (r, "g_id"), p) -> (props p) "int32"              
    | (_, DB.Integer            , p) -> (props p) "int32"              
    | (_, DB.Real               , p) -> (props p) "float"              
    | (_, DB.Bool               , p) -> (props p) "bool"               
    | _ -> failwith "Can't compile DB type to PGOCaml"
let rec ocaml_type = function
  | Bool        -> "bool"
  | Timestamp   -> "PGOCaml.timestamptz"
  | Int         -> "int32"
  | Real        -> "float"      
  | String      -> "string"
  | Option t -> sprintf "%s option" (ocaml_type t)
  | Array t -> sprintf "%s array" (ocaml_type t)
  | Pointer s -> "int32"
  | Function_name s -> sprintf "Function_%s.t" s
  | Enumeration_name s -> sprintf "Enumeration_%s.t" s
  | Record_name s -> sprintf "Record_%s.t" s

let let_in_typed_value  = function
  | (n, Enumeration_name e) -> 
    sprintf "  let %s = Enumeration_%s.to_string %s in\n" n e n
  | (n, Record_name r) ->
    sprintf "  let %s = %s.Record_%s.id in\n" n n r
  | (n, Option (Record_name r)) ->
    sprintf "  let %s = option_map %s (fun s -> s.Record_%s.id) in\n" n n r
  | (n, Array (Record_name r)) ->
    sprintf "  let %s = array_map %s (fun s -> s.Record_%s.id) in\n" n n r
  | _ -> ""

let convert_pgocaml_type = function
  | (n, Enumeration_name e) -> 
    sprintf "(Enumeration_%s.of_string_exn %s)" e n
  | (n, Record_name r) ->
    sprintf "{ Record_%s.id = %s }" r n
  | (n, Option (Record_name r)) ->
    sprintf "(option_map %s (fun id -> { Record_%s.id }))" n r
  | (n, Array (Record_name r)) ->
    sprintf "(array_map %s (fun id ->  { Record_%s.id }))" n r
  | (n, _) -> n

let ocaml_code dsl output_string =
  let print = ksprintf and out = output_string in
  print out "(** Autogenerated module *)\n\n";
  print out "type db_handle = (string, bool) Hashtbl.t PGOCaml.t\n\n";
  print out "type function_capabilities= [\n\
                \  | `can_nothing\n  | `can_start\n  | `can_complete\n\
                | `can_get_result\n]\n";
  print out "(**/**)\n\
                \ let option_map o f =\n\
                \     match o with None -> None | Some s -> Some (f s)\n\n\
                \ let array_map a f = Array.map f a\n\n\
                \ let pg_bind_bind amm f =\n\
                \   PGOCaml.bind amm (fun am -> PGOCaml.bind am f)\n\n\
                \ let pg_return t = PGOCaml.return t\n\n\
                (**/**)\n";
  List.iter dsl.nodes (function
    | Enumeration (name, fields) ->
      print out "module Enumeration_%s = struct\n" name;
      print out "type t = [%s]\n\n"
        (List.map fields (sprintf "`%s") |> String.concat ~sep:" | ");
      print out "let to_string : t -> string = function\n| %s\n" 
        (List.map fields (fun s -> sprintf "`%s -> \"%s\"" s s) |>
            String.concat ~sep:"\n| ");
      print out "\n";
      print out "let of_string_exn: string -> t = function\n| %s\n" 
        (List.map fields (fun s -> sprintf "\"%s\" -> `%s" s s) |>
            String.concat ~sep:"\n| ");
      print out "| s -> failwith (Printf.sprintf \"%s_of_string_exn: \
               input error: %%s\" s)\n\n" name;
      print out "end (* %s *)\n\n" name
    | Record (name, fields) ->
      print out "module Record_%s = struct\n" name;
      print out "type t = { id: int32 }\n";
      (* Function to add a new value: *)
      print out "let add_value\n";
      List.iter fields (fun (n, t) ->
        let kind_of_arg = 
          match type_is_option t with `yes -> "?" | `no -> "~" in
        print out "    %s(%s:%s)\n" kind_of_arg n (ocaml_type t);
      );
      let intos =
        "g_last_accessed" :: List.map fields fst |> String.concat ~sep:", " in
      let values =
        let prefix (n, t) =
          (match type_is_option t with `yes -> "$?" | `no -> "$") ^ n in
        "now()" :: List.map fields prefix |> String.concat ~sep:", " in
      print out "    (dbh:db_handle) : t PGOCaml.monad PGOCaml.monad =\n";
      List.iter fields (fun tv -> let_in_typed_value tv |> print out "%s");
      print out "  let id_list_monad_monad = PGSQL (dbh)\n";
      print out "    \"INSERT INTO %s (%s)\\\n     VALUES (%s)\\\n\
                   \     RETURNING g_id \" in\n" name intos values;
      print out "  PGOCaml.bind id_list_monad_monad \n\
                    \    (fun id_list_monad ->\n\
                    \      PGOCaml.bind id_list_monad (function\n\
                    \       | [ id ] -> PGOCaml.return { id }\n\
                    \       | _ -> failwith \"Insert did not return one id\"))\n\n";
      (* Access a value *)
      print out "let get_value_by_id ~id (dbh:db_handle) =\n";
      print out "  PGSQL (dbh)\n";
      print out "    \"SELECT * FROM %s WHERE g_id = $id\"\n\n" name;
      (* Delete a value *)
      print out "let delete_value_by_id ~id (dbh:db_handle) =\n";
      print out "  PGSQL (dbh)\n";
      print out "    \"DELETE FROM %s WHERE g_id = $id\"\n\n" name;
      print out "end (* %s *)\n\n" name;
    | Function (name, args, result) ->
      print out "module Function_%s = struct\n" name;
      print out "type 'a t = (* private *) { id: int32 }\n";
      (* Function to insert a new function evaluation: *)
      print out "let add_evaluation\n";
      List.iter args (fun (n, t) -> print out "    ~%s\n" n);
      print out "    ?(recomputable=false)\n";
      print out "    ?(recompute_penalty=0.)\n";
      print out "    (dbh:db_handle) : \n\
            \    [ `can_start | `can_complete ] t PGOCaml.monad PGOCaml.monad =\n";
      List.iter args (fun tv -> let_in_typed_value tv |> print out "%s");
      print out "  let id_list_monad_monad = PGSQL (dbh)\n";
      let intos =
        "g_recomputable" :: "g_recompute_penalty" :: "g_inserted" :: "g_status" :: 
          (List.map args fst) |> String.concat ~sep:", " in
      let values =
        "$recomputable" :: "$recompute_penalty" :: "now ()" :: "'Inserted'" ::
          (List.map args (fun (n, t) -> "$" ^ n)) |> String.concat ~sep:", " in
      print out "    \"INSERT INTO %s (%s)\\\n     VALUES (%s)\\\n\
                   \     RETURNING g_id \" in\n" name intos values;
      print out "  PGOCaml.bind id_list_monad_monad \n\
                    \    (fun id_list_monad ->\n\
                    \      PGOCaml.bind id_list_monad (function\n\
                    \       | [ id ] -> PGOCaml.return { id }\n\
                    \       | _ -> failwith \"Insert did not return one id\"))\n\n";
      (* Function to set the state of a function evaluation to 'STARTED': *)
      print out "let set_started (t : [> `can_start] t) (dbh:db_handle) =\n";
      print out "  let id = t.id in\n";
      print out "  let umm = PGSQL (dbh)\n";
      print out "    \"UPDATE %s SET g_status = 'Started', g_started = now ()\n\
                \    WHERE g_id = $id\" in\n\
                \    pg_bind_bind umm (fun () -> \
                pg_return ({ id } : [ `can_complete] t)) 
                \n\n\n" name;
      print out "let set_succeeded (t : [> `can_complete] t) \n\
                   \     ~result
                    \    (dbh:db_handle) =\n";
      let_in_typed_value ("result", Record_name result) |> print out "%s";
      print out "  let id = t.id in\n";
      print out "  let umm = PGSQL (dbh)\n";
      print out "    \"UPDATE %s SET g_status = 'Succeeded', \
                g_completed = now (), g_result = $result\n \
                \    WHERE g_id = $id\" in\n\
                \    pg_bind_bind umm (fun () -> \
                pg_return ({ id } : [ `can_get_result] t)) 
                \n\n\n" name;
      print out "let set_failed \
                   (t : [ `can_start | `can_complete] t) (dbh:db_handle) =\n";
      print out "  let id = t.id in\n";
      print out "  let umm = PGSQL (dbh)\n";
      print out "    \"UPDATE %s SET g_status = 'Failed', g_completed = now ()\n\
                \    WHERE g_id = $id\" in\n\
                \    pg_bind_bind umm (fun () -> \
                pg_return ({ id } : [ `can_nothing ] t)) 
                \n\n\n" name;
      (* print out "(\**/**\)\n"; *)
      let args_in_db = 
        List.map args (fun (s, t) ->
          let (t, p) = dsl_type_to_db t in (s, t, p)) in
      print out "(**/**)\ntype 'a _cache = \n(%s)\n(**/**)\n\n"
        (List.map (db_function_standard_fields result @ args_in_db)
           ~f:pgocaml_type_of_field |> String.concat ~sep:" *\n ");
      print out "type 'a cache = 'a _cache\n\n";
      (* Access a function *)
      print out "let get_evaluation (t: 'a t) (dbh:db_handle):\
                 'a cache PGOCaml.monad PGOCaml.monad =\n";
      print out "  let id = t.id in\n";
      print out "  let umm = PGSQL (dbh)\n";
      print out "    \"SELECT * FROM %s WHERE g_id = $id\" in\n" name;
      print out "  pg_bind_bind umm (function [one] -> pg_return one 
         | _ -> failwith \"get_evaluation(%s) did not \
         retrieve ONE element\")\n" name;

      print out "type arguments = {\n";
      List.iter args (fun (v,t) -> 
        print out "  %s: %s;\n" v (ocaml_type t));
      print out "}\n";
      print out "let get_arguments (cache : 'a cache) =\n";
      print out "  let (%s, %s) = cache in\n"
        (List.map (db_function_standard_fields result) (fun _ -> "_") |> 
            String.concat ~sep:", ")
        (List.map args (fun (s, t) -> s) |> String.concat ~sep:", ");
      print out "  {\n";
      List.iter args (fun (v, t) ->
        print out "    %s = %s;\n" v (convert_pgocaml_type (v, t)));
      print out "  }\n\n";

      print out "let get_status (cache: 'a cache) =\n";
      print out "  let (%s) = cache in\n"
        (List.map (db_function_standard_fields result @ args_in_db)
           (function 
             | ("g_status", _, _) -> "status_str" 
             | ("g_inserted", _, _) -> "inserted" 
             | ("g_started", _, _) -> "started" 
             | ("g_completed", _, _) -> "completed" 
             | (n, _, _) -> "_" ) |>
               String.concat ~sep:", ");
      print out "  let status =\n\
                \    Enumeration_process_status.of_string_exn \
                 status_str in\n";
      print out "  let opt = function \n\
                \    | Some (d, tz) -> (d, tz)\n\
                \    | None -> failwith \"get_status(%s) can't find timestamp\"
                \  in\n" name;
      print out "  (match status with\n\
                \  | `Inserted ->   (status, opt inserted)\n\
                \  | `Started ->    (status, opt started)\n\
                \  | `Failed ->     (status, opt completed)\n\
                \  | `Succeeded ->  (status, opt completed)\
                )\n\n";

      print out "let get_result (cache: [> `can_get_result] cache) =\n";
      print out "  let (%s) = cache in"
        (List.map (db_function_standard_fields result @ args_in_db)
           (function ("g_result", _, _) -> "res_pointer" | (n, _, _) -> "_" ) |>
               String.concat ~sep:", ");
      print out "  { Record_%s.id = \n\
                \    match res_pointer with Some id -> id\n\
                \    | None -> \
          failwith \"get_result(%s) result (%s) is not set\" }\n\n" 
        result name result;

      (* Delete a function *)
      print out "let _delete_evaluation_by_id ~id (dbh:db_handle) =\n";
      print out "  PGSQL (dbh)\n";
      print out "    \"DELETE FROM %s WHERE g_id = $id\"\n\n" name;
      (* print out "(\**/**\)\n"; *)


      print out "end (* %s *)\n\n" name;
  );
  ()

let testing_inserts dsl amount output_string =
  List.iter dsl.nodes (function
    | Enumeration _ -> (* Nothing to do *)()
    | Record (name, fields) ->
      let intos =
        "g_last_accessed" :: List.map fields fst |> String.concat ~sep:", " in
      let values i =
        let rec g (n, t) =
          match  t with
          | Bool        -> Random.bool () |> sprintf "%b"
          | Timestamp   -> "now()"
          | Int         -> Random.int 30_000 |> sprintf "%d"
          | Real        -> Random.float 42_000. |> sprintf "%f"
          | String      -> sprintf "random_%s_%d" n (Random.int 30000)
          | Option o    ->  g (n, o)
          | Array a     -> 
            sprintf "{%s}" (String.concat ~sep:", "
                              (List.init (Random.int 42 + 1) (fun _ -> g (n, a))))
          | Pointer _ | Function_name _|Record_name _ -> g (n, Int)
          | Enumeration_name _ -> g (n, String)
        in
        let f = Fn.compose (sprintf "'%s'") g in
        "now()" :: List.map fields ~f |> String.concat ~sep:", " in
      for i = 0 to Random.int amount do
        sprintf "INSERT INTO %s (%s) VALUES (%s);\n\n" 
          name intos (values i) |> output_string;
      done
    | Function (name, args, result) ->
      let intos =
        "g_result" ::
          "g_recomputable" ::
          "g_recompute_penalty" ::
          "g_inserted" ::
          "g_started" ::
          "g_completed" ::
          "g_status" ::
          (List.map args fst) |> String.concat ~sep:", " in
      let values i =
        (if Random.bool () then Random.int amount |> sprintf "'%d'" else "null") ::
          (Random.bool () |> sprintf "'%b'") ::
          (Random.float 42. |> sprintf "'%f'") ::
          "now()" ::
          "now()" ::
          "now()" ::
          (sprintf "'random_status_for_%s_%d'" name (Random.int 30000)) ::
          (List.map args (fun (n, t) ->
            sprintf "'%d'" (Random.int amount))) |> String.concat ~sep:", "
      in
      for i = 0 to Random.int amount do
        sprintf "INSERT INTO %s (%s) VALUES (%s);\n\n" 
          name intos (values i) |> output_string;
      done;
      ()
  )

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

    with_file (prefix ^ (Filename.basename file) ^ "_fuzzdata_afew.psql")
      ~f:(fun o -> testing_inserts dsl 42 (output_string o));
    with_file (prefix ^ (Filename.basename file) ^ "_fuzzdata_alot.psql")
      ~f:(fun o -> testing_inserts dsl 4242 (output_string o));

  | exec :: "codegen" :: in_file :: out_file :: [] ->
    let dsl =
      In_channel.(with_file in_file ~f:(fun i -> parse_str (input_all i))) in
    Out_channel.(with_file out_file
                   ~f:(fun o -> ocaml_code dsl (output_string o)))
      
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
  | exec :: "db_digraph" :: file :: outfile :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let db = to_db (parse_str (In_channel.input_all i)) in
        Out_channel.(with_file outfile
                       ~f:(fun o -> DB_dsl.digraph db (output_string o))))
  | exec :: "postgres" :: file :: outprefix :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let db = to_db (parse_str (In_channel.input_all i)) in
        Out_channel.(with_file 
                       (outprefix ^ (Filename.basename file) ^ "_init.psql") 
                       ~f:(fun o -> DB_dsl.init_db_postgres db (output_string o)));
        Out_channel.(with_file 
                       (outprefix ^ Filename.basename file ^ "_clear.psql") 
                       ~f:(fun o -> DB_dsl.clear_db_postgres db (output_string o))))
  | exec :: "digraph" :: file :: outfile :: [] ->
    In_channel.with_file file 
      ~f:(fun i -> 
        let dsl = parse_str (In_channel.input_all i) in
        Out_channel.(with_file outfile
                       ~f:(fun o -> digraph dsl (output_string o))))
  | _ ->
    eprintf "usage: \n\
       sexp2db {dbverify, dbpostgres, dbdraw, ...} <sexp_file>\n\
       sexp2db all <sexp_file> <outprefix>\n";
    ()


