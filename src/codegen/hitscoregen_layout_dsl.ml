open Core.Std
let (|>) x f = f x

module Psql = Hitscoregen_psql
module Sx = Sexplib.Sexp

exception Parse_error of string

type dsl_type =
  | Bool
  | Timestamp
  | Int
  | Real
  | String
  | Option of dsl_type
  | Array of dsl_type 
  | Record_name of string
  | Enumeration_name of string
  | Function_name of string
  | Volume_name of string 
  | Identifier

type typed_value = string * dsl_type

type dsl_runtime_type =
  | Enumeration of string * string list
  | Record of string * typed_value list
  | Function of string * typed_value list * string
  | Volume of string * string (* Volume (dsl-name, toplevel-dir) *)
      
type custom_action = 
  | Search_by_and of string * string list

type dsl_runtime_description = {
  nodes: dsl_runtime_type list;
  types: dsl_type list; (* TODO clarify what's in there! *)
  actions: custom_action list;
}

let built_in_complex_types = [
  Enumeration ("process_status",
               [ "Started"; "Inserted"; "Failed"; "Succeeded" ]);
  Record ("log", ["log", String]);
]

let type_name = function
  | Enumeration (n, _) -> Enumeration_name n
  | Record (n, _) -> Record_name n
  | Function (n, _, _) -> Function_name n
  | Volume (n, _) -> Volume_name n

let built_in_types =
  List.map built_in_complex_types ~f:type_name

let type_is_built_in t = 
  List.exists built_in_complex_types 
    ~f:(fun x -> type_name x = type_name t)

let rec string_of_dsl_type = function
  | Bool        -> "Bool"
  | Timestamp   -> "Timestamp"
  | Int         -> "Int"
  | Real        -> "Real"
  | String      -> "String"    
  | Option o    -> sprintf "%s option" (string_of_dsl_type o)
  | Array a     -> sprintf "%s array" (string_of_dsl_type a)
  | Record_name       n -> n |> sprintf "%s" 
  | Enumeration_name  n -> n |> sprintf "%s" 
  | Function_name     n -> n |> sprintf "%s" 
  | Volume_name     n -> n |> sprintf "%s"
  | Identifier -> "Identifier"

let rec type_is_pointer = function
  | Bool         -> `no
  | Timestamp    -> `no
  | Int          -> `no
  | Real         -> `no
  | String       -> `no
  | Option o     -> type_is_pointer o
  | Array a      -> type_is_pointer a
  | Record_name       n -> `yes n
  | Enumeration_name  n -> `no
  | Function_name     n -> `yes n
  | Volume_name     n -> `yes n
  | Identifier -> `no

(* A link is semantic: a pointer, or a enum-name, etc. *)
let rec type_is_link = function
  | Identifier
  | Bool      
  | Timestamp 
  | Int       
  | Real      
  | String       -> `no
  | Option o     -> type_is_link o
  | Array a      -> type_is_link a
  | Record_name       n
  | Enumeration_name  n
  | Function_name     n
  | Volume_name       n -> `yes n

let rec type_is_option = function
  | Identifier -> `no
  | Bool         -> `no
  | Timestamp    -> `no
  | Int          -> `no
  | Real         -> `no
  | String       -> `no
  | Option o     -> `yes
  | Array a      -> type_is_option a
  | Record_name       n -> `no
  | Enumeration_name  n -> `no
  | Function_name     n -> `no
  | Volume_name     n -> `no

let find_type l t =
  List.find l ~f:(function
    | Record_name       n -> n = t
    | Enumeration_name  n -> n = t
    | Function_name     n -> n = t
    | Volume_name     n -> n = t
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
  | "to" | "type" | "val" | "let" | "in" | "with" | "match" 
  | "null" | "process_status" ->
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
    | Sx.Atom "array" :: l -> parse_2nd_type (Array fst) l
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
  let actions = ref [] in
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
    | (Sx.Atom "volume") :: (Sx.Atom name) :: (Sx.Atom toplevel) :: [] ->
      existing_types := Volume_name name :: !existing_types;
      Some (Volume (name, toplevel))
    | (Sx.Atom "search") :: (Sx.Atom record) :: (Sx.Atom "by") :: search_by ->
      begin match check_type record with
      | Record_name r -> ()
      | Function_name r -> ()
      | t -> 
        fail (sprintf "A search-by can only operate on records or functions, \
                 not %S like (search %s by ...) does."
                (string_of_dsl_type t) record)
      end;
      begin match search_by with
      | [ Sx.List (Sx.Atom "and" :: fields) ] ->
        let fields = List.map fields (function
          | Sx.Atom f -> f
          | _ -> ksprintf fail "In (search %s by ...): 'and' operation only
                      accepts lists of fields" record)
        in
        actions := (Search_by_and (record, fields)) :: !actions
      | [ Sx.Atom field ] ->
        actions := (Search_by_and (record, [field])) :: !actions
      | _ ->
        ksprintf fail "Can't parse this (search %s by ...)" record
      end;
      None
    | s ->
      fail (sprintf "I'm lost while parsing entry with: %s\n"
              (Sx.to_string (Sx.List s)))
  in
  let check_actions actions nodes =
    List.iter actions (function
      | Search_by_and (record, fields) ->
        begin match List.find nodes (function
          | Record (n, _) -> n = record
          | _ -> false)  with
        | Some (Record (n, r_f)) ->
          List.iter fields (fun s ->
            if List.exists r_f (fun (n, _) -> s = n) then () 
            else
              ksprintf fail "In (search %s by ...): field %s does not exist" 
                record s)
        | _ -> ()
        end)
  in
  match sexp with
  | Sx.Atom s -> fail_atom s
  | Sx.List l ->
    let user_nodes =
      List.filter_map l
        ~f:(function
          | Sx.Atom s -> fail_atom s
          | Sx.List l -> parse_entry l) in
    let nodes = built_in_complex_types @ user_nodes in
    let actions = !actions in
    check_actions actions nodes;
    { nodes; 
      types = List.rev !existing_types;
      actions }
    

let parse_str str =
  let sexp = 
    try Sx.of_string (sprintf "(%s)" str) 
    with Failure msg ->
      raise (Parse_error (sprintf "Syntax Error (sexplib): %s" msg))
  in
  (parse_sexp sexp)

  
let dsl_type_to_db t =
  let props = ref [ Psql.Not_null] in
  let rec convert t =
    match t with
    | Identifier -> Psql.Identifier
    | Bool        -> Psql.Bool      
    | Timestamp   -> Psql.Text
    | Int         -> Psql.Integer       
    | Real        -> Psql.Real      
    | String      -> Psql.Text   
    | Option t2 -> 
      props := List.filter !props ~f:((<>) Psql.Not_null);
      convert t2
    | Array Real -> Psql.Text
    | Array (Enumeration_name n) -> Psql.Text
    | Array String -> Psql.Text
    | Array t2 ->
      props := Psql.Array :: !props;
      convert t2
    | Function_name s -> Psql.Pointer (s, "g_id")
    | Enumeration_name s -> Psql.Text
    | Record_name s -> Psql.Pointer (s, "g_id")
    | Volume_name s -> Psql.Pointer ("g_volume", "g_id")
  in
  let converted = convert t in
  (converted, !props)

let typed_value_to_db (n, t) =
  let (tdb, props) = dsl_type_to_db t in
  (n, tdb, props)

let record_standard_fields = [
  ("g_id", Identifier);
  ("g_created", Option Timestamp);
  ("g_last_modified", Option Timestamp);
]
let function_standard_fields result = [
  ("g_id"                , Identifier);
  ("g_result"            , Option (Record_name result));
  ("g_recomputable"      , Bool);
  ("g_recompute_penalty" , Option Real);
  ("g_inserted"          , Option Timestamp);
  ("g_started"           , Option Timestamp);
  ("g_completed"         , Option Timestamp);
  ("g_status"            , Enumeration_name "process_status");
]

let to_db dsl =
  let db_record_standard_fields = 
    List.map record_standard_fields typed_value_to_db in
  let db_function_standard_fields result = 
    List.map (function_standard_fields result) typed_value_to_db in
  let filesystem = [
    Psql.({ name = "g_volume"; fields = [
      ("g_id", Identifier, [Not_null]);
      ("g_toplevel", Text, [Not_null]);
      ("g_hr_tag", Text, []);
      ("g_content", Pointer ("g_file", "g_id"), [Array; Not_null]);] });
    Psql.({ name = "g_file"; fields = [
      ("g_id", Identifier, [Not_null]);
      ("g_name", Text, [Not_null]);
      ("g_type", Text, [Not_null]);
      ("g_content", Pointer ("g_file", "g_id"), [Array; Not_null]);] });
  ] in
  let nodes =
    List.map dsl.nodes (function
      | Record (name, record) ->
        let user_fields = List.map record typed_value_to_db in
        let fields = db_record_standard_fields @ user_fields in
        [{ Psql.name ; Psql.fields }]
      | Function (name, args, result) ->
        let arg_fields = List.map args typed_value_to_db in
        let fields = (db_function_standard_fields result) @ arg_fields in
        [ { Psql.name; Psql.fields } ]
      | Enumeration _ -> []
      | Volume (_, _) -> []
    ) |> List.flatten
  in
  filesystem @ nodes

      
let digraph dsl ?(name="dsl") output_string =
  sprintf "digraph %s {
        graph [fontsize=20 labelloc=\"t\" label=\"\" \
            splines=true overlap=false ];\n"
    name |> output_string;
  List.iter dsl.nodes (fun n ->
    if type_is_built_in n then
      ()
    else
      match n with
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
        sprintf "%s -> %s [penwidth=3,color=\"#008800\"];\n" name result |> 
            output_string
      | Enumeration (name, items) ->
        sprintf "%s [label=\"%s =\\l  | %s\\l\"];\n\n"
          name name 
          (String.concat ~sep:"\\l  | " items) |> output_string
      | Volume (name, toplevel) ->
        sprintf "%s [shape=folder, \
              label=<<i>%s</i><br/><font face=\"Courier\" >.../%s/</font>>];\n\n"
          name name toplevel |> output_string
  );
  output_string "}\n"

