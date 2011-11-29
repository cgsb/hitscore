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

type typed_value = string * dsl_type

type dsl_runtime_type =
  | Enumeration of string * string list
  | Record of string * typed_value list
  | Function of string * typed_value list * string
  | Volume of string * string (* Volume (dsl-name, toplevel-dir) *)
      
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

(* A link is semantic: a pointer, or a enum-name, etc. *)
let rec type_is_link = function
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
  | "to" | "type" | "val" | "let" | "in" | "with" | "match" | "null" ->
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
    | (Sx.Atom "volume") :: (Sx.Atom name) :: (Sx.Atom toplevel) :: [] ->
      existing_types := Volume_name name :: !existing_types;
      Some (Volume (name, toplevel))
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

  
let dsl_type_to_db t =
  let props = ref [ Psql.Not_null] in
  let rec convert t =
    match t with
    | Bool        -> Psql.Bool      
    | Timestamp   -> Psql.Timestamp 
    | Int         -> Psql.Integer       
    | Real        -> Psql.Real      
    | String      -> Psql.Text   
    | Option t2 -> 
      props := List.filter !props ~f:((<>) Psql.Not_null);
      convert t2
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

let db_record_standard_fields = [
  ("g_id", Psql.Identifier, [Psql.Not_null]);
  ("g_last_accessed", Psql.Timestamp, []);
]
let db_function_standard_fields result = [   
  ("g_id", Psql.Identifier, [Psql.Not_null]);
  ("g_result", Psql.Pointer (result, "g_id"), []);
  ("g_recomputable", Psql.Bool, [Psql.Not_null]);
  ("g_recompute_penalty", Psql.Real, []);
  ("g_inserted", Psql.Timestamp, []);
  ("g_started", Psql.Timestamp, []);
  ("g_completed", Psql.Timestamp, []);
  ("g_status", Psql.Text, [Psql.Not_null]);
]

let to_db dsl =
  let filesystem = [
    Psql.({ name = "g_volume"; fields = [
      ("g_id", Identifier, [Not_null]);
      ("g_toplevel", Text, [Not_null]);
      ("g_hr_tag", Text, []);
      ("g_content", Pointer ("g_file", "g_id"), [Array]);] });
    Psql.({ name = "g_file"; fields = [
      ("g_id", Identifier, [Not_null]);
      ("g_name", Text, [Not_null]);
      ("g_type", Text, [Not_null]);
      ("g_content", Pointer ("g_file", "g_id"), [Array]);] });
  ] in
  let nodes =
    List.map dsl.nodes (function
      | Record (name, record) ->
        let user_fields =
          List.map record (fun (n, t) ->
            let typ, props = dsl_type_to_db t in
            (n,  typ, props)) in
        let fields = db_record_standard_fields @ user_fields in
        [{ Psql.name ; Psql.fields }]
      | Function (name, args, result) ->
        let arg_fields =
          List.map args (fun (n, t) ->
            let typ, props = dsl_type_to_db t in
            (n,  typ, props)) in
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
        sprintf "%s [shape=folder, fontname=Courier, label=\".../%s/\"];\n\n"
          name toplevel |> output_string
  );
  output_string "}\n"


let pgocaml_type_of_field =
  let rec props  = function
    | [ Psql.Array    ; Psql.Not_null] -> sprintf "PGOCaml.%s_array"
    | [ Psql.Not_null ] -> sprintf "%s"
    | _ -> sprintf "%s option" in
  function
    | (_, Psql.Timestamp          , p) -> (props p) "PGOCaml.timestamptz"
    | (_, Psql.Identifier         , p) -> (props p) "int32"              
    | (_, Psql.Text               , p) -> (props p) "string"             
    | (_, Psql.Pointer (r, "g_id"), p) -> (props p) "int32"              
    | (_, Psql.Integer            , p) -> (props p) "int32"              
    | (_, Psql.Real               , p) -> (props p) "float"              
    | (_, Psql.Bool               , p) -> (props p) "bool"               
    | _ -> failwith "Can't compile DB type to PGOCaml"
let rec ocaml_type = function
  | Bool        -> "bool"
  | Timestamp   -> "PGOCaml.timestamptz"
  | Int         -> "int32"
  | Real        -> "float"      
  | String      -> "string"
  | Option t -> sprintf "%s option" (ocaml_type t)
  | Array t -> sprintf "%s array" (ocaml_type t)
  | Function_name s -> sprintf "Function_%s.t" s
  | Enumeration_name s -> sprintf "Enumeration_%s.t" s
  | Record_name s -> sprintf "Record_%s.t" s
  | Volume_name s -> sprintf "File_system.volume"

let let_in_typed_value  = function
  | (n, Enumeration_name e) -> 
    sprintf "  let %s = Enumeration_%s.to_string %s in\n" n e n
  | (n, Record_name r) ->
    sprintf "  let %s = %s.Record_%s.id in\n" n n r
  | (n, Option (Record_name r)) ->
    sprintf "  let %s = option_map %s (fun s -> s.Record_%s.id) in\n" n n r
  | (n, Array (Record_name r)) ->
    sprintf "  let %s = array_map %s (fun s -> s.Record_%s.id) in\n" n n r
  | (n, Volume_name v) ->
    sprintf "  let %s = %s.File_system.id in\n" n n 
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
  | (n, Volume_name v) ->
    sprintf "{ File_system.id = %s } " n
  | (n, _) -> n


let raw out fmt = ksprintf out ("" ^^ fmt)
let line out fmt = ksprintf out ("" ^^ fmt ^^ "\n")
let doc out fmt = (* Doc to put before values/types *)
  ksprintf out ("\n(** " ^^ fmt ^^ " *)\n")
let deprecate out fmt =
  doc out (" @deprecated This is left there for emergency purposes only." ^^ fmt)




let ocaml_exception name =
  let define out = ksprintf out "exception %s of string\n" name in
  let raise_exn out str =
    ksprintf out "(raise (%s %S))" name str in
  (define, raise_exn )

let new_tmp_output () =
  let buf = Buffer.create 42 in
  let tmp_out s = Buffer.add_string buf s in
  let print_tmp output_string = output_string (Buffer.contents buf) in
  (tmp_out, print_tmp)

let pgocaml_do_get_all_ids ~out name = 
  raw out "  let um = PGSQL(dbh)\n";
  raw out "    \"SELECT g_id FROM %s\" in\n" name;
  raw out "  pg_map um (fun l -> list_map l (fun id -> { id }))\n\n";
  ()

let ocaml_enumeration_module ~out name fields =
  raw out "module Enumeration_%s = struct\n" name;
  raw out "type t = [%s]\n\n"
    (List.map fields (sprintf "`%s") |> String.concat ~sep:" | ");
  raw out "let to_string : t -> string = function\n| %s\n" 
    (List.map fields (fun s -> sprintf "`%s -> \"%s\"" s s) |>
        String.concat ~sep:"\n| ");
  raw out "\n";
  let def_oserr, raise_oserr = ocaml_exception "Of_string_error" in
  def_oserr out;
  raw out "let of_string_exn: string -> t = function\n| %s\n" 
    (List.map fields (fun s -> sprintf "\"%s\" -> `%s" s s) |>
        String.concat ~sep:"\n| ");
  raw out "| s -> ";
  raise_oserr out "cannot recognize enumeration element";
  raw out "\n\nend (* %s *)\n\n" name


let pgocaml_add_to_database ~out ~raise_wrong_db  ?(id="id")
    table_name intos values =
  raw out "  let i32_list_monad = PGSQL (dbh)\n";
  raw out "    \"INSERT INTO %s (%s)\\\n     VALUES (%s)\\\n\
                   \     RETURNING g_id \" in\n" table_name
    (intos |> String.concat ~sep:", ")
    (values|> String.concat ~sep:", ");
  raw out "  pg_bind i32_list_monad \n\
                    \    (function\n\
                    \       | [ %s ] -> PGOCaml.return { %s }\n\
                    \       | _ -> " id id;
  raise_wrong_db out "INSERT did not return one id";
  raw out ")\n\n";
  ()

let ocaml_record_module ~out name fields = 
  raw out "module Record_%s = struct\n" name;
  doc out "Type [t] should be used like a private type, access to \
          the [id] field is there for hackability/emergency purposes.";
  raw out "type t = { id: int32 }\n";

  let def_wrong_db_error, raise_wrong_db_error = 
    ocaml_exception "Wrong_DB_return" in
  def_wrong_db_error out;

      (* Function to add a new value: *)
  doc out "Create a new value of type [%s] in the database, and \
              return a handle to it." name;
  raw out "let add_value\n";
  List.iter fields (fun (n, t) ->
    let kind_of_arg = 
      match type_is_option t with `yes -> "?" | `no -> "~" in
    raw out "    %s(%s:%s)\n" kind_of_arg n (ocaml_type t);
  );
  let intos = "g_last_accessed" :: List.map fields fst in
  let values =
    let prefix (n, t) =
      (match type_is_option t with `yes -> "$?" | `no -> "$") ^ n in
    "now()" :: List.map fields prefix  in
  raw out "    (dbh:db_handle) : t PGOCaml.monad =\n";
  List.iter fields (fun tv -> let_in_typed_value tv |> raw out "%s");
  pgocaml_add_to_database name intos values
    ~out ~raise_wrong_db:raise_wrong_db_error;

  doc out "Get all the values of type [%s]." name;
  raw out "let get_all (dbh: db_handle): t list PGOCaml.monad = \n";
  pgocaml_do_get_all_ids ~out name;

      (* Create the type 'cache' (hidden in documentation thanks to '_cache') *)
  let args_in_db = 
    List.map fields (fun (s, t) ->
      let (t, p) = dsl_type_to_db t in (s, t, p)) in
  raw out "(**/**)\ntype _cache = \n(%s)\n(**/**)\n\n"
    (List.map (db_record_standard_fields @ args_in_db)
       ~f:pgocaml_type_of_field |> String.concat ~sep:" *\n ");
  doc out "The [cache] is the info retrieved by the database queries.";
  raw out "type cache = _cache\n\n";

      (* Access a function *)
  doc out "Cache the contents of the record [t].";
  raw out "let cache_value (t: t) (dbh:db_handle): \
                  cache PGOCaml.monad =\n";
  raw out "  let id = t.id in\n";
  raw out "  let um = PGSQL (dbh)\n";
  raw out "    \"SELECT * FROM %s WHERE g_id = $id\" in\n" name;
  raw out "  pg_bind um (function [one] -> pg_return one\n    | _ -> ";
  raise_wrong_db_error out "INSERT did not return one id";
  raw out ")\n";

  doc out "The fields of the Record, the type is intended to \
        be used for \"record pattern matching\" \
        [[let { Mod. a ; b } = Mod.get_fields ... ]]";
  raw out "type fields = {\n";
  List.iter fields (fun (v,t) -> 
    raw out "  %s: %s;\n" v (ocaml_type t));
  raw out "}\n";
  raw out "let get_fields (cache: cache) =\n";
  raw out "  let (%s, %s) = cache in\n"
    (List.map (db_record_standard_fields) (fun _ -> "_") |> 
        String.concat ~sep:", ")
    (List.map fields (fun (s, t) -> s) |> String.concat ~sep:", ");
  raw out "  {\n";
  List.iter fields (fun (v, t) ->
    raw out "    %s = %s;\n" v (convert_pgocaml_type (v, t)));
  raw out "  }\n\n";

  doc out "Get the last time the record was modified (It depends on \
          the good-will of the people modifying the database {i manually}).";
  raw out "let last_write_access (cache: cache): \
          (CalendarLib.Calendar.t * CalendarLib.Time_Zone.t) option =\n";
  raw out "  let (_, ts, %s) = cache in\n"
    (List.map fields (fun (s, t) -> "_") |> String.concat ~sep:", ");
  raw out "  ts\n\n";


      (* Access a value *)
  deprecate out "Finds a value given its internal identifier ([g_id]).";
  raw out "let _get_value_by_id ~id (dbh:db_handle) =\n";
  raw out "  let um = PGSQL(dbh)\n";
  raw out "    \"SELECT * FROM %s WHERE g_id = $id\" in\n" name;
  raw out "  pg_bind um \n\
            \    (function\n\
            \       | [ one ] -> pg_return one\n\
            \       | _ -> ";
  raise_wrong_db_error out "SELECT did not return a single tuple";
  raw out ")\n\n";

      (* Delete a value *)
  deprecate out "Deletes a values with its internal id.";
  raw out "let _delete_value_by_id ~id (dbh:db_handle) =\n";
  raw out "  PGSQL (dbh)\n";
  raw out "    \"DELETE FROM %s WHERE g_id = $id\"\n\n" name;
  raw out "end (* %s *)\n\n" name;
  ()

let ocaml_function_module ~out name args result =
  raw out "module Function_%s = struct\n" name;
  raw out "type 'a t = (* private *) { id: int32 }\n";

  let def_wrong_db_error, raise_wrong_db_error = 
    ocaml_exception "Wrong_DB_return" in
  def_wrong_db_error out;

      (* Function to insert a new function evaluation: *)
  raw out "let add_evaluation\n";
  List.iter args (fun (n, t) -> raw out "    ~%s\n" n);
  raw out "    ?(recomputable=false)\n";
  raw out "    ?(recompute_penalty=0.)\n";
  raw out "    (dbh:db_handle) : \n\
            \    [ `can_start | `can_complete ] t PGOCaml.monad =\n";
  List.iter args (fun tv -> let_in_typed_value tv |> raw out "%s");
  let intos =
    "g_recomputable" :: "g_recompute_penalty" :: "g_inserted" :: "g_status" :: 
      (List.map args fst) in
  let values =
    "$recomputable" :: "$recompute_penalty" :: "now ()" :: "'Inserted'" ::
      (List.map args (fun (n, t) -> "$" ^ n)) in
  pgocaml_add_to_database name intos values
    ~out ~raise_wrong_db:raise_wrong_db_error;
  
      (* Function to set the state of a function evaluation to 'STARTED': *)
  raw out "let set_started (t : [> `can_start] t) (dbh:db_handle) =\n";
  raw out "  let id = t.id in\n";
  raw out "  let umm = PGSQL (dbh)\n";
  raw out "    \"UPDATE %s SET g_status = 'Started', g_started = now ()\n\
                \    WHERE g_id = $id\" in\n\
                \    pg_bind umm (fun () -> \
                pg_return ({ id } : [ `can_complete] t)) 
                \n\n\n" name;
  raw out "let set_succeeded (t : [> `can_complete] t) \n\
                   \     ~result (dbh:db_handle) =\n";
  let_in_typed_value ("result", Record_name result) |> raw out "%s";
  raw out "  let id = t.id in\n";
  raw out "  let umm = PGSQL (dbh)\n";
  raw out "    \"UPDATE %s SET g_status = 'Succeeded', \
                g_completed = now (), g_result = $result\n \
                \    WHERE g_id = $id\" in\n\
                \    pg_bind umm (fun () -> \
                pg_return ({ id } : [ `can_get_result] t)) 
                \n\n\n" name;
  raw out "let set_failed \
                   (t : [ `can_start | `can_complete] t) (dbh:db_handle) =\n";
  raw out "  let id = t.id in\n";
  raw out "  let umm = PGSQL (dbh)\n";
  raw out "    \"UPDATE %s SET g_status = 'Failed', g_completed = now ()\n\
                \    WHERE g_id = $id\" in\n\
                \    pg_bind umm (fun () -> \
                pg_return ({ id } : [ `can_nothing ] t)) 
                \n\n\n" name;

      (* Create the type 'cache' (hidden in documentation thanks to '_cache') *)
  let args_in_db = 
    List.map args (fun (s, t) ->
      let (t, p) = dsl_type_to_db t in (s, t, p)) in
  raw out "(**/**)\ntype 'a _cache = \n(%s)\n(**/**)\n\n"
    (List.map (db_function_standard_fields result @ args_in_db)
       ~f:pgocaml_type_of_field |> String.concat ~sep:" *\n ");
  doc out "The [cache] is the info retrieved by the database queries; \
               it inherits the capabilities of the handle (type [t]).";
  raw out "type 'a cache = 'a _cache\n\n";

      (* Access a function *)
  doc out "Cache the contents of the evaluation [t].";
  raw out "let cache_evaluation (t: 'a t) (dbh:db_handle):\
                 'a cache PGOCaml.monad =\n";
  raw out "  let id = t.id in\n";
  raw out "  let umm = PGSQL (dbh)\n";
  raw out "    \"SELECT * FROM %s WHERE g_id = $id\" in\n" name;
  raw out "  pg_bind umm (function [one] -> pg_return one\n    | _ -> ";
  raise_wrong_db_error out "INSERT did not return one id";
  raw out ")\n";

  doc out "The arguments of the Function, the type is intended to \
        be used for \"record pattern matching\" \
        [[let { Mod. a ; b } = Mod.get_arguments ... ]]";
  raw out "type arguments = {\n";
  List.iter args (fun (v,t) -> 
    raw out "  %s: %s;\n" v (ocaml_type t));
  raw out "}\n";
  raw out "let get_arguments (cache : 'a cache) =\n";
  raw out "  let (%s, %s) = cache in\n"
    (List.map (db_function_standard_fields result) (fun _ -> "_") |> 
        String.concat ~sep:", ")
    (List.map args (fun (s, t) -> s) |> String.concat ~sep:", ");
  raw out "  {\n";
  List.iter args (fun (v, t) ->
    raw out "    %s = %s;\n" v (convert_pgocaml_type (v, t)));
  raw out "  }\n\n";

  doc out "Get the current status of the Function and the last \
          time the status was updated.";
  raw out "let get_status (cache: 'a cache) =\n";
  raw out "  let (%s) = cache in\n"
    (List.map (db_function_standard_fields result @ args_in_db)
       (function 
         | ("g_status", _, _) -> "status_str" 
         | ("g_inserted", _, _) -> "inserted" 
         | ("g_started", _, _) -> "started" 
         | ("g_completed", _, _) -> "completed" 
         | (n, _, _) -> "_" ) |>
           String.concat ~sep:", ");
  raw out "  let status =\n\
                \    Enumeration_process_status.of_string_exn \
                 status_str in\n";
  raw out "  let opt = function \n\
                \    | Some (d, tz) -> (d, tz)\n\
                \    | None -> failwith \"get_status(%s) can't find timestamp\"
                \  in\n" name;
  raw out "  (match status with\n\
                \  | `Inserted ->   (status, opt inserted)\n\
                \  | `Started ->    (status, opt started)\n\
                \  | `Failed ->     (status, opt completed)\n\
                \  | `Succeeded ->  (status, opt completed)\
                )\n\n";

  doc out "Get the result of the function if is available.";
  raw out "let get_result (cache: [> `can_get_result] cache) =\n";
  raw out "  let (%s) = cache in"
    (List.map (db_function_standard_fields result @ args_in_db)
       (function ("g_result", _, _) -> "res_pointer" | (n, _, _) -> "_" ) |>
           String.concat ~sep:", ");
  raw out "  { Record_%s.id = \n\
                \    match res_pointer with Some id -> id\n\
                \    | None -> \
          failwith \"get_result(%s) result (%s) is not set\" }\n\n" 
    result name result;

  List.iter 
    [ ("inserted", "`Inserted", "[ `can_start | `can_complete]");
      ("started", "`Started", "[ `can_fail | `can_complete]");
      ("failed", "`Failed", "[ `can_nothing ]");
      ("succeeded", "`Succeeded", "[ `can_get_result ]"); ]
    (fun (suffix, polyvar, phamtom) -> 
      doc out "Safe cast of the capabilities to [ %s ]; returns [None] \
              if the cast is not allowed." phamtom;
      raw out "let is_%s (cache: 'a cache): %s cache option =\n" 
        suffix phamtom;
      raw out "  match get_status cache with\n\
                       | %s, _ -> Some (cache: %s cache)\n\
                       | _ -> None\n\n" polyvar phamtom;


      doc out "Get all the Functions whose status is [%s]." polyvar;
      raw out "let get_all_%s (dbh:db_handle): %s t list PGOCaml.monad =\n"
        suffix phamtom;
      raw out "  let status_str = \n\
                    \    Enumeration_process_status.to_string %s in\n" polyvar;
      raw out "  let umm = PGSQL(dbh)\n";
      raw out "    \"SELECT g_id FROM %s WHERE g_status = $status_str\" in\n"
        name;
      raw out "  pg_map umm (fun l -> list_map l (fun id -> { id }))\n\n");

  doc out "Get all the [%s] functions." name;
  raw out "let get_all (dbh: db_handle): \
                  [ `can_nothing ] t list PGOCaml.monad = \n";
  pgocaml_do_get_all_ids ~out name;

  (* Delete a function *)
  deprecate out "Deletes a function using its internal identifier.";
  raw out "let _delete_evaluation_by_id ~id (dbh:db_handle) =\n";
  raw out "  PGSQL (dbh)\n";
  raw out "    \"DELETE FROM %s WHERE g_id = $id\"\n\n" name;
  

  raw out "end (* %s *)\n\n" name;
  ()


let ocaml_toplevel_values_and_types ~out dsl =
  let rec_name = function Record (n, _) -> Some n | _ -> None in
  let fun_name = function Function (n, _, _) -> Some n | _ -> None in
  List.iter
    [("records"    , "record_"       , None, rec_name);
     ("values"     , "value_"        , Some "Record", rec_name);
     ("functions"  , "function_"     , None, fun_name);
     ("'a evaluations", "evaluation_", Some "'a Function", fun_name);
    ]
    (fun (type_name, prefix, modprefix, get_name) ->
      raw out "type %s = [\n" type_name;
      List.iter dsl.nodes (fun e ->
        get_name e |> Option.iter ~f:(fun name ->
          match modprefix with
          | Some s -> raw out " | `%s%s of %s_%s.t\n" prefix name s name
          | None -> raw out " | `%s%s\n" prefix name)
      );
      raw out "]\n";
    );

  let tmprec, print_tmprec = new_tmp_output () in
  let tmpfun, print_tmpfun = new_tmp_output () in
  doc tmprec "Get all record values";
  doc tmpfun "Get all function evaluations";
  raw tmprec "let get_all_values (dbh: db_handle): values list PGOCaml.monad =\n\
       \  let val_list_monad_list = [\n";
  raw tmpfun "let get_all_evaluations (dbh: db_handle):
                [ `can_nothing ] evaluations list PGOCaml.monad =\n\
              \  let val_list_monad_list = [\n";
  List.iter dsl.nodes (function
    | Record (n, fields) ->
      raw tmprec "pg_map (Record_%s.get_all dbh) \
          (fun l -> list_map l (fun x -> `value_%s x));\n" n n
    | Function (n, args, ret) ->
      raw tmpfun "pg_map (Function_%s.get_all dbh) (fun l -> list_map l \
                    (fun x -> `evaluation_%s x));\n" n n
    | _ -> ());
  let the_rest = [  
    "  ] in\n";
    "  let val_list_list_monad =\n\
             \    fold_left val_list_monad_list\n\
             \      ~init:(pg_return []) ~f:(fun lm fm -> \n\
             \         pg_bind lm (fun l -> pg_bind fm (fun f -> \n\
             \           pg_return (f :: l)))) in\n";
    "  pg_map val_list_list_monad list_flatten\n"; 
  ] |> String.concat in
  raw tmprec "%s" the_rest;
  raw tmpfun "%s" the_rest;

  print_tmprec out;
  print_tmpfun out;

  ()

let ocaml_file_system_module ~out = (* For now does not depend on the
                                       actual layout *)
  let def_wrong_db_error, raise_wrong_db = 
    ocaml_exception "Wrong_DB_return" in

  line out "module File_system = struct";
  def_wrong_db_error out;
  line out "type volume = { id : int32 }";
  line out "type file = { inode: int32 }";
  line out "type path = ";
  line out "  | File of string * Enumeration_file_type.t";
  line out "  | Directory of string * Enumeration_file_type.t * path list";
  line out "  | Opaque of string * Enumeration_file_type.t";
  doc out "Register a new file, directory (with its contents), or opaque \
        directory in the DB.";
  line out "let add_file (dbh: db_handle) \
                ~(kind:Enumeration_volume_kind.t) \
                ?(hr_tag: string option) \
                ~(files:path list) = ";
  line out "let rec add_path = function";
  line out "  | File (name, t) | Opaque (name, t) -> begin";
  line out "    let type_str = Enumeration_file_type.to_string t in";
  pgocaml_add_to_database "g_file" 
    [ "g_name"; "g_type"; "g_content" ]
    [ "$name"; "$type_str" ; "NULL" ]
    ~out ~raise_wrong_db ~id:"inode";
  line out "    end";
  line out "  | Directory (name, t, pl) -> begin";
  line out "    let type_str = Enumeration_file_type.to_string t in";
  line out "    let added_file_monad_list = (list_map pl add_path) in";
  line out "    let file_list_monad =
                 fold_left added_file_monad_list
                  ~init:(pg_return []) ~f:(fun lm fm -> 
                  pg_bind lm (fun l -> pg_bind fm (fun f -> \
                    pg_return (f :: l)))) in ";
  line out "    pg_bind (file_list_monad) (fun file_list -> \n\
                  let pg_inodes = array_map (list_to_array file_list) \n\
                       (fun { inode } ->  inode) in";
  pgocaml_add_to_database "g_file" 
    [ "g_name"; "g_type"; "g_content" ]
    [ "$name"; "$type_str" ; "$pg_inodes" ]
    ~out ~raise_wrong_db ~id:"inode";

  line out "  )  end in";
  line out " let added_file_monad_list = (list_map files add_path) in";
  line out " let file_list_monad =
              fold_left added_file_monad_list
               ~init:(pg_return []) ~f:(fun lm fm -> 
               pg_bind lm (fun l -> pg_bind fm (fun f -> \
                 pg_return (f :: l)))) in ";
  line out "    pg_bind (file_list_monad) (fun file_list -> \n\
                  let pg_inodes = array_map (list_to_array file_list) \n\
                       (fun { inode } ->  inode) in";
  line out " let vol_type_str = Enumeration_volume_kind.to_string kind in";
  pgocaml_add_to_database "g_volume" 
    [ "g_toplevel"; "g_hr_tag"; "g_content" ]
    [ "$vol_type_str" ; "$?hr_tag"; "$pg_inodes" ]
    ~out ~raise_wrong_db ~id:"id";
  line out ")";

  doc out "Module for constructing file paths less painfully.";
  line out "module Path = struct";
  line out "let file ?(t=`blob) n = File (n, t)";
  line out "let dir ?(t=`directory) n l = Directory (n, t, l)";
  line out "let opaque ?(t=`opaque) n = Opaque (n, t)";
  line out "end";
  line out "end (* File_system *)"; 
  ()

let ocaml_code ?(functorize=true) dsl output_string =
  let out = output_string in
  doc out "Autogenerated module.";
  if functorize then
    line out "module Make (PGOCaml : PGOCaml_generic.PGOCAML_GENERIC) = struct";
  doc out "PG'OCaml's connection handle.";
  raw out "type db_handle = (string, bool) Hashtbl.t PGOCaml.t\n\n";
  doc out "Access rights on [Function_*.{t,cache}].";
  raw out "type function_capabilities= [\n\
                \  | `can_nothing\n  | `can_start\n  | `can_complete\n\
                | `can_get_result\n]\n";

  raw out "(**/**)\n\
      \ let option_map o f =\n\
      \     match o with None -> None | Some s -> Some (f s)\n\n\
      \ let array_map a f = ArrayLabels.map ~f a\n\n\
      \ let list_map l f = ListLabels.map ~f l\n\n\
      \ let list_flatten l = List.flatten l\n\n\
      \ let fold_left = Core.Std.List.fold_left\n\n\
      \ let array_to_list a = Array.to_list a\n\n\
      \ let list_to_array l = Array.of_list l\n\n\
      \ let pg_bind am f =\nPGOCaml.bind am f\n\n\
      \ let pg_return t = PGOCaml.return t\n\n\
     \ let pg_map am f = pg_bind am (fun x -> pg_return (f x))\n\n\
      (**/**)\n";
  let volume_kinds = 
    ("g_trash" :: (List.filter_map dsl.nodes 
                     (function | Volume (n, _) -> Some n | _ -> None))) in
  let file_types =  ["blob"; "directory"; "opaque"] in
  ocaml_enumeration_module ~out "volume_kind" volume_kinds;
  ocaml_enumeration_module ~out "file_type" file_types;
  ocaml_file_system_module ~out;
  List.iter dsl.nodes (function
    | Enumeration (name, fields) ->
      ocaml_enumeration_module ~out name fields
    | Record (name, fields) ->
      ocaml_record_module ~out name fields
    | Function (name, args, result) ->
      ocaml_function_module ~out name args result
    | Volume (_, _) -> ()
  );
  ocaml_toplevel_values_and_types ~out dsl;
  if functorize then
    line out "end (* Make *)";
  ()

let testing_inserts dsl amount output_string =
  List.iter dsl.nodes (function
    | Volume _ -> ()
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
          | Volume_name _ | Function_name _|Record_name _ -> g (n, Int)
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
