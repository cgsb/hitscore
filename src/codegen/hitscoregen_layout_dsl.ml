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
    | [ Psql.Array    ; Psql.Not_null] -> sprintf "%s array"
    | [ Psql.Not_null ] -> sprintf "%s"
    | _ -> sprintf "%s option" in
  function
    | (_, Psql.Timestamp          , p) -> (props p) "pg_timestamptz"
    | (_, Psql.Identifier         , p) -> (props p) "int32"              
    | (_, Psql.Text               , p) -> (props p) "string"             
    | (_, Psql.Pointer (r, "g_id"), p) -> (props p) "int32"              
    | (_, Psql.Integer            , p) -> (props p) "int32"              
    | (_, Psql.Real               , p) -> (props p) "float"              
    | (_, Psql.Bool               , p) -> (props p) "bool"               
    | _ -> failwith "Can't compile DB type to PGOCaml"
let rec ocaml_type = function
  | Bool        -> "bool"
  | Timestamp   -> "pg_timestamptz"
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
  | (n, Option (Enumeration_name e)) -> 
    sprintf "  let %s = option_map %s Enumeration_%s.to_string in\n" n n e
  | (n, Array (Enumeration_name e)) -> 
    sprintf "  let %s = array_map Enumeration_%s.to_string %s in\n" n n e
  | (n, Record_name r) ->
    sprintf "  let %s = %s.Record_%s.id in\n" n n r
  | (n, Option (Record_name r)) ->
    sprintf "  let %s = option_map %s (fun s -> s.Record_%s.id) in\n" n n r
  | (n, Array (Record_name r)) ->
    sprintf "  let %s = array_map %s (fun s -> s.Record_%s.id) in\n" n n r
  | (n, Volume_name v) ->
    sprintf "  let %s = %s.File_system.id in\n" n n 
  | (n, Option (Volume_name r)) ->
    sprintf "  let %s = option_map %s (fun s -> s.File_system.id) in\n" n n
  | (n, Array (Volume_name r)) ->
    sprintf "  let %s = array_map %s (fun s -> s.File_system.id) in\n" n n
  | _ -> ""

let convert_pgocaml_type = function
  | (n, Enumeration_name e) -> 
    sprintf "(Enumeration_%s.of_string_exn %s)" e n
  | (n, Option (Enumeration_name e)) -> 
    sprintf "(option_map %s Enumeration_%s.of_string_exn)" n e
  | (n, Array (Enumeration_name e)) -> 
    sprintf "(array_map %s Enumeration_%s.of_string_exn)" n e
  | (n, Record_name r) ->
    sprintf "{ Record_%s.id = %s }" r n
  | (n, Option (Record_name r)) ->
    sprintf "(option_map %s (fun id -> { Record_%s.id }))" n r
  | (n, Array (Record_name r)) ->
    sprintf "(array_map %s (fun id ->  { Record_%s.id }))" n r
  | (n, Volume_name v) ->
    sprintf "{ File_system.id = %s } " n
  | (n, Option (Volume_name r)) ->
    sprintf "(option_map %s (fun id -> { File_system..id }))" n
  | (n, Array (Volume_name r)) ->
    sprintf "(array_map %s (fun id ->  { File_system.id }))" n
  | (n, _) -> n


let new_tmp_output () =
  let buf = Buffer.create 42 in
  let tmp_out s = Buffer.add_string buf s in
  let print_tmp output_string = output_string (Buffer.contents buf) in
  (tmp_out, print_tmp)

let raw out fmt = ksprintf out ("" ^^ fmt)
let line out fmt = ksprintf out ("" ^^ fmt ^^ "\n")
let doc out fmt = (* Doc to put before values/types *)
  ksprintf out ("\n(** " ^^ fmt ^^ " *)\n")
let deprecate out fmt =
  doc out (" @deprecated This is left there for emergency purposes only." ^^ fmt)
let debug out metafmt fmt =
  line out ("Printf.ksprintf PGThread.err \"%s\" " ^^ fmt ^^ ";") metafmt

let hide out (f: (string -> unit) -> unit) =
  let actually_hide = true in
  let tmpout, printtmp =  new_tmp_output () in
  if actually_hide then
    raw tmpout "\n(**/**)\n"
  else
    raw tmpout "\n(** {4 Begin: Hidden} *)\n\n";
  f tmpout;
  if actually_hide then
    raw tmpout "\n(**/**)\n"
  else
    raw tmpout "\n(** {4 End: Hidden} *)\n\n";
  printtmp out;
  ()

let ocaml_exception ?(thread=true) name =
  let define out = ksprintf out "exception %s of string\n" name in
  let raise_exn out str =
    ksprintf out "(raise (%s %S))" name str in
  let thread_fail out str =
    ksprintf out "(PGThread.fail (%s %S))" name str in
  (define, if thread then thread_fail else raise_exn)

let pgocaml_do_get_all_ids ~out ?(id="id") name = 
  raw out "  let um = PGSQL(dbh)\n";
  raw out "    \"SELECT g_id FROM %s\" in\n" name;
  raw out "  pg_map um (fun l -> list_map l (fun %s -> { %s }))\n\n" id id;
  ()

let pgocaml_select_from_one_by_id 
    ~out ~raise_wrong_db ?(get_id="t.id") table_name  =
  raw out "  let id = %s in\n" get_id;
  raw out "  let um = PGSQL (dbh)\n";
  raw out "    \"SELECT * FROM %s WHERE g_id = $id\" in\n" table_name;
  raw out "  pg_bind um (function [one] -> pg_return one\n    | _ -> ";
  raise_wrong_db out (sprintf "SELECT (%s of %s) did not return one id" 
                        get_id table_name);
  raw out ")\n";
  ()

let sexp_functions_for_hidden ~out ?param type_name =
  let param_str = Option.value_map ~default:"" ~f:(sprintf "%s ") param in
  let param_arg = Option.value_map ~default:"" ~f:(fun _ -> " x") param in
  doc out "Dump a [%s%s] to a S-Expression." param_str type_name;
  line out "let sexp_of_%s%s:" type_name param_arg;
  line out " %s%s -> Sexplib.Sexp.t = sexp_of__%s%s" 
    param_str type_name type_name param_arg;
  doc out "Create a [%s%s] from a {i dump}." param_str type_name;
  line out "let %s_of_sexp%s:" type_name param_arg; 
  line out " Sexplib.Sexp.t -> %s%s = _%s_of_sexp%s"
    param_str type_name type_name param_arg;
  ()

let ocaml_enumeration_module ~out name fields =
  raw out "module Enumeration_%s = struct\n" name;
  raw out "type t = [%s]\n\n"
    (List.map fields (sprintf "`%s") |> String.concat ~sep:" | ");
  raw out "let to_string : t -> string = function\n| %s\n" 
    (List.map fields (fun s -> sprintf "`%s -> \"%s\"" s s) |>
        String.concat ~sep:"\n| ");
  raw out "\n";
  let def_oserr, raise_oserr = 
    ocaml_exception ~thread:false "Of_string_error" in
  def_oserr out;
  raw out "let of_string_exn: string -> t = function\n| %s\n" 
    (List.map fields (fun s -> sprintf "\"%s\" -> `%s" s s) |>
        String.concat ~sep:"\n| ");
  raw out "| s -> ";
  raise_oserr out "cannot recognize enumeration element";
  raw out "\n\nend (* %s *)\n\n" name

let pgocaml_db_handle_arg = "~(dbh: db_handle)"

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
  raise_wrong_db out (sprintf "INSERT (%s) did not return one id" table_name);
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
  raw out "    %s : t PGOCaml.monad =\n" pgocaml_db_handle_arg;
  List.iter fields (fun tv -> let_in_typed_value tv |> raw out "%s");
  pgocaml_add_to_database name intos values
    ~out ~raise_wrong_db:raise_wrong_db_error;

  doc out "Get all the values of type [%s]." name;
  raw out "let get_all %s: t list PGOCaml.monad = \n" pgocaml_db_handle_arg;
  pgocaml_do_get_all_ids ~out name;

      (* Create the type 'cache' (hidden in documentation thanks to '_cache') *)
  let args_in_db = 
    List.map fields (fun (s, t) ->
      let (t, p) = dsl_type_to_db t in (s, t, p)) in
  hide out (fun out ->
    raw out "type _cache = \n(%s) with sexp\n\n"
    (List.map (db_record_standard_fields @ args_in_db)
       ~f:pgocaml_type_of_field |> String.concat ~sep:" *\n ");
  );
  doc out "The [cache] is the info retrieved by the database queries.";
  raw out "type cache = _cache\n\n";

  doc out "Cache the contents of the record [t].";
  raw out "let cache_value (t: t) %s: \
                  cache PGOCaml.monad =\n" pgocaml_db_handle_arg;
  pgocaml_select_from_one_by_id ~out ~raise_wrong_db:raise_wrong_db_error name;

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

  doc out "{3 S-Expression Dumps}";
  sexp_functions_for_hidden ~out "cache";

  doc out "{3 Low Level Access}";
  (* Access a value *)
  deprecate out "Finds a value given its internal identifier ([g_id]).";
  raw out "let _get_value_by_id ~id %s =\n" pgocaml_db_handle_arg;
  pgocaml_select_from_one_by_id 
    ~out ~raise_wrong_db:raise_wrong_db_error ~get_id:"id" name;

  (* Delete a value *)
  deprecate out "Deletes a values with its internal id.";
  raw out "let _delete_value_by_id ~id %s =\n" pgocaml_db_handle_arg;
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
  raw out "    %s : \n" pgocaml_db_handle_arg;
  raw out "    [ `can_start | `can_complete ] t PGOCaml.monad =\n";
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
  raw out "let set_started (t : [> `can_start] t) %s =\n" pgocaml_db_handle_arg;
  raw out "  let id = t.id in\n";
  raw out "  let umm = PGSQL (dbh)\n";
  raw out "    \"UPDATE %s SET g_status = 'Started', g_started = now ()\n\
                \    WHERE g_id = $id\" in\n\
                \    pg_bind umm (fun () -> \
                pg_return ({ id } : [ `can_complete] t)) 
                \n\n\n" name;
  raw out "let set_succeeded (t : [> `can_complete] t) \n\
                   \     ~result %s =\n" pgocaml_db_handle_arg;
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
           (t : [> `can_complete ] t) %s =\n" pgocaml_db_handle_arg;
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
  hide out (fun out ->
    raw out "type 'a _cache = \n(%s) with sexp\n"
      (List.map (db_function_standard_fields result @ args_in_db)
         ~f:pgocaml_type_of_field |> String.concat ~sep:" *\n ");
  );
  doc out "The [cache] is the info retrieved by the database queries; \
               it inherits the capabilities of the handle (type [t]).";
  raw out "type 'a cache = 'a _cache\n\n";

  (* Access a function *)
  doc out "Cache the contents of the evaluation [t].";
  raw out "let cache_evaluation (t: 'a t) %s:\
                 'a cache PGOCaml.monad =\n" pgocaml_db_handle_arg;
  pgocaml_select_from_one_by_id ~out ~raise_wrong_db:raise_wrong_db_error name;

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
      ("started", "`Started", "[ `can_complete]");
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
      raw out "let get_all_%s %s: %s t list PGOCaml.monad =\n"
        suffix pgocaml_db_handle_arg phamtom;
      raw out "  let status_str = \n\
                    \    Enumeration_process_status.to_string %s in\n" polyvar;
      raw out "  let umm = PGSQL(dbh)\n";
      raw out "    \"SELECT g_id FROM %s WHERE g_status = $status_str\" in\n"
        name;
      raw out "  pg_map umm (fun l -> list_map l (fun id -> { id }))\n\n");

  doc out "Get all the [%s] functions." name;
  raw out "let get_all %s: \
          [ `can_nothing ] t list PGOCaml.monad = \n" pgocaml_db_handle_arg;
  pgocaml_do_get_all_ids ~out name;

  doc out "{3 S-Expression Dumps}";
  sexp_functions_for_hidden ~out ~param:"'a" "cache";

  doc out "{3 Low Level Access}";
  (* Delete a function *)
  deprecate out "Deletes a function using its internal identifier.";
  raw out "let _delete_evaluation_by_id ~id %s =\n" pgocaml_db_handle_arg;
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
  raw tmprec "let get_all_values %s: values list PGOCaml.monad =\n\
       \  let thread_fun_list = [\n" pgocaml_db_handle_arg;
  raw tmpfun "let get_all_evaluations %s:
                [ `can_nothing ] evaluations list PGOCaml.monad =\n\
              \  let thread_fun_list = [\n" pgocaml_db_handle_arg;
  let apply_get_tag get tag =
    sprintf "(fun () -> pg_map (%s dbh) (fun l -> list_map l %s));\n" get tag in
  List.iter dsl.nodes (function
    | Record (n, fields) ->
      let get, tag = 
        sprintf "Record_%s.get_all" n, sprintf "(fun x -> `value_%s x)" n in
      raw tmprec "%s" (apply_get_tag get tag)
    | Function (n, args, ret) ->
      let get, tag = 
        sprintf "Function_%s.get_all" n, 
        sprintf "(fun x -> `evaluation_%s x)" n in
      raw tmpfun "%s" (apply_get_tag get tag);
    | _ -> ());
  let the_rest =
    "  ] in\n\
    \  let v_list_list_monad = \
         PGThread.map_s (fun f -> f ()) thread_fun_list in\n\
    \  pg_map v_list_list_monad list_flatten\n\n"
  in
  raw tmprec "%s" the_rest;
  raw tmpfun "%s" the_rest;

  print_tmprec out;
  print_tmpfun out;

  ()

let ocaml_file_system_module ~out dsl = (* For now does not depend on the
                                       actual layout *)
  let def_wrong_db_error, raise_wrong_db = 
    ocaml_exception "Wrong_DB_return" in
  let def_inconsistency_error, raise_inconsistency_error = 
    ocaml_exception ~thread:false "DB_inconsistency" in

  let id_field = "g_id", Int in
  let file_fields = [
    "g_name", String;
    "g_type", String;
    "g_content", Array (Record_name "g_file"); (* A bit hackish, duck-typing. *)
  ] in
  let volume_fields = [
    "g_toplevel", String;
    "g_hr_tag", Option String;
    "g_content", Array (Record_name "g_file");
  ] in


  line out "module File_system = struct";
  def_wrong_db_error out;
  def_inconsistency_error out;
  line out "type volume = { id : int32 }";
  line out "type file = { inode: int32 }";
  line out "type tree = ";
  line out "  | File of string * Enumeration_file_type.t";
  line out "  | Directory of string * Enumeration_file_type.t * tree list";
  line out "  | Opaque of string * Enumeration_file_type.t";
  doc out "Register a new file, directory (with its contents), or opaque \
        directory in the DB.";
  line out "let add_volume %s \
                ~(kind:Enumeration_volume_kind.t) \
                ?(hr_tag: string option) \
                ~(files:tree list) = " pgocaml_db_handle_arg;
  line out "let rec add_tree = function";
  line out "  | File (name, t) | Opaque (name, t) -> begin";
  line out "    let type_str = Enumeration_file_type.to_string t in";
  line out "    let empty_array = [| |] in";
  pgocaml_add_to_database "g_file"
    (List.map file_fields fst)
    [ "$name"; "$type_str" ; "$empty_array" ]
    ~out ~raise_wrong_db ~id:"inode";
  line out "    end";
  line out "  | Directory (name, t, pl) -> begin";
  line out "    let type_str = Enumeration_file_type.to_string t in";
  line out "    let added_file_list_monad = PGThread.map_s add_tree pl in";
  line out "    pg_bind (added_file_list_monad) (fun file_list -> \n";
  line out "       let pg_inodes = array_map (list_to_array file_list) \
                       (fun { inode } ->  inode) in";
  pgocaml_add_to_database "g_file" 
    (List.map file_fields fst)
    [ "$name"; "$type_str" ; "$pg_inodes" ]
    ~out ~raise_wrong_db ~id:"inode";
  line out "  )  end in";
  line out " let added_file_list_monad = PGThread.map_s add_tree files in";
  line out "    pg_bind (added_file_list_monad) (fun file_list -> \n";
  line out "     let pg_inodes = array_map (list_to_array file_list) \n\
                       (fun { inode } ->  inode) in";
  line out " let toplevel = match kind with";
  List.iter dsl.nodes (function
    | Volume (n, t) -> line out "    | `%s -> %S" n t
    | _ -> ());
  line out " in";
  pgocaml_add_to_database "g_volume" 
    (List.map volume_fields fst)
    [ "$toplevel" ; "$?hr_tag"; "$pg_inodes" ]
    ~out ~raise_wrong_db ~id:"id";
  line out ")";

  doc out "Module for constructing file trees less painfully.";
  line out "module Tree = struct";
  line out "let file ?(t=`blob) n = File (n, t)";
  line out "let dir ?(t=`directory) n l = Directory (n, t, l)";
  line out "let opaque ?(t=`opaque) n = Opaque (n, t)";
  line out "end";
  
  doc out "Get all the volumes.";
  raw out "let get_all %s: volume list PGOCaml.monad = \n" pgocaml_db_handle_arg;
  pgocaml_do_get_all_ids ~out ~id:"id" "g_volume";

  let file_stuff_in_db = 
    List.map (id_field :: file_fields) (fun (s, t) ->
      let (t, p) = dsl_type_to_db t in (s, t, p)) in
  let volume_stuff_in_db =
    List.map (id_field :: volume_fields) (fun (s, t) ->
      let (t, p) = dsl_type_to_db t in (s, t, p)) in
  hide out (fun out ->
    doc out "[_file_cache] is hidden.";
    raw out "type _file_cache = \n(%s) with sexp\n\n"
      (List.map (file_stuff_in_db)
         ~f:pgocaml_type_of_field |> String.concat ~sep:" *\n ");
    doc out "[_volume_entry_cache] is hidden.";
    raw out "type _volume_entry_cache = \n(%s) with sexp\n\n"
      (List.map (volume_stuff_in_db)
         ~f:pgocaml_type_of_field |> String.concat ~sep:" *\n ");

    doc out "Retrieve a file from the DB.";
    raw out "let cache_file (file: file) %s: \
                  _file_cache PGOCaml.monad =\n" pgocaml_db_handle_arg;
    pgocaml_select_from_one_by_id 
      ~out ~raise_wrong_db ~get_id:"file.inode" "g_file";

    doc out "A whole volume cache is the volume entry and a \
              list of file caches.";
    line out "type _volume_cache = (_volume_entry_cache * (_file_cache list)) \
              with sexp";
  );

  doc out "The [volume_entry_cache] is the info retrieved by the
      database queries for the \"toplevel part\" of a volume.";
  raw out "type volume_entry_cache = _volume_entry_cache\n\n";
     
  doc out "Retrieve the \"entry\" part of a volume from the DB.";
  raw out "let cache_volume_entry (volume: volume) %s: \
                  volume_entry_cache PGOCaml.monad =\n" pgocaml_db_handle_arg;
  pgocaml_select_from_one_by_id 
    ~out ~raise_wrong_db ~get_id:"volume.id" "g_volume";

  doc out "The [volume_cache] contains the whole volume (entry and files).";
  line out "type volume_cache = _volume_cache";


  doc out "Retrieve a \"whole\" volume.";
  line out "let cache_volume %s (volume: volume) : volume_cache PGOCaml.monad ="
    pgocaml_db_handle_arg;
  line out "  let entry_m = cache_volume_entry ~dbh volume in";
  line out "  pg_bind entry_m (fun entry ->";
  line out "    let files = ref ([] : _file_cache list) in";
  line out "    let (%s) = entry in"
    (List.map (id_field :: volume_fields)
       (function ("g_content", _) -> "contents" | (n, _) -> "_" ) |>
           String.concat ~sep:", ");
  line out "    let rec get_contents arr =";
  line out "      let contents = array_to_list arr in";
  line out "      let f inode = cache_file ~dbh { inode } in";
  line out "      pg_bind (PGThread.map_s f contents) (fun fs ->";
  line out "        files := list_append fs !files;";
  line out "        let todo = list_map fs (fun (%s) -> contents) in"
    (List.map (id_field :: file_fields)
       (function ("g_content", _) -> "contents" | (n, _) -> "_" ) |>
           String.concat ~sep:", ");
  line out "        pg_bind (PGThread.map_s get_contents todo)";
  line out "          (fun _ -> pg_return ())) in";
  line out "    pg_bind (PGThread.map_s get_contents [ contents ])";
  line out "      (fun _ -> pg_return (entry, !files)))";

  doc out "Get the entry from the whole volume.";
  line out "let volume_entry_cache (vec: volume_cache): \
      volume_entry_cache = fst vec";

  doc out "Toplevel volume information.";
  line out "type volume_entry = \
        {vol_id: int32; toplevel: string; hr_tag: string option }";

  doc out "Convert a [volume_entry_cache] to an [volume_entry].";
  line out "let volume_entry (vec: volume_entry_cache) =";
  line out "  let (%s) = vec in"
    (List.map (id_field :: volume_fields)
       (function 
         | ("g_id", _) -> "vol_id" 
         | ("g_toplevel", _) -> "toplevel" 
         | ("g_hr_tag", _) -> "hr_tag" 
         | (n, _) -> "_" ) |> String.concat ~sep:", ");
  line out "  {vol_id; toplevel; hr_tag}";

  doc out "Get a list of trees `known' for a given volume.";
  line out "let volume_trees (vc : volume_cache) : tree list =";
  line out "  let files = snd vc in";
  line out "  let find_file i = \
            match list_find files (fun (%s) -> inode = i) with"
    (List.map (id_field :: file_fields)
       (function ("g_id", _) -> "inode" | (n, _) -> "_" ) |>
           String.concat ~sep:", ");
  line out "    | Some (%s) ->"
    (List.map (id_field :: file_fields)
       (function 
         | ("g_name", _) -> "name" 
         | ("g_type", _) -> "type_str" 
         | ("g_content", _) -> "more_inodes" 
         | (n, _) -> "_" ) |> String.concat ~sep:", ");
  line out "      (name, Enumeration_file_type.of_string_exn type_str, \
                    array_to_list more_inodes)";
  line out "     | None -> ";
  raise_inconsistency_error out "volume_unix_paths: did not find an inode";
  raw out " in\n";
  line out "  let rec go_through_cache inode =";
  line out "    match find_file inode with";
  (* For the future: when the file types are customizable, 
     this will have to change.  *)
  line out "    | name, `opaque, [] -> Opaque (name, `opaque)";
  line out "    | name, `directory, l -> ";
  line out "      Directory (name, `directory, (list_map l go_through_cache))";
  line out "    | name, `blob, [] -> File (name, `blob)";
  line out "    | name, _, _ -> ";
  raise_inconsistency_error out "volume_unix_paths: did not find an inode";
  line out "  in";
  line out "  let (%s) = fst vc in"
    (List.map (id_field :: volume_fields)
       (function 
         | ("g_content", _) -> "vol_content"
         | (n, _) -> "_" ) |> String.concat ~sep:", ");

  line out "  list_map (array_to_list vol_content) go_through_cache\n";

  doc out "{3 S-Expression Dumps}";
  sexp_functions_for_hidden ~out "volume_entry_cache";
  sexp_functions_for_hidden ~out "volume_cache";
  

  let unix_sep = "/" in
  doc out "{3 Unix paths }";
  doc out "Create a Unix directory path for a volume-entry (relative
            to a `root').";
  line out  "let entry_unix_path (ve: volume_entry): string =";
  line out "  Printf.sprintf \"%%s/%%09ld%%s\" ve.toplevel ve.vol_id";
  line out "    (option_value_map ~default:\"\" ve.hr_tag ~f:((^) \"_\"))";

  doc out "Convert a bunch of trees to a list of Unix paths.";
  line out "let trees_to_unix_paths trees =";
  line out "  let paths = ref [] in";
  line out "  let rec descent parent = function";
  line out "  | File (n, _) -> paths := (parent ^ n) :: !paths";
  line out "  | Opaque (n, _) -> paths := (parent ^ n ^ %S) :: !paths" unix_sep;
  line out "  | Directory (n, _, l) -> list_iter l (descent (parent ^ n ^ %S))"
    unix_sep;
  line out "  in";
  line out "  list_iter trees (descent \"\");";
  line out "  !paths";
  line out "end (* File_system *)";
  ()

(* This goes to the dump files through sexplib. *)
let dump_version_string = "0.1-dev"

let ocaml_dump_and_reload ~out dsl =
  let tmp_type, print_tmp_type = new_tmp_output () in
  let tmp_get_fun, print_tmp_get_fun = new_tmp_output () in
  let tmp_get_fun2, print_tmp_get_fun2 = new_tmp_output () in

  doc tmp_type "An OCaml record containing the whole data-base.";
  line tmp_type "type dump = {";
  line tmp_type "  version: string;";
  line tmp_type "  file_system: File_system.volume_cache list;";
 
  doc tmp_get_fun "Retrieve the whole data-base.";
  line tmp_get_fun "let get_dump %s = " pgocaml_db_handle_arg;
  let closing = ref [] in
  line tmp_get_fun2 "pg_return { version = %S;" dump_version_string;

  line tmp_get_fun "pg_bind (File_system.get_all ~dbh) (fun t_list ->";
  line tmp_get_fun "pg_bind (PGThread.map_s (File_system.cache_volume ~dbh) \
                                    t_list) (fun file_system ->";
  line tmp_get_fun2 "     file_system;";
  closing := "))" :: !closing;

  List.iter dsl.nodes (function
    | Enumeration (name, fields) -> ()
    | Record (name, fields) ->
      line tmp_type "  record_%s: Record_%s.cache list;" name name;
      line tmp_get_fun "pg_bind (Record_%s.get_all ~dbh) (fun t_list ->" name;
      line tmp_get_fun "pg_bind (PGThread.map_s (Record_%s.cache_value ~dbh) \
                                    t_list) (fun record_%s ->" name name;
      line tmp_get_fun2 "     record_%s;" name;
      closing := "))" :: !closing;
    | Function (name, args, result) ->
      line tmp_type "  function_%s: [ `can_nothing ] Function_%s.cache list;" 
        name name;
      line tmp_get_fun "pg_bind (Function_%s.get_all ~dbh) (fun t_list ->" name;
      line tmp_get_fun "pg_bind (PGThread.map_s (Function_%s.cache_evaluation \
                                    ~dbh) t_list) (fun function_%s ->" name name;
      line tmp_get_fun2 "     function_%s;" name;
      closing := "))" :: !closing;
    | Volume (_, _) -> ()
  );
  line tmp_type "} with sexp";
  line tmp_get_fun2 "}";
  print_tmp_type out;
  print_tmp_get_fun out;
  print_tmp_get_fun2 out;
  line out "%s" (String.concat ~sep:"" !closing);
  ()

let ocaml_code ?(functorize=true) dsl output_string =
  let out = output_string in
  doc out "Autogenerated module.";
  if functorize then (
    doc out "The signature to provide.";
    line out "\
module type HDB_THREAD = sig
include PGOCaml_generic.THREAD
val err: string -> unit t
val map_s : ('a -> 'b t) -> 'a list -> 'b list t
end\n\n
module Make \
(PGThread : HDB_THREAD) = struct\n\
module PGOCaml = PGOCaml_generic.Make(PGThread)\n";
  );
  doc out "PG'OCaml's connection handle.";
  raw out "type db_handle = (string, bool) Hashtbl.t PGOCaml.t\n\n";
  doc out "Access rights on [Function_*.{t,cache}].";
  raw out "type function_capabilities= [\n\
                \  | `can_nothing\n  | `can_start\n  | `can_complete\n\
                | `can_get_result\n]\n";
  hide out (fun out ->
    raw out "\
open Sexplib.Conv\n\
type pg_timestamptz = PGOCaml.timestamptz\n\
let pg_timestamptz_of_sexp = function \n\
  | Sexplib.Sexp.Atom s -> (PGOCaml.timestamptz_of_string s)\n\
  | _ -> failwith \"pg_timestamptz_of_sexp\"\n\
let sexp_of_pg_timestamptz s = \
    Sexplib.Sexp.Atom (PGOCaml.string_of_timestamptz s)\n\
let option_map o f =\n\
    match o with None -> None | Some s -> Some (f s)\n\n\
let option_value_map = Core.Std.Option.value_map \n\n\
let array_map a f = Core.Std.Array.map ~f a\n\n\
let list_map l f = Core.Std.List.map ~f l\n\n\
let list_iter l f = Core.Std.List.iter ~f l\n\n\
let list_find l f = Core.Std.List.find ~f l\n\n\
let list_append l = Core.Std.List.append l\n\n\
let list_flatten (l : 'a list list) : 'a list = Core.Std.List.flatten l\n\n\
let fold_left = Core.Std.List.fold_left\n\n\
let array_to_list a = Core.Std.Array.to_list a\n\n\
let list_to_array l = Core.Std.Array.of_list l\n\n\
let pg_bind am f = PGOCaml.bind am f\n\n\
let pg_return t = PGOCaml.return t\n\n\
let pg_map am f = pg_bind am (fun x -> pg_return (f x))\n\n\
";
  );
  let volume_kinds = 
    (List.filter_map dsl.nodes 
       (function | Volume (n, _) -> Some n | _ -> None)) in
  let file_types =  ["blob"; "directory"; "opaque"] in
  doc out "\n {3 Virtual File System in The Data-base }\n\n ";
  doc out "Volume `kinds' are defined by the DSL.";
  ocaml_enumeration_module ~out "volume_kind" volume_kinds;
  doc out "File-types are, for now, [`blob], [`directory], and [`opaque].";
  ocaml_enumeration_module ~out "file_type" file_types;
  ocaml_file_system_module ~out dsl;
  doc out "\n {3 Records and Functions of The Layout}\n\n ";
  List.iter dsl.nodes (function
    | Enumeration (name, fields) ->
      doc out "";
      ocaml_enumeration_module ~out name fields
    | Record (name, fields) ->
      ocaml_record_module ~out name fields
    | Function (name, args, result) ->
      ocaml_function_module ~out name args result
    | Volume (_, _) -> ()
  );
  doc out "{3 Top-level Queries On The Data-base }";
  doc out "";
  ocaml_toplevel_values_and_types ~out dsl;
  doc out "\n{3 Dump and Reload The Data-base}\n";
  ocaml_dump_and_reload ~out dsl;
  if functorize then
    line out "end (* Make *)";
  ()
