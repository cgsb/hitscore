open Core.Std

module Psql = Hitscoregen_psql
module Sx = Sexplib.Sexp

open Hitscoregen_layout_dsl

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
  line out ("Printf.ksprintf Flow.IO.log_error \"%s\" " ^^ fmt ^^ ";") metafmt


let rec ocaml_type = function
  | Identifier  -> "int"
  | Bool        -> "bool"
  | Timestamp   -> "Timestamp.t"
  | Int         -> "int"
  | Real        -> "float"      
  | String      -> "string"
  | Option t -> sprintf "%s option" (ocaml_type t)
  | Array t -> sprintf "%s array" (ocaml_type t)
  | Function_name s -> sprintf "Function_%s.pointer" s
  | Enumeration_name s -> sprintf "Enumeration_%s.t" s
  (* | Record_name "g_volume" -> sprintf "pointer" *)
  | Record_name s -> sprintf "Record_%s.pointer" s
  | Volume_name s -> sprintf "File_system.pointer"


    
let filesystem_kinds_and_types dsl =
  let volume_kinds = 
    (List.filter_map dsl.nodes 
       (function | Volume (n, _) -> Some n | _ -> None)) in
  let file_types =  ["blob"; "directory"; "opaque"] in
  (volume_kinds, file_types)

let ocaml_file_system_module out dsl =
  doc out "The [pointer] represents a handle to a volume in the DB.";
  line out "type pointer = { id : int } with sexp";

  doc out "Unsafely create a volume.";
  line out "let unsafe_cast id = { id }";

  doc out "The specification of file-trees.";
  line out "type tree = %s with sexp"
    (String.concat ~sep:"\n" [
      "  | File of string * Enumeration_file_type.t";
      "  | Directory of string * Enumeration_file_type.t * tree list";
      "  | Opaque of string * Enumeration_file_type.t";]);

  doc out "The volume content is …";
  line out "type content = \n\
      \  | Link of pointer
      \  | Tree of string option * tree list  with sexp";
  
  doc out "A volume is then …";
  line out "type t =\n  %s"
    "{ g_id:int; g_kind: Enumeration_volume_kind.t; \
       \  g_content: content; } with sexp";
  doc out "Get the toplevel directory corresponding to a volume-kind.";
  line out " let toplevel_of_kind = function";
  List.iter dsl.nodes (function
  | Volume (n, t) -> line out "    | `%s -> %S" n t
  | _ -> ());
  
  doc out "Get the volume-kind corresponding to a toplevel directory name.";
  line out " let kind_of_toplevel = function";
  List.iter dsl.nodes (function
  | Volume (n, t) -> line out "    | %S -> Ok `%s" t n
  | _ -> ());
  line out "    | s -> Error (`toplevel_not_found s)";


  doc out "Module for constructing file trees less painfully.";
  line out "module Tree = struct";
  line out "let file ?(t=`blob) n = File (n, t)";
  line out "let dir ?(t=`directory) n l = Directory (n, t, l)";
  line out "let opaque ?(t=`opaque) n = Opaque (n, t)";
  line out "end";
  
  ()

let ocaml_encapsulate_layout_errors out ~error_location f =
  line out "let work_m =";
  f out;
  line out "in";
  line out "bind_on_error work_m (fun e -> \
              error (`Layout (%s, e)))" error_location;
  ()
      
let ocaml_record_access_module ~out name fields =
  line out "module Record_%s = struct" name;
  line out "  include Record_%s" name;

  let error_location = sprintf "`Record %S" name in

  line out "let add_value";
  List.iter fields (function
  | (n, Option t) -> line out "   ?(%s:%s option)" n (ocaml_type t);
  | (n, t) -> line out "   ~(%s:%s)" n (ocaml_type t);
  );
  line out " ~dbh =\n   let v = { ";
  List.iter fields (function (n, _) -> raw out "%s; " n);
  line out "} in";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.add_value_sexp ~record_name:%S \
                   (Record_%s.sexp_of_value v) in" name name;
    line out "  Backend.query ~dbh query";
    line out "  >>| Sql_query.single_id_of_result";
    line out "  >>= (function Some id -> return {id} | \n\
             \       None -> error (`wrong_add_value))";
  );

  line out "let value_of_result r = ";
  line out "  let open Sql_query in";
  line out "  try Ok {g_id = r.r_id; g_created = r.r_created; \n\
      \  g_last_modified = r.r_last_modified; g_value = value_of_sexp r.r_sexp } \
      \  with e -> Error (`parse_sexp_error (r.r_sexp, e))";

  line out "let get ~dbh pointer =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.get_value_sexp ~record_name:%S pointer.id in"
      name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun r -> of_result (Sql_query.should_be_single r)";
    line out "  >>= fun r -> of_result (Sql_query.parse_value r)";
    line out "  >>= fun r -> of_result (value_of_result r)";
  );
  
  line out "let get_all ~dbh =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.get_all_values_sexp ~record_name:%S in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun results ->";
    line out "  of_list_sequential results ~f:(fun row ->";
    line out "    of_result Result.(Sql_query.parse_value row >>= value_of_result))";
  );

  line out "let delete_value_unsafe ~dbh v =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.delete_value_sexp \
                            ~record_name:%S v.g_id in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ -> return ()";
  );

  line out "end";

  ()

let ocaml_function_access_module ~out name result_type args =
  line out "module Function_%s = struct" name;
  line out "  include Function_%s" name;

  let error_location = sprintf "`Function %S" name in

  line out "let add_evaluation";
  line out "    ?(recomputable=false)";
  line out "    ?(recompute_penalty=0.)";
  List.iter args (function
  | (n, Option t) -> line out "   ?(%s:%s option)" n (ocaml_type t);
  | (n, t) -> line out "   ~(%s:%s)" n (ocaml_type t);
  );
  line out " ~dbh =\n   let v = { ";
  List.iter args (function (n, _) -> raw out "%s; " n);
  line out "} in";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.add_evaluation_sexp ~function_name:%S \
                   ~recomputable ~recompute_penalty \
                   ~status:(Enumeration_process_status.to_string `Inserted) \
                   (sexp_of_evaluation v) in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>| Sql_query.single_id_of_result";
    line out "  >>= (function Some id -> return {id} | \n\
             \       None -> error `wrong_add_value)";
  );
  
  line out "let evaluation_of_result r = ";
  line out "  let open Sql_query in";
  line out "  try Ok \
    {g_id                = r.f_id;
     g_result            = Option.map ~f:Record_%s.unsafe_cast r.f_result;
     g_recomputable      = r.f_recomputable;
     g_recompute_penalty = r.f_recompute_penalty;
     g_inserted          = r.f_inserted;
     g_started           = r.f_started;
     g_completed         = r.f_completed;
     g_status            = r.f_status;
     g_evaluation   = evaluation_of_sexp r.f_sexp } \
     with e -> Error (`parse_sexp_error (r.f_sexp, e))" result_type;

  line out "let get ~dbh pointer =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.get_evaluation_sexp \
                ~function_name:%S pointer.id in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun r -> of_result (Sql_query.should_be_single r)";
    line out "  >>= fun r -> of_result (Sql_query.parse_evaluation r)";
    line out "  >>= fun r -> of_result (evaluation_of_result r)";
  );
  
  line out "let get_all ~dbh =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out " let query = Sql_query.get_all_evaluations_sexp \
      ~function_name:%S in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun results ->";
    line out "  of_list_sequential results ~f:(fun row ->";
    line out "    of_result Result.(Sql_query.parse_evaluation row >>= \
                 evaluation_of_result))";
  );

  line out "let delete_value_unsafe ~dbh v =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.delete_evaluation_sexp \
                            ~function_name:%S v.g_id in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ -> return ()";
  );

  line out "end";
  
  ()

let ocaml_file_system_access_module ~out dsl =
  line out "module File_system = struct";
  line out "  include File_system";

  let error_location = "`File_system" in

  line out "let add_volume ~kind ~content ~dbh = ";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "let str_kind = Enumeration_volume_kind.to_string kind in";
    line out "  let query = Sql_query.add_volume_sexp ~kind:str_kind \
                   (sexp_of_content content) in";
    line out "  Backend.query ~dbh query";
    line out "  >>| Sql_query.single_id_of_result";
    line out "  >>= (function Some id -> return {id} | \n\
             \       None -> error (`wrong_add_value))";
  );

  line out "let add_tree_volume ~kind ?hr_tag ~files ~dbh =";
  line out "  add_volume ~kind ~dbh ~content:(Tree (hr_tag, files))";
  line out "let add_link_volume ~kind ~dbh pointer =";
  line out "  add_volume ~kind ~dbh ~content:(Link pointer)";

  
  line out "let volume_of_result r = ";
  line out "  let open Sql_query in";
  line out "  try Ok {g_id = r.v_id; \
      \   g_kind = Enumeration_volume_kind.of_string_exn r.v_kind; \n\
      \   g_content = content_of_sexp r.v_sexp } \
      \  with e -> Error (`parse_sexp_error (r.v_sexp, e))";

  line out "let get ~dbh pointer =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.get_volume_sexp pointer.id in";
    line out "  Backend.query ~dbh query";
    line out "  >>= fun r -> of_result (Sql_query.should_be_single r)";
    line out "  >>= fun r -> of_result (Sql_query.parse_volume r)";
    line out "  >>= fun r -> of_result (volume_of_result r)";
  );
  line out "let get_all ~dbh =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.get_all_volumes_sexp () in";
    line out "  Backend.query ~dbh query";
    line out "  >>= fun results ->";
    line out "  of_list_sequential results ~f:(fun row ->";
    line out "    of_result Result.(Sql_query.parse_volume row \
                    >>= volume_of_result))";
  );

  line out "let delete_volume_unsafe ~dbh v =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.delete_volume_sexp v.g_id in";
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ -> return ()";
  );
  line out "end"

  
let ocaml_module raw_dsl dsl output_string =
  let out = output_string in
  doc out "Autogenerated Layout module.";
  line out "open Hitscore_std";
  line out "open Hitscore_db_backend";

  let (volume_kinds, file_types) = filesystem_kinds_and_types dsl in
  let all_nodes =
    Enumeration ("volume_kind", volume_kinds)
    :: Enumeration ("file_type", file_types)
    :: dsl.nodes in

  doc out "Enumeration types modules";
  List.iter all_nodes (function
  | Enumeration (name, fields) ->
    line out "module Enumeration_%s = struct" name;
    doc out "The type of {i %s} items." name;
    line out "type t = [%s] with sexp"
      (List.map fields (sprintf "`%s") |! String.concat ~sep:" | ");
    
    line out "let to_string : t -> string = function\n| %s\n" 
      (List.map fields (fun s -> sprintf "`%s -> \"%s\"" s s) |!
          String.concat ~sep:"\n| ");
    line out "let of_string_exn: string -> t = function\n| %s\n" 
      (List.map fields (fun s -> sprintf "\"%s\" -> `%s" s s) |!
          String.concat ~sep:"\n| ");
    line out "| s -> raise (Failure %S)"
      (sprintf "Enumeration_%s.of_string" name);
    
    line out "let of_string s = try Core.Std.Ok (of_string_exn s) \
                          with e -> Core.Std.Error s";
    line out "end";
  | _ -> ()
  );
  doc out "File-system types module";
  line out "module File_system = struct";
  ocaml_file_system_module out dsl;
  line out "end";
  
  let record_standard_fields = [
    ("g_id", Identifier);
    ("g_created", Timestamp);
    ("g_last_modified", Timestamp);
  ] in
  let function_standard_fields result = [
    ("g_id"                , Identifier);
    ("g_result"            , Option (Record_name result));
    ("g_recomputable"      , Bool);
    ("g_recompute_penalty" , Real);
    ("g_inserted"          , Timestamp);
    ("g_started"           , Option Timestamp);
    ("g_completed"         , Option Timestamp);
    ("g_status"            , Enumeration_name "process_status");
  ] in
  List.iter all_nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    line out "module Record_%s = struct" name;
    line out "type pointer = { id: int}  with sexp";
    line out "type value = {\n %s} with sexp"
      (List.map fields (fun (n,t) -> sprintf "%s : %s" n (ocaml_type t))
       |! String.concat ~sep:";\n  ");
    line out "type t = {%s;\n  g_value: value} with sexp" 
      (List.map record_standard_fields
         ~f:(fun (n,t) -> sprintf "%s : %s" n (ocaml_type t))
       |! String.concat ~sep:";\n  ");
    line out "let pointer v = { id = v.g_id} ";
    line out "let unsafe_cast id = { id }";
    line out "end";
  | Function (name, args, result) ->
    line out "module Function_%s = struct" name;
    line out "type pointer = { id: int}  with sexp";
    line out "type evaluation = {\n %s} with sexp"
      (List.map args (fun (n,t) -> sprintf "%s : %s" n (ocaml_type t))
       |! String.concat ~sep:";\n  ");
    line out "type t = {%s;\n  g_evaluation: evaluation} with sexp" 
      (List.map (function_standard_fields result)
         ~f:(fun (n,t) -> sprintf "%s : %s" n (ocaml_type t))
       |! String.concat ~sep:";\n  ");
    line out "let pointer v = { id = v.g_id} ";
    line out "end";
  | Volume (_, _) -> ()
  );

  (*
  let record_db_reprsentation_fields =
    let standard = List.map record_standard_fields fst |! List.tl_exn in
    standard @ ["g_sexp"] in
  *)
  
  doc out "Queries manipulating the stuff in the layout";
  line out "module Make_access \
    (Flow: Sequme_flow_monad.FLOW_MONAD)
    (Backend : module type of Hitscore_db_backend.Make(Flow))
    = struct";

  line out "open Flow";
  ocaml_file_system_access_module ~out dsl;
  List.iter all_nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    ocaml_record_access_module ~out name fields
  | Function (name, args, result) ->
    ocaml_function_access_module ~out name result args
  | Volume (_, _) -> ()
  );
  line out "end";
  ()


(* -------------------------------------------------------------------------- *)  

let (|>) x f = f x

type fashion = [`implementation|`interface|`types]


let pgocaml_type_of_field =
  let rec props  = function
    | [ Psql.Array    ; Psql.Not_null] -> sprintf "%s array"
    | [ Psql.Not_null ] -> sprintf "%s"
    | _ -> sprintf "%s option" in
  function
    | (_, Psql.Timestamp          , p, tag) -> (props p) "inexistent_timestamptz"
    | (_, Psql.Identifier         , p, tag) -> (props p) "int32"              
    | (_, Psql.Text               , p, `timestamp) -> (props p) "Timestamp.t"
    | (_, Psql.Text               , p, tag) -> (props p) "string"             
    | (_, Psql.Integer            , p, tag) -> (props p) "int32"              
    | (_, Psql.Real               , p, tag) -> (props p) "float"              
    | (_, Psql.Bool               , p, tag) -> (props p) "bool"               
    | (_, Psql.Pointer (r, "g_id"), p, `id) -> 
      (props p) (sprintf "Record_%s.pointer" r)
    | (_, Psql.Pointer (r, "g_id"), p, tag) -> (props p) "int32"
    | _ -> failwith "Can't compile DB type to PGOCaml"
      
let rec ocaml_type = function
  | Identifier  -> "int32"
  | Bool        -> "bool"
  | Timestamp   -> "Timestamp.t"
  | Int         -> "int32"
  | Real        -> "float"      
  | String      -> "string"
  | Option t -> sprintf "%s option" (ocaml_type t)
  | Array t -> sprintf "%s array" (ocaml_type t)
  | Function_name s -> sprintf "Function_%s.pointer" s
  | Enumeration_name s -> sprintf "Enumeration_%s.t" s
  | Record_name "g_volume" -> sprintf "pointer"
  | Record_name s -> sprintf "Record_%s.pointer" s
  | Volume_name s -> sprintf "File_system.pointer"

let let_in_typed_value  = function
  | (n, Enumeration_name e) -> 
    sprintf "  let %s = Enumeration_%s.to_string %s in\n" n e n
  | (n, Option (Enumeration_name e)) -> 
    sprintf "  let %s = option_map %s Enumeration_%s.to_string in\n" n n e
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
  | (n, Timestamp) ->
    sprintf "let %s = Timestamp.to_string %s in"  n n
  | (n, Option Timestamp) ->
    sprintf "let %s = option_map %s Timestamp.to_string in\n"  n n
  | (n, Array Timestamp) ->
    sprintf "let %s = array_map %s Timestamp.to_string in\n"  n n
  | (n, Array Real) ->
    sprintf "let %s = Sexplib.Sexp.to_string \
            (Core.Std.Array.sexp_of_t Core.Std.Float.sexp_of_t %s) in\n" n n
  | (n, Array String) ->
    sprintf "let %s = Sexplib.Sexp.to_string \
            (Core.Std.Array.sexp_of_t Core.Std.String.sexp_of_t %s) in\n" n n
  | (n, Array (Enumeration_name e)) -> 
    sprintf "let %s = Sexplib.Sexp.to_string \
            (Core.Std.Array.sexp_of_t Enumeration_%s.sexp_of_t %s) in\n" n e n
  | _ -> ""

let convert_pgocaml_type = function
  | (n, Enumeration_name e) -> 
    sprintf "(Enumeration_%s.of_string_exn %s)" e n
  | (n, Option (Enumeration_name e)) -> 
    sprintf "(option_map %s Enumeration_%s.of_string_exn)" n e
  | (n, Record_name r) ->
    sprintf "{ Record_%s.id = %s }" r n
  | (n, Option (Record_name r)) ->
    sprintf "(option_map %s (fun id -> { Record_%s.id }))" n r
  | (n, Array (Record_name r)) ->
    sprintf "(array_map %s (fun id ->  { Record_%s.id }))" n r
  | (n, Volume_name v) ->
    sprintf "{ File_system.id = %s } " n
  | (n, Option (Volume_name r)) ->
    sprintf "(option_map %s (fun id -> { File_system.id }))" n
  | (n, Array (Volume_name r)) ->
    sprintf "(array_map %s (fun id ->  { File_system.id }))" n
  | (n, Timestamp) ->
    sprintf "(Timestamp.of_string %s)" n
  | (n, Option Timestamp) ->
    sprintf "(option_map %s Timestamp.of_string)" n
  | (n, Array Timestamp) ->
    sprintf "(array_map %s Timestamp.of_string)" n
  | (n, Array Real) ->
    sprintf "Core.Std.(Array.t_of_sexp Float.t_of_sexp (Sexplib.Sexp.of_string %s))" n
  | (n, Array String) ->
    sprintf "Core.Std.(Array.t_of_sexp String.t_of_sexp (Sexplib.Sexp.of_string %s))" n
  | (n, Array (Enumeration_name e)) -> 
    sprintf "Core.Std.(Array.t_of_sexp Enumeration_%s.t_of_sexp \
              (Sexplib.Sexp.of_string %s))" e n
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
  line out ("Printf.ksprintf Flow.IO.log_error \"%s\" " ^^ fmt ^^ ";") metafmt

let line_mli ~fashion out fmt =
  match fashion with
  | `interface -> line out fmt
  | `implementation | `types -> line ignore fmt

let line_ml ~fashion out fmt =
  match fashion with
  | `interface | `types -> line ignore fmt
  | `implementation -> line out fmt

let hide ?(fashion:fashion=`implementation) out (f: (string -> unit) -> unit) =
  match fashion with
  | `implementation ->
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
  | `interface | `types -> ()

module OCaml_hiden_exception = struct 
  type t = {
    module_name: string;
    name: string;
    types: string list;
    in_dumps: bool;
    in_loads: bool;
    in_gets: bool;
  } 
  let _all_exceptions = ref []

  let make ?(in_dumps=false) ?(in_loads=false) ?(in_gets=false)
      module_name name types =
    let ohe = 
      { module_name ; name ; types; in_dumps; in_loads; in_gets } in
    _all_exceptions := ohe :: !_all_exceptions;
    ohe
      
  let define ?(fashion:fashion option) t out = 
    hide ?fashion out (fun out ->
      line out "exception Local_exn_%s of %s" 
        t.name (String.concat ~sep:" * " t.types);
    )
      
  let throw t out what =
    line out "(Flow.IO.fail (Local_exn_%s %s))" t.name what
  
  let explode_module_name nm =
    match String.lsplit2 ~on:'_' nm with
    | Some ("Record", r) -> sprintf "`Record %S" r
    | Some ("Function", r) -> sprintf "`Function %S" r
    | Some ("File", "system") -> sprintf "`File_system"
    | _ -> failwithf "explode_module_name, wrong module name: %S" nm ()

      
  let transform_local t =
    let vals = 
      (String.concat ~sep:", " (List.mapi t.types (fun i x -> sprintf "x%d" i)))
    in
    sprintf 
      "`pg_exn (Local_exn_%s (%s)) -> \
        `layout_inconsistency (%s, `%s (%s))"
      t.name vals (explode_module_name t.module_name) t.name vals

  let transform_global t =
    let vals = 
      (String.concat ~sep:", " (List.mapi t.types (fun i x -> sprintf "x%d" i))) in
    sprintf 
      "`pg_exn (%s.Local_exn_%s (%s)) -> \
        `layout_inconsistency (%s, `%s (%s))"
      t.module_name t.name vals (explode_module_name t.module_name) t.name vals
      
  let poly_type_local tl = 
    sprintf "`layout_inconsistency of \
              [> %s ] * [> %s]"
      (String.concat ~sep:"\n | "
         (List.dedup (List.map tl (fun t ->
           match String.lsplit2 ~on:'_' t.module_name with
           | Some ("Record", r) -> sprintf "`Record of string"
           | Some ("Function", r) -> sprintf "`Function of string"
           | Some ("File", "system") -> sprintf "`File_system"
           | _ ->
             failwithf "explode_module_name, wrong module name: %S"
               t.module_name ()))))      
      (String.concat ~sep:"\n  | " (List.dedup (List.map tl (fun t ->
        sprintf "`%s of (%s)" t.name (String.concat ~sep:" * " t.types)))))
      
  let poly_type_global tl = poly_type_local tl

  let all_dumps () = 
    List.filter !_all_exceptions ~f:(fun t -> t.in_dumps)
  let all_gets () = 
    List.filter !_all_exceptions ~f:(fun t -> t.in_gets)
  let all_loads () = 
    List.filter !_all_exceptions ~f:(fun t -> t.in_loads)

end

let ocaml_poly_flow out left right =
  line out "(%s, [> %s ]) Flow.monad" 
    left (String.concat ~sep:" | " right) 

let pgocaml_do_get_all_ids ~out ?(id="id") name = 
  raw out "  let um = PGSQL(dbh)\n";
  raw out "    \"SELECT g_id FROM %s\" in\n" name;
  raw out "  pg_map um (fun l -> list_map l (fun %s -> { %s }))\n\n" id id;
  ()

let pgocaml_select_from_one_by_id_exn 
    ~out ~on_not_one_id ?(get_id="t.id") table_name  =
  raw out "  let id = %s in\n" get_id;
  raw out "  let um = PGSQL (dbh)\n";
  raw out "    \"SELECT * FROM %s WHERE g_id = $id\" in\n" table_name;
  raw out "  pg_bind um (function [one] -> pg_return one\n    | l -> ";
  on_not_one_id ~out ~table_name ~returned:"l";
  raw out ")\n";
  ()

let pgocaml_db_handle_arg = "~(dbh: db_handle)"

let pgocaml_add_to_database_exn ~out ~on_not_one_id  ?(id="id")
    table_name intos values =
  raw out "  let i32_list_monad = PGSQL (dbh)\n";
  raw out "    \"INSERT INTO %s (%s)\n     VALUES (%s)\n\
                   \     RETURNING g_id \" in\n" table_name
    (intos |> String.concat ~sep:", ")
    (values|> String.concat ~sep:", ");
  raw out "  pg_bind i32_list_monad \n\
                    \    (function\n\
                    \       | [ %s ] -> PGOCaml.return { %s }\n\
                    \       | l -> " id id;
  on_not_one_id ~out  ~table_name ~returned:"l";
  raw out ")\n\n";
  ()

let pgocaml_to_flow ?(fashion=`implementation) out ?transform_exceptions f =
  match fashion with
  | `implementation ->
    line out "let _exn_version () =";
    f out;
    line out "in";
    begin match transform_exceptions with
    | None ->
      line out "catch_pg_exn _exn_version ()";
    | Some l ->
      line out "Flow.bind_on_error (catch_pg_exn _exn_version ())";
      line out "  (fun e -> Flow.error \
                    (match e with %s | `pg_exn e -> `pg_exn e))"
        (String.concat ~sep:"\n   | " l);
    end
  | `interface | `types -> ()

let pgocaml_insert_in_db_with_id_exn
    ~out ~on_not_one_id ?id name all_db_fields values =
  line out "let t_m = ";
  pgocaml_add_to_database_exn ?id name all_db_fields values
    ~out ~on_not_one_id;
  line out "  in pg_bind t_m (fun t ->";
  line out "    pg_bind (PGSQL(dbh) \"SELECT max(g_id) from %s\")" name;
  line out "      (function [Some max] -> (pg_bind ";
  line out "          (let m64 = Int64.(add (of_int32 max) 1L) in";
  line out "           PGSQL(dbh) \"select setval('%s_g_id_seq', $m64)\")" name;
  line out "          (fun _ -> pg_return t))";
  line out "       | l -> Flow.IO.fail (Failure \
                           \"Postgres is itself inconsistent\")))";
  ()

let ocaml_start_module ~out ~name = function
  | `implementation -> raw out "module %s = struct\n" name
  | `interface -> raw out "module %s : sig\n" name
  | `types -> ()

let ocaml_sexped_type ~out ~name ?(privated=false) ?param ~(fashion:fashion) value = 
  match fashion with
  | `implementation | `types -> 
    line out "type %s%s = %s with sexp"
      (Option.value_map param ~default:"" ~f:(sprintf "%s "))
      name value
  | `interface -> 
    line out "type %s%s = %s%s"
      (Option.value_map param ~default:"" ~f:(sprintf "%s "))
      name (if privated then "private " else "") value;
    doc out "Sexplib functions may raise exceptions.";
    line out "val %s_of_sexp:%s Sexplib.Sexp.t -> %s%s" name
      (Option.value_map param ~default:"" ~f:(sprintf " %s ->"))
      (Option.value_map param ~default:"" ~f:(sprintf "%s ")) name;
    doc out "Sexplib functions may raise exceptions.";
    line out "val sexp_of_%s:%s %s -> Sexplib.Sexp.t" name
      (Option.value_map param ~default:"" ~f:(fun s -> sprintf " %s -> %s" s s))
      name;
    ()

let ocaml_enumeration_module ~fashion ~out name fields =
  ocaml_start_module ~out ~name:(sprintf "Enumeration_%s" name) fashion;
  
  doc out "The type of {i %s} items." name;
  raw (ocaml_sexped_type ~out ~name:"t" ~fashion) "Types.%s" name;

  doc out "Convert to a string.";
  line_mli ~fashion out "val to_string: t -> string";
  line_ml ~fashion out "let to_string : t -> string = function\n| %s\n" 
    (List.map fields (fun s -> sprintf "`%s -> \"%s\"" s s) |>
        String.concat ~sep:"\n| ");
  
  hide ~fashion out (fun out ->
    line out "exception Of_string_error of string";
    line out "let of_string_exn: string -> t = function\n| %s\n" 
      (List.map fields (fun s -> sprintf "\"%s\" -> `%s" s s) |>
          String.concat ~sep:"\n| ");
    line out "| s -> raise (Of_string_error s)";
  );

  doc out "[of_string s] returns [Error s] if [s] cannot be recognized";
  line_mli ~fashion out "val of_string: string -> (t, string) Core.Std.Result.t";
  line_ml ~fashion out "let of_string s = try Core.Std.Ok (of_string_exn s) \
                          with e -> Core.Std.Error s";
  line out "\n\nend (* %s *)\n" name;
  ()

let ocaml_record_module ~out ~(fashion: fashion) name fields = 
  let out_ml = 
    match fashion with | `implementation -> out | `interface | `types -> ignore in
  let out_mli = 
    match fashion with | `implementation | `types -> ignore | `interface -> out in

  ocaml_start_module ~out ~name:(sprintf "Record_%s" name) fashion;

  doc out "A handle to a given record-value in the data-base (access to \
          the [id] field is there for hackability/emergency purposes).";
  ocaml_sexped_type ~out ~name:"pointer" ~privated:true ~fashion "{ id: int32 }";

  doc out "Unsafely create a pointer.";
  line out_mli "val unsafe_cast: int32 -> pointer";
  line out_ml "let unsafe_cast id = { id }";

  doc out "The type [t] represents the contents of a value (a copy of
          the database record in the memory of the process).";
  raw
    (ocaml_sexped_type ~out ~name:"t" ~fashion)
    "{%s}"
    (List.map (record_standard_fields @ fields) ~f:(fun (s, t) ->
      sprintf "%s: %s;" s (ocaml_type t))
        |! String.concat ~sep:"");

  let wrong_add_value = 
    OCaml_hiden_exception.make 
      (sprintf "Record_%s" name) 
      "insert_did_not_return_one_id"
      [ "string"; "int32 list" ] in
  let on_not_one_id ~out ~table_name  ~returned  =
    OCaml_hiden_exception.throw wrong_add_value out
      (sprintf "(%S,  %s)" table_name returned) in
  OCaml_hiden_exception.define wrong_add_value out_ml;

  (* Function to add a new value: *)
  doc out "Create a new value of type [%s] in the database, and \
              return a handle to it." name;
  line out_mli "val add_value:";
  line out_ml "let add_value";
  List.iter fields (function
  | (n, Option t) ->
    line out_ml "   ?(%s:%s option)" n (ocaml_type t);
    line out_mli "   ?%s:%s ->" n (ocaml_type t);
  | (n, t) ->
    line out_ml "   ~(%s:%s)" n (ocaml_type t);
    line out_mli "   %s:%s ->" n (ocaml_type t);
  );
  line out_ml "    %s :" pgocaml_db_handle_arg;
  line out_mli "    dbh:db_handle ->";
  ocaml_poly_flow out "pointer" [
    (OCaml_hiden_exception.poly_type_local [wrong_add_value]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_flow out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_add_value;
  ] (fun out ->
    line out "  let now = Timestamp.(to_string (now ())) in";
    let intos = "g_created" :: "g_last_modified" :: List.map fields fst in
    let values =
      let prefix (n, t) =
        (match type_is_option t with `yes -> "$?" | `no -> "$") ^ n in
      "$now" :: "$now" :: List.map fields prefix  in
    List.iter fields (fun tv -> let_in_typed_value tv |> raw out "%s");
    pgocaml_add_to_database_exn name intos values ~out ~on_not_one_id;
  );

  hide out_ml (fun out ->
    line out "let get_all_exn %s: pointer list PGOCaml.monad ="
      pgocaml_db_handle_arg;
    pgocaml_do_get_all_ids ~out name;
  );
  doc out "Get all the values of type [%s]." name;
  line out_mli "val get_all: dbh:db_handle ->";
  line out_ml "let get_all %s:" pgocaml_db_handle_arg;
  ocaml_poly_flow out "pointer list" ["`pg_exn of exn";];
  line out_ml " =";
  pgocaml_to_flow out_ml (fun out ->
    line out "get_all_exn ~dbh";
  );

  let wrong_select_one = 
    OCaml_hiden_exception.make ~in_dumps:true 
      (sprintf "Record_%s" name) 
      "select_did_not_return_one_tuple"
      [ "string"; "int" ] in
  let on_not_one_id ~out ~table_name  ~returned  =
    OCaml_hiden_exception.throw wrong_select_one out
      (sprintf "(%S, List.length %s)" table_name returned) in
  OCaml_hiden_exception.define wrong_select_one out_ml;

  hide out_ml (fun out ->
    line out "let get_value_exn t %s =" pgocaml_db_handle_arg;
    line out " let tuple_m = ";
    pgocaml_select_from_one_by_id_exn ~out ?get_id:None ~on_not_one_id name;
    line out " in";
    line out " pg_bind tuple_m (fun (%s,%s) -> "
      (List.map record_standard_fields ~f:fst |! String.concat ~sep:", ")
      (List.map fields ~f:fst |! String.concat ~sep:", ");

    line out "    pg_return {";
    List.iter (record_standard_fields @ fields) (fun (v, t) ->
      line out "      %s = %s;" v (convert_pgocaml_type (v, t)));
    line out "    })\n";

  );

  doc out "Get the contents of the record at [pointer].";
  line out_mli "val get: pointer -> dbh:db_handle ->";
  line out_ml "let get (pointer: pointer) %s:" pgocaml_db_handle_arg;
  ocaml_poly_flow out "t" [
    (OCaml_hiden_exception.poly_type_local [wrong_select_one]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_flow out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_select_one;
  ] (fun out ->
    line out "get_value_exn ~dbh pointer";
  );
  (*
  doc out "Get the time when the record was created modified (It depends on \
          the good-will of the people modifying the database {i manually}).";
  line out "let created (t: t): Timestamp.t option = t.g_created";

  doc out "Get the last time the record was modified (It depends on \
          the good-will of the people modifying the database {i manually}).";
  line out "let last_modified (t: t): Timestamp.t option = t.g_last_modified\n";
  *)

  doc out "{3 Low Level Access}";
  (* Delete a value *)
  hide out_ml (fun out ->
    line out "let delete_value_exn %s (p:pointer) =" pgocaml_db_handle_arg;
    line out "  let id = p.id in";
    line out "  PGSQL (dbh)";
    line out "    \"DELETE FROM %s WHERE g_id = $id\"\n" name;
  );
  doc out "Delete a value.";
  line out_mli "val delete_value: dbh:db_handle -> pointer ->";
  line out_ml "let delete_value %s (p: pointer):" pgocaml_db_handle_arg;
  ocaml_poly_flow out "unit" [
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_flow out_ml ~transform_exceptions:[
  ] (fun out ->
    line out "(delete_value_exn ~dbh p)";
  );


  let wrong_cache_insert = 
    OCaml_hiden_exception.make ~in_loads:true
      (sprintf "Record_%s" name)
      "insert_cache_did_not_return_one_id"
      [ "string"; "int32 list" ] in
  let on_not_one_id ~out ~table_name  ~returned  =
    OCaml_hiden_exception.throw wrong_cache_insert out
      (sprintf "(%S, %s)" table_name returned) in
  OCaml_hiden_exception.define wrong_cache_insert out_ml;

  hide out_ml (fun out ->  
    line out "let insert_value_exn %s (t: t): pointer PGOCaml.monad = " 
      pgocaml_db_handle_arg;
    List.iter (record_standard_fields @ fields) ~f:(fun (s, t) ->
      line out "  let %s = t.%s in" s s;
      line out "  %s" (let_in_typed_value (s,t))
    );
    let all_field_names = List.map (record_standard_fields @ fields) fst in
    let all_field_names_prefixed =
      let prefix (n, t) =
        match type_is_option t with 
          `yes -> sprintf "$?%s" n | `no -> sprintf "$%s" n in 
      List.map (record_standard_fields @ fields) prefix in
    pgocaml_insert_in_db_with_id_exn
      ~out ~on_not_one_id name all_field_names all_field_names_prefixed;

  );

  doc out "Load a value in the database ({b Unsafe!}).";
  line out_mli "val insert_value: dbh:db_handle -> t ->";
  line out_ml "let insert_value %s (v: t):" pgocaml_db_handle_arg;
  ocaml_poly_flow out "pointer" [
    (OCaml_hiden_exception.poly_type_local [wrong_cache_insert]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_flow out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_cache_insert;
  ] (fun out ->
    line out "(insert_value_exn ~dbh v)";
  );

  line out "\nend (* %s *)\n" name;
  ()

let ocaml_function_module ~out ~fashion name args result =
  let out_ml = 
    match fashion with | `implementation -> out | `interface | `types -> ignore in
  let out_mli = 
    match fashion with | `implementation -> ignore | `interface | `types -> out in

  ocaml_start_module ~out ~name:(sprintf "Function_%s" name) fashion;

  doc out "A handle to a given function-evaluation, \
            with {!function_capabilities} as access-rights.";
  ocaml_sexped_type ~out ~fashion ~param:"'capabilities" ~privated:true
    ~name:"pointer" "{ id: int32 }";

  doc out "Unsafely create a pointer.";
  line out_mli "val unsafe_cast: int32 -> 'a pointer";
  line out_ml "let unsafe_cast id = { id }";


  raw
    (ocaml_sexped_type ~out ~fashion ~param:"'capabilities" ~name:"t")
    "{%s}"
    (List.map (function_standard_fields result @ args) ~f:(fun (s, t) ->
      sprintf "%s: %s" s (ocaml_type t))
      |! String.concat ~sep:";\n");

  let let_now out =
    line out "  let now = Timestamp.(to_string (now ())) in" in

  let wrong_add = 
    OCaml_hiden_exception.make 
      (sprintf "Function_%s" name) 
      "insert_did_not_return_one_id"
      [ "string"; "int32 list" ] in
  let on_not_one_id ~out ~table_name  ~returned  =
    OCaml_hiden_exception.throw wrong_add out
      (sprintf "(%S, %s)" table_name returned) in
  OCaml_hiden_exception.define wrong_add out_ml;

  (* Function to insert a new function evaluation: *)
  line out_mli "val add_evaluation:";
  line out_ml "let add_evaluation";
  List.iter args (function
  | (n, Option t) ->
    line out_ml "   ?(%s:%s option)" n (ocaml_type t);
    line out_mli "   ?%s:%s ->" n (ocaml_type t);
  | (n, t) ->
    line out_ml "   ~(%s:%s)" n (ocaml_type t);
    line out_mli "   %s:%s ->" n (ocaml_type t);
  );
  line out_ml  "    ?(recomputable=false)";
  line out_mli "    ?recomputable:bool ->";
  line out_ml  "    ?(recompute_penalty=0.)";
  line out_mli "    ?recompute_penalty: float -> dbh:db_handle ->";
  line out_ml "    %s :" pgocaml_db_handle_arg;
  ocaml_poly_flow out "[ `can_start | `can_complete ] pointer" [
    (OCaml_hiden_exception.poly_type_local [wrong_add]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_flow out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_add;
  ] (fun out ->
    List.iter args (fun tv -> let_in_typed_value tv |> raw out "%s");
    let_now out;
    let intos =
      "g_recomputable" :: "g_recompute_penalty" :: "g_inserted" :: "g_status" :: 
        (List.map args fst) in
    let values =
      let prefix (n, t) =
        (match type_is_option t with `yes -> "$?" | `no -> "$") ^ n in
      "$recomputable" :: "$recompute_penalty" :: "$now" :: "'Inserted'" ::
        (List.map args prefix) in
    pgocaml_add_to_database_exn name intos values ~out ~on_not_one_id
  );

  (* Function to set the state of a function evaluation to 'STARTED': *)
  line out_mli "val set_started: [> `can_start] pointer -> dbh:db_handle ->";
  line out_ml  "let set_started (p : [> `can_start] pointer) %s:" 
    pgocaml_db_handle_arg;
  ocaml_poly_flow out "[`can_complete] pointer" ["`pg_exn of exn";];
  line out_ml " =";
  pgocaml_to_flow out_ml (fun out ->
    raw out "  let id = p.id in\n";
    let_now out;
    raw out "  let umm = PGSQL (dbh)\n";
    raw out "    \"UPDATE %s SET g_status = 'Started', g_started = $now\n\
              \    WHERE g_id = $id\" in\n\
              \    pg_bind umm (fun () -> \
              pg_return ({ id } : [ `can_complete] pointer)) 
              \n" name;
  );

  doc out "Set the evaluation as a success.";
  line out_mli "val set_succeeded: [> `can_complete] pointer -> \
                result:%s -> dbh:db_handle ->"
    (ocaml_type (Record_name result));
  raw out_ml "let set_succeeded (p : [> `can_complete] pointer) \n\
                   \     ~result %s:\n" pgocaml_db_handle_arg;
  ocaml_poly_flow out "[ `can_get_result] pointer" ["`pg_exn of exn";];
  line out_ml " =";
  pgocaml_to_flow out_ml (fun out ->
    let_in_typed_value ("result", Record_name result) |> raw out "%s";
    raw out "  let id = p.id in\n";
    let_now out;
    raw out "  let umm = PGSQL (dbh)\n";
    raw out "    \"UPDATE %s SET g_status = 'Succeeded', \
                g_completed = $now, g_result = $result\n \
                \    WHERE g_id = $id\" in\n\
                \    pg_bind umm (fun () -> \
                pg_return ({ id } : [ `can_get_result] pointer)) 
                \n" name;
  );

  doc out "Set the evaluation failed.";
  line out_mli "val set_failed: [> `can_complete ] pointer -> dbh:db_handle ->";
  line out_ml "let set_failed \
           (p : [> `can_complete ] pointer) %s:" pgocaml_db_handle_arg;
  ocaml_poly_flow out "[ `can_nothing ] pointer" ["`pg_exn of exn";];
  line out_ml " =";
  pgocaml_to_flow out_ml (fun out ->
    raw out "  let id = p.id in\n";
    let_now out;
    raw out "  let umm = PGSQL (dbh)\n";
    raw out "    \"UPDATE %s SET g_status = 'Failed', g_completed = $now\n\
                \    WHERE g_id = $id\" in\n\
                \    pg_bind umm (fun () -> \
                pg_return ({ id } : [ `can_nothing ] pointer)) 
                \n" name;
  );

  let wrong_select_one = 
    OCaml_hiden_exception.make ~in_dumps:true
      (sprintf "Function_%s" name) 
      "select_did_not_return_one_tuple"
      [ "string"; "int" ] in
  let on_not_one_id ~out ~table_name  ~returned  =
    OCaml_hiden_exception.throw wrong_select_one out
      (sprintf "(%S, List.length %s)" table_name returned) in
  OCaml_hiden_exception.define wrong_select_one out_ml;

  (* Access a function *)
  hide out_ml (fun out ->
    line out "let get_evaluation_exn %s t ="
      pgocaml_db_handle_arg;
    line out " let tuple_m = ";
    pgocaml_select_from_one_by_id_exn ~out ?get_id:None ~on_not_one_id name;
    line out " in";
    line out " pg_bind tuple_m (fun (%s) -> "
      (List.map (function_standard_fields result @ args) ~f:fst
       |! String.concat ~sep:", ");

    line out "    pg_return {";
    List.iter (function_standard_fields result @ args) ~f:(fun (v, t) ->
      line out "      %s = %s;" v (convert_pgocaml_type (v, t)));
    line out "    })\n";
  );

  doc out "Get the contents of the evaluation [t].";
  line out_mli "val get: 'any pointer -> dbh:db_handle ->";
  line out_ml "let get (p: 'any pointer) %s : " pgocaml_db_handle_arg;
  ocaml_poly_flow out "'any t" [
    (OCaml_hiden_exception.poly_type_local [wrong_select_one]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_flow out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_select_one;
  ] (fun out ->
    line out "get_evaluation_exn ~dbh p";
  );

  doc out "Get the status of an evaluation [t].";
  line out_mli "val get_status: 'a pointer ->  dbh:db_handle ->";
  line out_ml "let get_status pointer ~dbh ";
  ocaml_poly_flow out_mli "Enumeration_process_status.t" [
    (OCaml_hiden_exception.poly_type_local [wrong_select_one]);
    "`pg_exn of exn";
  ];
  line out_ml " = Flow.(get pointer ~dbh >>= fun t -> return t.g_status)";
  
  List.iter 
    [ ("inserted", "`Inserted", "[ `can_start | `can_complete]");
      ("started", "`Started", "[ `can_complete]");
      ("failed", "`Failed", "[ `can_nothing ]");
      ("succeeded", "`Succeeded", "[ `can_get_result ]"); ]
    (fun (suffix, polyvar, phamtom) -> 
      hide out_ml (fun out ->
        raw out "let get_all_%s_exn %s: %s pointer list PGOCaml.monad =\n"
          suffix pgocaml_db_handle_arg phamtom;
        raw out "  let status_str = \n\
                    \    Enumeration_process_status.to_string %s in\n" polyvar;
        raw out "  let umm = PGSQL(dbh)\n";
        raw out "    \"SELECT g_id FROM %s WHERE g_status = $status_str\" in\n"
          name;
        raw out "  pg_map umm (fun l -> list_map l (fun id -> { id }))\n\n";
      );
      
      doc out "Get all the Function evaluations whose status is [%s]." polyvar;
      line out_mli "val get_all_%s: dbh:db_handle ->" suffix;
      line out_ml "let get_all_%s %s:" suffix pgocaml_db_handle_arg;
      ocaml_poly_flow out (sprintf "%s pointer list" phamtom) ["`pg_exn of exn"];
      line out_ml " =";
      pgocaml_to_flow out_ml (fun out ->
        line out "get_all_%s_exn ~dbh" suffix
      );
      
    );
  
  hide out_ml (fun out ->
    line out "let get_all_exn %s: \
          [ `can_nothing ] pointer list PGOCaml.monad = " pgocaml_db_handle_arg;
    pgocaml_do_get_all_ids ~out name;
  );
  doc out "Get all the [%s] functions." name;
  line out_mli "val get_all: dbh:db_handle ->";
  line out_ml "let get_all %s:" pgocaml_db_handle_arg;
  ocaml_poly_flow out "[`can_nothing] pointer list" [ "`pg_exn of exn"];
  line out_ml " =";
  pgocaml_to_flow out_ml (fun out ->
    line out "get_all_exn ~dbh" 
  );

  doc out "{3 Low Level Access}";
  (* Delete a function *)
  deprecate out "Deletes a function using its internal identifier.";
  line out_mli "val _delete_evaluation_by_id: id:int32 -> dbh:db_handle ->\
                  unit PGOCaml.monad";
  raw out_ml "let _delete_evaluation_by_id ~id %s =\n" pgocaml_db_handle_arg;
  raw out_ml "  PGSQL (dbh)\n";
  raw out_ml "    \"DELETE FROM %s WHERE g_id = $id\"\n\n" name;

  let wrong_insert_cache = 
    OCaml_hiden_exception.make ~in_loads:true
      (sprintf "Function_%s" name) 
      "insert_value_did_not_return_one_id"
      [ "string"; "int32 list" ] in
  let on_not_one_id ~out ~table_name  ~returned  =
    OCaml_hiden_exception.throw wrong_insert_cache out
      (sprintf "(%S, %s)" table_name returned) in
  OCaml_hiden_exception.define wrong_insert_cache out_ml;

  hide out_ml (fun out ->
    line out "let insert_evaluation_exn %s (t: 'a t): \
      [`can_nothing] pointer PGOCaml.monad = " pgocaml_db_handle_arg;

    List.iter (function_standard_fields result @ args) ~f:(fun (s, t) ->
      line out "  let %s = t.%s in" s s;
      line out "  %s" (let_in_typed_value (s,t))
    );
    let all_field_names = 
      List.map (function_standard_fields result @ args) fst in
    let all_field_names_prefixed =
      let prefix (n, t) =
        match type_is_option t with 
          `yes -> sprintf "$?%s" n | `no -> sprintf "$%s" n in 
      List.map (function_standard_fields result @ args) prefix in
    pgocaml_insert_in_db_with_id_exn
      ~out ~on_not_one_id name all_field_names all_field_names_prefixed;

  );

  doc out "Load an evaluation into the database ({b Unsafe!}).";
  line out_mli "val insert_evaluation: dbh:db_handle -> 'any t ->";
  line out_ml "let insert_evaluation %s (t: 'a t):"
    pgocaml_db_handle_arg;
  ocaml_poly_flow out "[`can_nothing] pointer" [
    (OCaml_hiden_exception.poly_type_local [wrong_insert_cache]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_flow out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_insert_cache;
  ] (fun out ->
    line out "insert_evaluation_exn ~dbh t";
  );

  raw out "end (* %s *)\n\n" name;
  ()


let ocaml_toplevel_values_and_types ~out ~fashion dsl =

  let out_ml = 
    match fashion with
    | `implementation -> out
    | `interface | `types -> ignore in
  let out_mli = 
    match fashion with
    | `implementation | `types -> ignore
    | `interface -> out in

  doc out "{3 Top-level Queries On The Data-base }";
  doc out "";

  let rec_name = function Record (n, _) -> Some n | _ -> None in
  let fun_name = function Function (n, _, _) -> Some n | _ -> None in
  List.iter [
     (* ("records"    , "record_"       , None, rec_name); *)
     ("value"     , "value_"        , Some "Record", rec_name);
     (* ("functions"  , "function_"     , None, fun_name); *)
    ("'a evaluation", "evaluation_", Some "'a Function", fun_name);
    ]
    (fun (type_name, prefix, modprefix, get_name) ->
      raw out "type %s = [\n" type_name;
      List.iter dsl.nodes (fun e ->
        get_name e |> Option.iter ~f:(fun name ->
          match modprefix with
          | Some s -> raw out " | `%s%s of %s_%s.pointer\n" prefix name s name
          | None -> raw out " | `%s%s\n" prefix name)
      );
      raw out "]\n";
    );

  let tmprec, print_tmprec = new_tmp_output () in
  let tmpfun, print_tmpfun = new_tmp_output () in
  doc tmprec "Get all record values";
  doc tmpfun "Get all function evaluations";
  raw tmprec "let get_all_values_exn %s: value list PGOCaml.monad =\n\
       \  let thread_fun_list = [\n" pgocaml_db_handle_arg;
  raw tmpfun "let get_all_evaluations_exn %s:
                [ `can_nothing ] evaluation list PGOCaml.monad =\n\
              \  let thread_fun_list = [\n" pgocaml_db_handle_arg;
  let apply_get_tag get tag =
    sprintf "(fun () -> pg_map (%s dbh) (fun l -> list_map l %s));\n" get tag in
  List.iter dsl.nodes (function
    | Record (n, fields) ->
      let get, tag = 
        sprintf "Record_%s.get_all_exn" n, sprintf "(fun x -> `value_%s x)" n in
      raw tmprec "%s" (apply_get_tag get tag)
    | Function (n, args, ret) ->
      let get, tag = 
        sprintf "Function_%s.get_all_exn" n, 
        sprintf "(fun x -> `evaluation_%s x)" n in
      raw tmpfun "%s" (apply_get_tag get tag);
    | _ -> ());
  let the_rest =
    "  ] in\n\
    \  let v_list_list_monad = \
         map_s ~f:(fun f -> f ()) thread_fun_list in\n\
    \  pg_map v_list_list_monad list_flatten\n\n"
  in
  raw tmprec "%s" the_rest;
  raw tmpfun "%s" the_rest;

  hide out_ml (fun out ->
    print_tmprec out;
    print_tmpfun out;
  );

  doc out "Get all the values in the database.";
  line out_mli "val get_all_values: dbh:db_handle ->";
  line out_ml "let get_all_values %s:" pgocaml_db_handle_arg;
  ocaml_poly_flow out "value list" ["`pg_exn of exn";];
  line out_ml " =";
  pgocaml_to_flow out_ml 
    ~transform_exceptions:
    (OCaml_hiden_exception.(List.map ~f:transform_global (all_gets ())))
    (fun out ->
      line out "get_all_values_exn ~dbh";
    );

  doc out "Get all the evaluations in the database.";
  line out_mli "val get_all_evaluations: dbh:db_handle ->";
  line out_ml "let get_all_evaluations %s:" pgocaml_db_handle_arg;
  ocaml_poly_flow out "[`can_nothing] evaluation list" ["`pg_exn of exn";];
  line out_ml " =";
  pgocaml_to_flow out_ml 
    ~transform_exceptions:
    (OCaml_hiden_exception.(List.map ~f:transform_global (all_gets ())))
    (fun out ->
      line out "get_all_evaluations_exn ~dbh";
    );

  ()

    
let ocaml_file_system_module ~fashion ~out dsl =

  let out_ml = 
    match fashion with
    | `implementation -> out | `interface | `types -> ignore in
  let out_mli = 
    match fashion with
    | `implementation | `types -> ignore | `interface -> out in

  let id_field = "g_id", Int in
  let volume_fields = [
    "g_kind",  String;
    "g_sexp",  String;
  ] in

  ocaml_start_module ~out ~name:"File_system" fashion;

  doc out "The [pointer] represents a handle to a volume in the DB.";
  ocaml_sexped_type ~out ~name:"pointer" ~fashion
    ~privated:true "{ id : int32 }";

  doc out "Unsafely create a volume.";
  line out_mli "val unsafe_cast: int32 -> pointer";
  line out_ml "let unsafe_cast id = { id }";

  doc out "The specification of file-trees.";
  ocaml_sexped_type ~out ~name:"tree" ~fashion
    (String.concat ~sep:"\n" [
      "  | File of string * Enumeration_file_type.t";
      "  | Directory of string * Enumeration_file_type.t * tree list";
      "  | Opaque of string * Enumeration_file_type.t";]);

  doc out "The volume content is …";
  ocaml_sexped_type ~out ~name:"volume_content" ~fashion
    " | Link of pointer
      | Tree of string option * tree list ";
  
  doc out "A volume is then …";
  ocaml_sexped_type ~out ~name:"volume" ~fashion
    "{ volume_pointer: pointer; volume_kind: Enumeration_volume_kind.t;
       volume_content: volume_content; }";

  let wrong_insert = 
    OCaml_hiden_exception.make "File_system" "add_did_not_return_one"
      [ "string"; "int32 list" ] in
  let on_not_one_id ~out ~table_name  ~returned  =
    OCaml_hiden_exception.throw wrong_insert out
      (sprintf "(%S, %s)" table_name returned) in

  OCaml_hiden_exception.define ~fashion wrong_insert out;

  doc out "Get the toplevel directory corresponding to a volume-kind.";
  line out_mli "val toplevel_of_kind: Enumeration_volume_kind.t -> string";
  line out_ml " let toplevel_of_kind = function";
  List.iter dsl.nodes (function
  | Volume (n, t) -> line out_ml "    | `%s -> %S" n t
  | _ -> ());
  line out_ml "";
  
  doc out "Get the volume-kind corresponding to a toplevel directory name.";
  line out_mli "val kind_of_toplevel:
       string -> (Enumeration_volume_kind.t,
                  [> `toplevel_not_found of string]) Core.Std.Result.t";
  line out_ml " let kind_of_toplevel = function";
  List.iter dsl.nodes (function
  | Volume (n, t) -> line out_ml "    | %S -> Core.Std.Ok `%s" t n
  | _ -> ());
  line out_ml "    | s -> Core.Std.Error (`toplevel_not_found s)";
  
  hide out_ml (fun out ->
    line out "let add_any_volume_exn ~dbh ~kind ~content =";
    line out "let sexp = sexp_of_volume_content content in";
    line out "let str_sexp = Sexplib.Sexp.to_string_hum sexp in";
    line out "let str_kind = Enumeration_volume_kind.to_string kind in";
    pgocaml_add_to_database_exn "g_volume" 
      (List.map volume_fields fst) [ "$str_kind"; "$str_sexp" ]
      ~out ~on_not_one_id ~id:"id";
  );
  
  doc out "Register a new file, directory (with its contents),
           or opaque-directory in the DB (i.e. a “classical” volume).";
  line out_ml "let add_volume %s \
                ~(kind:Enumeration_volume_kind.t) \
                ?(hr_tag: string option) \
                ~(files:tree list) : " 
    pgocaml_db_handle_arg;
  line out_mli "val add_volume: dbh: db_handle -> \
                 kind:Enumeration_volume_kind.t -> \
                ?hr_tag: string -> \
                files:tree list -> ";
  ocaml_poly_flow out "pointer" [
    (OCaml_hiden_exception.poly_type_local [wrong_insert]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_flow out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_insert;
  ] (fun out ->
    line out "add_any_volume_exn ~dbh ~kind ~content:(Tree (hr_tag, files))"
  );

  doc out "Register a “link” volume to another volume.";
  line out_ml "let add_link ~dbh ~(kind:Enumeration_volume_kind.t) pointer:"; 
  line out_mli "val add_link: dbh:db_handle -> \
                 kind:Enumeration_volume_kind.t -> pointer ->";
  ocaml_poly_flow out "pointer" [
    (OCaml_hiden_exception.poly_type_local [wrong_insert]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_flow out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_insert;
  ] (fun out ->
    line out "add_any_volume_exn ~dbh ~kind ~content:(Link pointer)"
  );


  

  doc out "Module for constructing file trees less painfully.";
  line out_ml "module Tree = struct";
  line out_ml "let file ?(t=`blob) n = File (n, t)";
  line out_ml "let dir ?(t=`directory) n l = Directory (n, t, l)";
  line out_ml "let opaque ?(t=`opaque) n = Opaque (n, t)";
  line out_ml "end";
  line out_mli "module Tree: sig
val file: ?t:Enumeration_file_type.t -> string -> tree
val dir : ?t:Enumeration_file_type.t -> string -> tree list -> tree
val opaque : ?t:Enumeration_file_type.t -> string -> tree
end
";

  hide out_ml (fun out ->
    line out "let get_all_exn %s: \
            pointer list PGOCaml.monad =" pgocaml_db_handle_arg;
    pgocaml_do_get_all_ids ~out ~id:"id" "g_volume";
  );
  
  doc out "Get all the volumes.";
  line out_ml "let get_all %s: " pgocaml_db_handle_arg;
  line out_mli "val get_all: dbh:db_handle ->";
  ocaml_poly_flow out "pointer list" ["`pg_exn of exn";];
  line out_ml " =";
  pgocaml_to_flow out_ml (fun out ->
    pgocaml_do_get_all_ids ~out ~id:"id" "g_volume";
  );

  let wrong_cache_select = 
    OCaml_hiden_exception.make ~in_dumps:true "File_system" 
      "select_did_not_return_one_tuple"
      [ "string"; "int" ] in
  let on_not_one_id ~out ~table_name  ~returned  =
    OCaml_hiden_exception.throw wrong_cache_select out
      (sprintf "(%S, List.length %s)" table_name returned) in
  OCaml_hiden_exception.define wrong_cache_select out_ml;

  hide out_ml (fun out ->     
    raw out "let get_volume_exn (volume_pointer: pointer) %s: \
                  volume PGOCaml.monad =\n" pgocaml_db_handle_arg;
    line out "let tuple_m =";
    pgocaml_select_from_one_by_id_exn 
      ~out ~on_not_one_id ~get_id:"volume_pointer.id" "g_volume";
    line out "in";
    line out "pg_bind tuple_m (fun (%s,%s) -> " (fst id_field)
      (List.map volume_fields ~f:fst |! String.concat ~sep:", ");
    line out "    pg_return {
        volume_pointer;
        volume_kind = Enumeration_volume_kind.of_string_exn g_kind;
        volume_content = volume_content_of_sexp (Sexplib.Sexp.of_string g_sexp);}) ";
  );

  doc out "Retrieve the \"entry\" part of a volume from the DB.";
  line out_ml "let get_volume (pointer: pointer) %s:" 
    pgocaml_db_handle_arg;
  line out_mli "val get_volume: pointer -> dbh:db_handle ->";
  ocaml_poly_flow out "volume" [
    (OCaml_hiden_exception.poly_type_local [wrong_cache_select]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_flow out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_cache_select;
  ] (fun out ->
    line out "get_volume_exn ~dbh pointer";
  );

  doc out "{3 Low-level Access}";

  let wrong_cache_insert = 
    OCaml_hiden_exception.make ~in_loads:true "File_system" 
      "insert_tuple_did_not_return_one_id"
      [ "string"; "int32 list" ] in
  let on_not_one_id ~out ~table_name  ~returned  =
    OCaml_hiden_exception.throw wrong_cache_insert out
      (sprintf "(%S, %s)" table_name returned) in
  OCaml_hiden_exception.define wrong_cache_insert out_ml;

  hide out_ml (fun out ->
    line out "let insert_volume_exn %s (volume: volume): \
            pointer PGOCaml.monad = " pgocaml_db_handle_arg;
    line out "let g_id = volume.volume_pointer.id in";
    line out "let g_kind = Enumeration_volume_kind.to_string volume.volume_kind in";
    line out "let g_sexp =
       Sexplib.Sexp.to_string_hum
          (sexp_of_volume_content volume.volume_content) in";
    pgocaml_insert_in_db_with_id_exn
      ~out ~on_not_one_id "g_volume"
      ["g_id"; "g_kind"; "g_sexp"] ["$g_id"; "$g_kind"; "$g_sexp"];
  );
  doc out "Load a volume in the database ({b Unsafe!}) .";
  line out_ml "let insert_volume %s (volume: volume):" pgocaml_db_handle_arg;
  line out_mli "val insert_volume: dbh:db_handle -> volume ->";
  ocaml_poly_flow out "pointer" [
    (OCaml_hiden_exception.poly_type_local [wrong_cache_insert]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_flow out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_cache_insert;
  ] (fun out ->
    line out "insert_volume_exn ~dbh volume"
  );
  

  hide out_ml (fun out ->
    line out "let delete_volume_exn %s id =" pgocaml_db_handle_arg;
    line out "    PGSQL(dbh)";
    line out "      %S" "DELETE FROM g_volume WHERE g_id = $id";
  );
  doc out "Delete a volume with its contents ({b UNSAFE:} does \
            not verify if there is a link to it).";
  line out_ml "let delete_volume %s id =" pgocaml_db_handle_arg;
  line out_mli "val delete_volume: dbh:db_handle -> int32 ->";
  ocaml_poly_flow out_mli "unit" [
    "`pg_exn of exn";
  ];
  pgocaml_to_flow out_ml ~transform_exceptions:[] (fun out ->
    line out "  delete_volume_exn ~dbh id";
  );
  
  line out "end (* File_system *)";
  ()


let ocaml_dump_and_reload ~out ~fashion dsl =
  let out_ml = 
    match fashion with
    | `implementation -> out
    | `interface | `types -> ignore in
  let out_mli = 
    match fashion with
    | `implementation | `types -> ignore
    | `interface -> out in
  
  let tmp_type_buffer = Buffer.create 42 in
  let tmp_type = Buffer.add_string tmp_type_buffer in
  let tmp_get_fun, print_tmp_get_fun = new_tmp_output () in
  let tmp_get_fun2, print_tmp_get_fun2 = new_tmp_output () in

  let tmp_ins_fun, print_tmp_ins_fun = new_tmp_output () in

  line tmp_type "{";
  line tmp_type "  version: string;";
  line tmp_type "  file_system: File_system.volume list;";
 
  let close_get_fun = ref [] in
  line tmp_get_fun2 "pg_return { version = Hitscore_conf_values.version;";

  line tmp_get_fun "pg_bind (File_system.get_all_exn ~dbh) (fun t_list ->";
  line tmp_get_fun "pg_bind (map_s ~f:(File_system.get_volume_exn ~dbh) \
                                    t_list) (fun file_system ->";
  line tmp_get_fun2 "     file_system;";
  close_get_fun := "))" :: !close_get_fun;

  let close_ins_fun = ref [] in
  line tmp_ins_fun "    let fs_m = \
                         map_s ~f:(File_system.insert_volume_exn ~dbh) \
                         dump.file_system in";
  line tmp_ins_fun "    pg_bind fs_m (fun _ ->";
  close_ins_fun := ")" :: !close_ins_fun;


  List.iter dsl.nodes (function
    | Enumeration (name, fields) -> ()
    | Record (name, fields) ->
      line tmp_type "  record_%s: Record_%s.t list;" name name;
      line tmp_get_fun "pg_bind (Record_%s.get_all_exn ~dbh) (fun p_list ->" name;
      line tmp_get_fun "pg_bind (map_s ~f:(Record_%s.get_value_exn ~dbh) \
                                    p_list) (fun record_%s ->" name name;
      line tmp_get_fun2 "     record_%s;" name;
      close_get_fun := "))" :: !close_get_fun;

      line tmp_ins_fun "     let r_m = map_s ~f:(Record_%s.insert_value_exn \
                              ~dbh) dump.record_%s in\n\
                       \     pg_bind r_m (fun _ ->" name name;
      close_ins_fun := ")" :: !close_ins_fun;

    | Function (name, args, result) ->
      line tmp_type "  function_%s: [ `can_nothing ] Function_%s.t list;" 
        name name;
      line tmp_get_fun "pg_bind (Function_%s.get_all_exn ~dbh) (fun p_list ->" name;
      line tmp_get_fun "pg_bind (map_s ~f:(Function_%s.get_evaluation_exn \
                                    ~dbh) p_list) (fun function_%s ->" name name;
      line tmp_get_fun2 "     function_%s;" name;
      close_get_fun := "))" :: !close_get_fun;

      line tmp_ins_fun "     let f_m = map_s ~f:(Function_%s.insert_evaluation_exn \
                              ~dbh) dump.function_%s in\n\
                       \     pg_bind f_m (fun _ ->" name name;
      close_ins_fun := ")" :: !close_ins_fun;

    | Volume (_, _) -> ()
  );
  line tmp_type "}";
  line tmp_get_fun2 "}";

  doc tmp_type "An OCaml record containing the whole data-base.";
  ocaml_sexped_type ~out ~fashion ~name:"dump" (Buffer.contents tmp_type_buffer);

  doc  out "Retrieve the whole data-base.";
  line out_mli "val get_dump: dbh:db_handle ->";
  line out_ml "let get_dump %s :" pgocaml_db_handle_arg;
  ocaml_poly_flow out "dump" [
    (OCaml_hiden_exception.(poly_type_global (all_dumps ())));
    "`pg_exn of exn";
  ];
  line out_ml " =";
  
  pgocaml_to_flow out_ml
    ~transform_exceptions:
    (OCaml_hiden_exception.(List.map ~f:transform_global (all_dumps ())))
    (fun out ->
      print_tmp_get_fun out;
      print_tmp_get_fun2 out;
      line out "%s" (String.concat ~sep:"" !close_get_fun);
    );
  

  doc out "Insert the contents of a [dump] in the data-base
                  ({b Unsafe!}).";
  line out_mli "val insert_dump: dbh:db_handle -> dump ->";
  line out_ml "let insert_dump %s dump :" pgocaml_db_handle_arg;
    ocaml_poly_flow out "unit" [
    "`wrong_version of string * string";
    (OCaml_hiden_exception.(poly_type_global (all_loads ())));
    "`pg_exn of exn";
  ];
  line out_ml " =";

  line out_ml "  if dump.version <> Hitscore_conf_values.version then (";
  line out_ml "    Flow.error (`wrong_version (dump.version, \
                          Hitscore_conf_values.version))";
  line out_ml "  ) else";
  pgocaml_to_flow out_ml 
    ~transform_exceptions:
    (OCaml_hiden_exception.(List.map ~f:transform_global (all_loads ())))
    (fun out -> 
      print_tmp_ins_fun out;
      line out "pg_return ()";
      line out "%s" (String.concat ~sep:"" !close_ins_fun);
    );
  ()

let ocaml_search_module ~out ~fashion dsl =

  let out_ml = 
    match fashion with
    | `implementation -> out
    | `interface -> ignore in
  let out_mli = 
    match fashion with
    | `implementation -> ignore
    | `interface -> out in

  doc out "{3 Custom Queries On the Layout}";

  ocaml_start_module ~out ~name:"Search" fashion;

  List.iter dsl.actions (function
    | Search_by_and (record, fields) ->
      let record_type, kindstr, all_typed_fields  =
        List.find_map dsl.nodes (function
          | Record (n, fs) when n = record -> 
            Some (Record_name n, "record", fs)
          | Function (n, tv, r) when n = record -> 
            Some (Function_name n, "function", tv)
          | _ -> None) |> Option.value_exn
      in
      let typed_fields =
        List.map fields (fun s ->
          List.find_exn all_typed_fields ~f:(fun (n, _) -> n = s))
      in
      let fun_name = 
        sprintf "%s_%s_by_%s" kindstr record (String.concat ~sep:"_" fields) in
      hide out_ml (fun out ->
        line out "let %s_exn %s %s ="
          fun_name pgocaml_db_handle_arg
          (String.concat ~sep:" " (List.map fields (sprintf "%s")));
        List.iter typed_fields (fun tv -> let_in_typed_value tv |> raw out "%s");
        line out " let search = ";
        line out "PGSQL(dbh)";
        let prefix (n, t) =
          (match type_is_option t with `yes -> "$?" | `no -> "$") ^ n in
        line out "\"SELECT g_id FROM %s WHERE %s\"" record
          (String.concat ~sep:" AND "
             (List.map typed_fields (fun (n, t) -> 
               sprintf "%s is not distinct from %s" n (prefix (n,t)))));
        line out "  in";
        line out "  pg_bind search (fun l -> \
                pg_return (list_map l (fun %s -> %s)))" record
          (convert_pgocaml_type (record, record_type));
      );
      let plural_s, sing_es = 
        if List.length fields > 1 then "s", "" else "", "es" in
      doc out "Search all the %ss [%s] for which the field%s %s match%s \
              the argument%s."
        kindstr record plural_s 
        (String.concat ~sep:", " fields) sing_es plural_s;
      line out_ml "let %s %s %s ="
        fun_name pgocaml_db_handle_arg
        (String.concat ~sep:" " (List.map fields (sprintf "%s")));
      line out_mli "val %s: dbh:db_handle ->" fun_name;
      List.iter typed_fields (fun (n, t) ->
        line out_mli "    %s ->" (ocaml_type t));
      line out_mli " (%s list, [> `pg_exn of exn]) Flow.monad"
        (ocaml_type record_type);
      pgocaml_to_flow out_ml ~transform_exceptions:[] (fun out ->
        line out "  %s_exn ~dbh %s"
          fun_name (String.concat ~sep:" " (List.map fields (sprintf "%s")));
      );

  );
  line out "end (* module Search *)";
  ()



let toplevel_generic_types ~fashion out =
  begin match fashion with
  | `interface | `implementation ->
    doc out "PG'OCaml's connection handle.";
    raw out "type db_handle = (string, bool) Hashtbl.t PGOCaml.t\n\n";
    line out "type function_capabilities = Types.function_capabilities"
  | `types ->
    doc out "Access rights on [Function_*.{t,cache}].";
    raw out "type function_capabilities= [\n\
                \  | `can_nothing\n  | `can_start\n  | `can_complete\n\
                | `can_get_result\n]\n";
  end;
  ()
    
let filesystem_kinds_and_types dsl =
  let volume_kinds = 
    (List.filter_map dsl.nodes 
       (function | Volume (n, _) -> Some n | _ -> None)) in
  let file_types =  ["blob"; "directory"; "opaque"] in
  (volume_kinds, file_types)

let file_system_section ~(fashion: fashion) out dsl = 

  let (volume_kinds, file_types) = filesystem_kinds_and_types dsl in


  doc out "\n {3 Virtual File System in The Data-base }\n\n ";
  doc out "Volume `kinds' are defined by the DSL.";
  ocaml_enumeration_module ~out ~fashion "volume_kind" volume_kinds;
  doc out "File-types are, for now, [`blob], [`directory], and [`opaque].";
  ocaml_enumeration_module ~out ~fashion "file_type" file_types;
  
  ocaml_file_system_module ~fashion ~out dsl;

  ()

let functions_and_records ~fashion ~out dsl =
  doc out "\n {3 Records and Functions of The Layout}\n\n ";
  List.iter dsl.nodes (function
    | Enumeration (name, fields) ->
      doc out "";
      ocaml_enumeration_module ~out ~fashion name fields
    | Record (name, fields) ->
      ocaml_record_module ~out ~fashion name fields
    | Function (name, args, result) ->
      ocaml_function_module ~out ~fashion name args result
    | Volume (_, _) -> ()
  )

let ocaml_classy_module ~out ~fashion dsl =
  let out_ml = 
    match fashion with
    | `implementation -> out
    | `interface -> ignore in
  let out_mli = 
    match fashion with
    | `implementation -> ignore
    | `interface -> out in

  let new_method name typ body =
    line out_mli "method %s : %s" name typ;
    line out_ml "method %s = %s" name body
  in

  doc out "Classy Access to the Layout";
  (* ocaml_start_module ~out ~name:"Classy" fashion; *)
  line out_ml "open Core.Std";

  line out_mli "class ['a] collection: 'a list -> object";
  line out_ml "class ['a] collection (l : 'a list) = object (self)";
  line out "constraint 'a = < g_id : int32; .. >";
  new_method "find" "('a -> bool) -> 'a option" "fun f -> List.find l ~f";
  new_method "find_all" "('a -> bool) -> 'a list" "fun f -> List.filter l ~f";
  new_method "get32_exn" "int32 -> 'a"
    "fun i32 -> List.find_exn l ~f:(fun x -> x#g_id = i32)";
  new_method "all"  "'a list" "l";
  line out "end";

  line out_mli "class ['a] pointer: int32 -> 'a collection -> object";
  line out_ml "class ['a] pointer id (col : 'a collection) = object (self)";
  line out "constraint 'a = < g_id : int32; .. >";
  new_method "id" "int32" "id";
  new_method "get_exn" "'a" "col#get32_exn id";
  line out "end";

    line out_mli "class volume: File_system.volume -> layout -> object";
    line out_ml "class volume fsvolume (layout: layout) = object(self)";
    line out_ml "val volume_pointer = fsvolume.File_system.volume_pointer";
    line out_ml "val volume_kind    = fsvolume.File_system.volume_kind   ";
    line out_ml "val volume_content = fsvolume.File_system.volume_content";
    new_method "g_id" "int32" "volume_pointer.File_system.id";
    new_method "volume_pointer" "File_system.pointer" "volume_pointer";
    new_method "volume_kind" "Enumeration_volume_kind.t" "volume_kind";
    new_method "volume_content" "File_system.volume_content" "volume_content";
    line out "end and";

  List.iter dsl.nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    line out_mli " %s: Record_%s.t -> layout -> object" name name;
    line out_ml " %s t (layout: layout) = object (self)" name;
    List.iter (record_standard_fields @ fields) ~f:(fun (s, t) ->
      let rec parse_type = function
        | Identifier | Bool | Timestamp | Int | Real             
        | Enumeration_name _ | String      ->
          line out_ml "val %s: %s = t.Record_%s.%s" s (ocaml_type t) name s;
          new_method s (ocaml_type t) s;
        | Function_name s -> failwith "funcpointer"
        | Record_name r ->
          let typ = sprintf "%s pointer" r in
          line out_ml "val %s: %s =" s typ;
          line out_ml "   new pointer t.Record_%s.%s.Record_%s.id layout#%s"
            name s r r;
          new_method s typ s;
        | Option (Record_name r) ->
          let typ = sprintf "%s pointer option" r in
          line out_ml "val %s: %s =" s typ;
          line out_ml "   Option.map t.Record_%s.%s (fun p -> \
                              new pointer p.Record_%s.id layout#%s)"
            name s r r;
          new_method s typ s;
        | Array (Record_name r) ->
          let typ = sprintf "%s pointer list" r in
          line out_ml "val %s: %s =" s typ;
          line out_ml "   Array.to_list (Array.map t.Record_%s.%s ~f:(fun p -> \
                              new pointer p.Record_%s.id layout#%s))"
            name s r r;
          new_method s typ s;
        | Volume_name v ->
          let typ = sprintf "volume pointer" in
          line out_ml "val %s: %s =" s typ;
          line out_ml "   new pointer t.Record_%s.%s.File_system.id layout#file_system"
            name s;
          new_method s typ s;
        | Option (Volume_name v) ->
          let typ = sprintf "volume pointer option" in
          line out_ml "val %s: %s =" s typ;
          line out_ml "   Option.map t.Record_%s.%s (fun p -> \
                              new pointer p.File_system.id layout#file_system)"
            name s;
          new_method s typ s;
        | Option t -> (parse_type  t)
        | Array t  -> (parse_type  t)
      in
      parse_type t;
    );
    line out "end and";

  | Function (name, args, result) ->
    line out_mli " %s: [`can_nothing] Function_%s.t -> layout -> object" name name;
    line out_ml " %s t (layout: layout) = object (self)" name;
    List.iter (function_standard_fields result @ args) ~f:(fun (s, t) ->
      line out_ml "val %s: %s = t.Function_%s.%s" s (ocaml_type t) name s;
      new_method s (ocaml_type t) s;
    );
    line out "end and";

  | Volume (name, toplevel) -> ()
  );

  let delayed_collection_method name dump class_name =
    line out_ml "val mutable %s = None" name;
    line out_ml "method private _%s = \
                new collection (List.map dump.%s (fun t -> new %s t (self :> layout)))"
      name dump class_name;
    new_method name (sprintf "%s collection" class_name)
      (sprintf "match %s with Some s -> s
               | None -> let s = self#_%s in %s <- Some s; s"
         name name name)
  in
    
  line out_ml " layout dump = object (self)";
  line out_mli " layout:  dump -> object";
  delayed_collection_method "file_system" "file_system" "volume";
  List.iter dsl.nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    delayed_collection_method name ("record_" ^ name) name;
  | Function (name, args, result) ->
    delayed_collection_method name ("function_" ^ name) name;
  | Volume (name, toplevel) -> ()
  );
  line out "end";
  
  (* line out "end (\* Classy *\)"; *)
  ()

  
let ocaml_meta_module ~out ~fashion ?raw_dsl dsl =

  let out_ml = 
    match fashion with
    | `implementation -> out
    | `interface -> ignore in
  let out_mli = 
    match fashion with
    | `implementation -> ignore
    | `interface -> out in

  doc out "{3 Meta-Information On the Layout}";

  doc out "Meta-Information On the Layout";
  ocaml_start_module ~out ~name:"Meta" fashion;

  doc out "The source used to generate all {i this}.";
  line out_mli "val source_file : string";
  line out_ml "let source_file = %S" (Option.value ~default:"" raw_dsl);

  doc out "Get the Layout (will be parsed only once).";
  line out_mli "val layout: unit -> Hitscoregen_layout_dsl.dsl_runtime_description";
  line out_ml "let layout = \n\
                  let l = ref None in \n\
                  fun () ->\n\
                  match !l with\n| Some s -> s \n\
                  | None ->\n\
                    let lp = Hitscoregen_layout_dsl.parse_str source_file in\n\
                    l := Some lp; lp";

  line out "end"

let ocaml_interface dsl output_string =

  let out = output_string in
  
  let (volume_kinds, file_types) = filesystem_kinds_and_types dsl in
  let all_nodes =
    Enumeration ("volume_kind", volume_kinds)
    :: Enumeration ("file_type", file_types)
    :: dsl.nodes in

  let fashion = `types in
  doc out "Autogenerated module.";
  line out "module Types: sig " ;
  line out "  module Timestamp: module type of Core.Std.Time";
  toplevel_generic_types ~fashion out;
  doc out "\n {3 Types for Records and Functions of The Layout}\n\n ";
  List.iter all_nodes (function
  | Enumeration (name, fields) ->
    doc out "The type of {i %s} items." name;
    raw
      (ocaml_sexped_type ~out ~name ~fashion:`interface)
      "[%s]"
      (List.map fields (sprintf "`%s") |> String.concat ~sep:" | ");
  | Record (name, fields) -> ()
  | Function (name, args, result) -> ()
  | Volume (_, _) -> ()
  );
  line out "end = struct";
  line out "module Timestamp = struct 
      include Core.Std.Time
      let () = write_new_string_and_sexp_formats__read_both ()
    end";
  toplevel_generic_types ~fashion out;
  doc out "\n {3 Types for Records and Functions of The Layout}\n\n ";
  List.iter all_nodes (function
  | Enumeration (name, fields) ->
    doc out "The type of {i %s} items." name;
    raw
      (ocaml_sexped_type ~out ~name ~fashion)
      "[%s]"
      (List.map fields (sprintf "`%s") |> String.concat ~sep:" | ");
  | Record (name, fields) -> ()
  | Function (name, args, result) -> ()
  | Volume (_, _) -> ()
  );
  line out "end";
  
  let fashion = `interface in
  doc out "Autogenerated module.";
  line out "module type LAYOUT = sig";
  line out "open Types";
  line out "module Flow : Sequme_flow_monad.FLOW_MONAD";
  line out "module PGOCaml: PGOCaml_generic.PGOCAML_GENERIC ";
  toplevel_generic_types ~fashion out;
  file_system_section ~fashion out dsl;
  functions_and_records ~fashion dsl ~out;
  ocaml_search_module ~out ~fashion dsl;
  ocaml_toplevel_values_and_types ~out ~fashion dsl;
  ocaml_dump_and_reload ~out ~fashion dsl;
  ocaml_meta_module ~out ~fashion dsl;
  line out "end";

  line out "module type CLASSY = sig";
  line out "module Layout: LAYOUT";
  line out "open Layout";
  line out "open Types";
  ocaml_classy_module ~out ~fashion dsl;
  line out "end";
  
  ()

let ocaml_code ?(with_interface=true) raw_dsl dsl output_string =

  
  let out = output_string in

  line out "module Test = struct";
  ocaml_module raw_dsl dsl output_string;
  line out "end";

  
  doc out "Autogenerated module.";
  doc out "To make the DB-accesses thread-agnostic we need an \
        \"Outside word model\".";
  raw out "\
open Hitscore_layout_interface
module Make \
(Flow : Sequme_flow_monad.FLOW_MONAD)";
  if with_interface then (
    line out ": LAYOUT\n";
    line out "  with module Flow = Flow\n";
    line out "  with type 'a PGOCaml.monad = 'a Flow.IO.t";
  );
  raw out "\
 = struct\n\
  module PGOCaml = PGOCaml_generic.Make(Flow.IO)
  module Flow = Flow
  open Types
";

  let fashion = `implementation in
  toplevel_generic_types ~fashion out;
  hide out (fun out ->
    raw out "\
open Sexplib.Conv\n\
(* Legacy stuff: *)\n\
let err = Flow.IO.log_error\n\n\
let map_s = Flow.IO.map_sequential\n\
let catch_pg_exn f x =\n\
 Flow.(bind_on_error (catch_io f x)\n\
  (fun e -> error (`pg_exn e)))\n\n\
let option_map o f =\n\
    match o with None -> None | Some s -> Some (f s)\n\n\
let option_value_map = Core.Std.Option.value_map \n\n\
let array_map a f = Core.Std.Array.map ~f a\n\n\
let list_map l f = Core.Std.List.map ~f l\n\n\
let list_iter l f = Core.Std.List.iter ~f l\n\n\
let list_find l f = Core.Std.List.find ~f l\n\n\
let list_append l = Core.Std.List.append l\n\n\
let list_flatten (l : 'a list list) : 'a list = Core.Std.List.concat l\n\n\
let fold_left = Core.Std.List.fold_left\n\n\
let array_to_list a = Core.Std.Array.to_list a\n\n\
let list_to_array l = Core.Std.Array.of_list l\n\n\
let pg_bind am f = PGOCaml.bind am f\n\n\
let pg_return t = PGOCaml.return t\n\n\
let pg_map am f = pg_bind am (fun x -> pg_return (f x))\n\n\

";
  );
  file_system_section ~fashion out dsl; 

  functions_and_records ~fashion ~out dsl;

  ocaml_search_module ~out ~fashion dsl;
  ocaml_toplevel_values_and_types ~out ~fashion dsl;
  ocaml_dump_and_reload ~out ~fashion dsl;
  ocaml_meta_module ~out ~fashion ~raw_dsl dsl;

  line out "end (* Make *)";

  line out "module Make_classy (Layout : LAYOUT) = struct";
  line out "let list_map l f = Core.Std.List.map ~f l";
  line out "module Layout = Layout";
  line out "open Layout";
  line out "open Types";
  ocaml_classy_module ~out ~fashion dsl;
  line out "end (* Make_classy *)";
  
  
  ()

