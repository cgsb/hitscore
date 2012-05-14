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

  line out "let pointer t = {id = t.g_id}";

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
  line out "module %s = struct" (String.capitalize name);
  line out "  open Record_%s" name;

  let error_location = sprintf "`Record %S" name in

  line out "let add_value";
  List.iter fields (function
  | (n, Option t) -> line out "   ?%s" n;
  | (n, t) -> line out "   ~%s" n;
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

  line out "let of_value r = ";
  line out "  let open Sql_query in";
  line out "  try Ok {g_id = r.r_id; g_created = r.r_created; \n\
      \  g_last_modified = r.r_last_modified; g_value = value_of_sexp r.r_sexp }\n\
      \  with e -> Error (`parse_sexp_error (r.r_sexp, e))";

  line out "let to_value t = ";
  line out "  {Sql_query. ";
  line out "    r_id = t.g_id;";
  line out "    r_type = %S;" name;
  line out "    r_created = t.g_created;";
  line out "    r_last_modified = t.g_last_modified;";
  line out "    r_sexp = sexp_of_value t.g_value; ";
  line out "  }";

  line out "let get ~dbh pointer =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.get_value_sexp ~record_name:%S pointer.id in"
      name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun r -> of_result (Sql_query.should_be_single r)";
    line out "  >>= fun r -> of_result (Sql_query.parse_value r)";
    line out "  >>= fun r -> of_result (of_value r)";
  );
  
  line out "let get_all ~dbh =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.get_all_values_sexp ~record_name:%S in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun results ->";
    line out "  of_list_sequential results ~f:(fun row ->";
    line out "    of_result Result.(Sql_query.parse_value row >>= of_value))";
  );

  doc out "Update the 'value' (one {b should not} modify the [g_*] fields.";
  line out "let update ~dbh t =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.update_value_sexp \
                            ~record_name:%S t.g_id (sexp_of_value t.g_value) in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ -> return ()";
  );

  line out "let delete_value_unsafe ~dbh v =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.delete_value_sexp \
                            ~record_name:%S v.g_id in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ -> return ()";
  );

  line out "let insert_value_unsafe ~dbh v =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.insert_value (to_value v) in";
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ ->";
    line out "  Backend.query ~dbh Sql_query.update_sequence >>= fun _ ->";
    line out "  return ()";
  );

  
  line out "end";

  ()

let ocaml_function_access_module ~out name result_type args =
  line out "module %s = struct" (String.capitalize name);
  line out "  open Function_%s" name;

  let error_location = sprintf "`Function %S" name in

  line out "let add_evaluation";
  line out "    ?(recomputable=false)";
  line out "    ?(recompute_penalty=0.)";
  List.iter args (function
  | (n, Option t) -> line out "   ?%s" n;
  | (n, t) -> line out "   ~%s" n;
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
  
  line out "let of_evaluation r = ";
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

  line out "let to_evaluation t =";
  line out "  {Sql_query. ";
  line out "  f_id = t.g_id;";
  line out "  f_type = %S;" name;
  line out "  f_result = Option.map t.g_result (fun x -> x.Record_%s.id); "
    result_type;
  line out "  f_recomputable = t.g_recomputable; ";
  line out "  f_recompute_penalty = t.g_recompute_penalty; ";
  line out "  f_inserted = t.g_inserted; ";
  line out "  f_started = t.g_started; ";
  line out "  f_completed = t.g_completed; ";
  line out "  f_status = t.g_status; ";
  line out "  f_sexp = sexp_of_evaluation t.g_evaluation; ";
  line out "  }";

  line out "let get ~dbh pointer =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.get_evaluation_sexp \
                ~function_name:%S pointer.id in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun r -> of_result (Sql_query.should_be_single r)";
    line out "  >>= fun r -> of_result (Sql_query.parse_evaluation r)";
    line out "  >>= fun r -> of_result (of_evaluation r)";
  );
  
  line out "let get_all ~dbh =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out " let query = Sql_query.get_all_evaluations_sexp \
      ~function_name:%S in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun results ->";
    line out "  of_list_sequential results ~f:(fun row ->";
    line out "    of_result Result.(Sql_query.parse_evaluation row >>= \
                 of_evaluation))";
  );

  line out "let set_started ~dbh p =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.set_evaluation_started \
                            ~function_name:%S p.id in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ -> return ()";
  );
  line out "let set_failed ~dbh p =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.set_evaluation_failed \
                            ~function_name:%S p.id in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ -> return ()";
  );
  line out "let set_succeeded ~dbh ~result p =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.set_evaluation_succeeded \
                            ~function_name:%S p.id result.Record_%s.id in"
      name result_type;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ -> return ()";
  );
  
  line out "let delete_evaluation_unsafe ~dbh v =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.delete_evaluation_sexp \
                            ~function_name:%S v.g_id in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ -> return ()";
  );

  line out "let insert_evaluation_unsafe ~dbh v =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.insert_evaluation (to_evaluation v) in";
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ ->";
    line out "  Backend.query ~dbh Sql_query.update_sequence >>= fun _ ->";
    line out "  return ()";
  );

  line out "end";
  
  ()

let ocaml_file_system_access_module ~out dsl =
  line out "module Volume = struct";
  line out "  open File_system";

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
  line out "let add_link_volume ~kind ~dbh ~pointer =";
  line out "  add_volume ~kind ~dbh ~content:(Link pointer)";

  
  line out "let of_volume r = ";
  line out "  let open Sql_query in";
  line out "  try Ok {g_id = r.v_id; \
      \   g_kind = Enumeration_volume_kind.of_string_exn r.v_kind; \n\
      \   g_content = content_of_sexp r.v_sexp } \
      \  with e -> Error (`parse_sexp_error (r.v_sexp, e))";
  line out "let to_volume t = {Sql_query. v_id = t.g_id; ";
  line out "    v_kind = Enumeration_volume_kind.to_string t.g_kind;";
  line out "    v_sexp = sexp_of_content t.g_content } ";

  line out "let get ~dbh pointer =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.get_volume_sexp pointer.id in";
    line out "  Backend.query ~dbh query";
    line out "  >>= fun r -> of_result (Sql_query.should_be_single r)";
    line out "  >>= fun r -> of_result (Sql_query.parse_volume r)";
    line out "  >>= fun r -> of_result (of_volume r)";
  );
  line out "let get_all ~dbh =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.get_all_volumes_sexp () in";
    line out "  Backend.query ~dbh query";
    line out "  >>= fun results ->";
    line out "  of_list_sequential results ~f:(fun row ->";
    line out "    of_result Result.(Sql_query.parse_volume row \
                    >>= of_volume))";
  );

  line out "let delete_volume_unsafe ~dbh v =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.delete_volume_sexp v.g_id in";
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ -> return ()";
  );

  line out "let insert_volume_unsafe ~dbh v =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.insert_volume (to_volume v) in";
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ ->";
    line out "  Backend.query ~dbh Sql_query.update_sequence >>= fun _ ->";
    line out "  return ()";
  );
  line out "end"

let ocaml_classy_access ~out dsl =
  line out "
class ['pointer, 'pointed, 'error ] pointer
  (layout_get: 'pointer -> ('pointed, 'error) Flow.monad)
  (id: 'pointer -> int)
  (p: 'pointer) =
object
  method id = id p
  method pointer = p
  method get = layout_get p
end";
  let make_pointer_object p t modname =
    sprintf "new pointer layout#%s#get (fun p -> p.%s.id) %s" t modname p
  in
  let ocaml_object_transform_field modname field = function
    | Record_name s ->
      make_pointer_object (sprintf "%s.(%s)" modname field) s
        (sprintf "Record_%s" s)
    | Option (Record_name s) ->
      sprintf "Option.map %s.(%s) ~f:(fun p -> %s)" modname field
        (make_pointer_object "p" s (sprintf "Record_%s" s))
    | Array (Record_name s) ->
      sprintf "Array.map %s.(%s) ~f:(fun p -> %s)" modname field
        (make_pointer_object "p" s (sprintf "Record_%s" s))
    | Volume_name s ->
      make_pointer_object 
        (sprintf "%s.(%s)" modname field) "file_system" "File_system"
    | Option (Volume_name s) ->
      sprintf "Option.map %s.(%s) ~f:(fun p -> %s)" modname field
        (make_pointer_object "p" "file_system" "File_system")
    | Array (Volume_name s) ->
      sprintf "Array.map %s.(%s) ~f:(fun p -> %s)" modname field
        (make_pointer_object "p" "file_system" "File_system")

    | _ -> sprintf "%s.(%s)" modname field
  in
  let get_like_easy_methods modname eltname types_module =
    line out "\
      method all =
        (%s.get_all ~dbh)
        >>| List.map ~f:(%s layout)
      method get p =
        %s.get ~dbh p >>| %s layout
      method get_unsafe p = 
        %s.(get ~dbh (%s.unsafe_cast p)) >>| %s layout"
      modname eltname modname eltname modname types_module eltname;
  in

  let add_method ?(method_name="add") add_function fields name modname =
    line out "  method %s " method_name;
    List.iter fields (function
    | (n, Option t) -> line out "   ?%s" n
    | (n, t) -> line out "   ~%s" n
    );
    line out "    () =";
    line out "    %s ~dbh " add_function;
    List.iter fields (function
    | (n, Option t) -> line out "   ?%s" n
    | (n, t) -> line out "   ~%s" n
    );
    line out "    >>= fun p ->";
    line out "    return (%s)"
      (make_pointer_object "p" name modname)
  in
  line out "type hidden_for_ocamldoc = unit";
  line out "let classy (dbh : Backend.db_handle) = ( () : hidden_for_ocamldoc )";

  line out "(**/**)";
  line out "let rec make (dbh : Backend.db_handle) =";
  line out "  object (self_layout)";
  line out "  method _dbh = dbh";
  List.iter dsl.nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    line out "  method %s = %s self_layout" name name
  | Function (name, args, result) ->
    line out "  method %s = %s self_layout" name name
  | Volume (_, _) -> ()
  );
  line out "  method file_system = file_system self_layout";
  line out "  end";
  line out "and volume layout t = object";
  line out "  method g_id = t.File_system.g_id";
  line out "  method g_kind = t.File_system.g_kind";
  line out "  method g_content = t.File_system.g_content";
  line out "  method g_pointer = File_system.(pointer t)";
  line out "  end";
  line out "and file_system layout = let dbh = layout#_dbh in object";
  get_like_easy_methods "Volume" "volume" "File_system";
  (* Using type 'Int' is a hack, add_method only cares about options: *)
  add_method ~method_name:"add_volume"
    "Volume.add_volume " ["kind", Int; "content", Int]
    "file_system" "File_system";
  add_method ~method_name:"add_tree_volume"
    "Volume.add_tree_volume "
    ["kind", Int; "hr_tag", Option String; "files", Int]
    "file_system" "File_system";
  add_method ~method_name:"add_link_volume"
    "Volume.add_link_volume "
    ["kind", Int; "pointer", Int]
    "file_system" "File_system";
  
  line out "  end";
  List.iter dsl.nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    (* record element  *)
    line out "and %s_element layout t = object " name;
    line out "  method g_pointer = Record_%s.(pointer t)" name;
    List.iter record_standard_fields (fun (n, t) ->
      line out "  method %s = t.Record_%s.%s" n name n;
    );
    List.iter fields (fun (n, t) ->
      line out "  method %s = %s" n
        (ocaml_object_transform_field
           (sprintf "Record_%s" name) (sprintf "t.g_value.%s" n) t);
    );
    line out "  end";
    (* record collection *)
    line out "and %s layout = let dbh = layout#_dbh in object " name;
    let modname = sprintf "%s" (String.capitalize name) in
    get_like_easy_methods modname (sprintf "%s_element" name)
      (sprintf "Record_%s" name);
    add_method (sprintf "%s.add_value" modname) fields
      name (sprintf "Record_%s" name);
    line out "  end";
  | Function (name, args, result) ->
    (* function element  *)
    line out "and %s_element layout t = let dbh = layout#_dbh in object(self)" name;
    let modname = sprintf "%s" (String.capitalize name) in
    let types_module = sprintf "Function_%s" name in
    line out "val mutable t = t";
    line out "  method g_pointer = Function_%s.(pointer t)" name;
    List.iter (function_standard_fields result) (fun (n, t) ->
      line out "  method %s = %s" n
        (ocaml_object_transform_field types_module (sprintf "t.%s" n) t);
    );
    List.iter args (fun (n, t) ->
      line out "  method %s = %s" n
        (ocaml_object_transform_field types_module (sprintf "t.g_evaluation.%s" n) t);
    );
    line out "  method re_get = \n\
                  %s.(get ~dbh (%s.pointer t)) >>= fun new_t -> \n\
                  t <- new_t; return ()" modname types_module;
    line out "  method set_started = \n\
                  %s.(set_started ~dbh (%s.pointer t)) >>= fun () -> \n\
                  self#re_get" modname types_module;
    line out "  method set_failed = \n\
                  %s.(set_failed ~dbh (%s.pointer t)) >>= fun () -> \n\
                  self#re_get" modname types_module;
    line out "  method set_succeeded result = \n\
                  %s.(set_succeeded ~result ~dbh (%s.pointer t)) \n\
                  >>= fun () -> \n\
                  self#re_get" modname types_module;
    line out "  end";
    (* function collection  *)
    line out "and %s layout = let dbh = layout#_dbh in object " name;
    get_like_easy_methods modname (sprintf "%s_element" name) types_module;
    let add_args =
      ("recomputable", Option Bool) :: ("recompute_penalty", Option Real) :: args in
    add_method (sprintf "%s.add_evaluation" modname) add_args name types_module;
    line out "  end";
  | Volume (_, _) -> ()
  );

  line out "(**/**)";
  
  ()

let ocaml_meta_module ~out ~raw_dsl dsl =
  doc out "{3 Meta-Information On the Layout}";
  line out "module Meta = struct";

  doc out "The source used to generate all {i this}.";
  line out "let source_file = %S" raw_dsl;

  doc out "Get the Layout (will be parsed only once).";
  line out "let layout = \n\
            \   let l = ref None in \n\
            \   fun () ->\n\
            \   match !l with\n| Some s -> s \n\
            \   | None ->\n\
            \     let lp = Hitscoregen_layout_dsl.parse_str source_file in\n\
            \     l := Some lp; lp";

  line out "end"

    
let ocaml_module raw_dsl dsl output_string =
  let out = output_string in
  doc out "Autogenerated Layout module.";
  line out "open Hitscore_std";
  line out "open Hitscore_db_backend";

  line out "module Layout = struct ";

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
    line out "let unsafe_cast id = { id }";
    line out "end";
  | Volume (_, _) -> ()
  );

  line out "type dump = {";
  line out "  version: string;";
  line out "  file_system: File_system.t list;";
  List.iter all_nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    line out "  %s: Record_%s.t list;" name name;
  | Function (name, args, result) ->
    line out "  %s: Function_%s.t list;" name name;
  | Volume (_, _) -> ()
  );
  line out "} with sexp";

  line out "end";
  line out "open Layout";
  
  doc out "Queries manipulating the stuff in the layout";
  line out "module Access = struct";

  ocaml_file_system_access_module ~out dsl;
  List.iter all_nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    ocaml_record_access_module ~out name fields
  | Function (name, args, result) ->
    ocaml_function_access_module ~out name result args
  | Volume (_, _) -> ()
  );


  line out "let get_dump ~dbh =";
  line out "  let version = Hitscore_conf_values.version in";
  line out "  Volume.get_all ~dbh >>= fun file_system ->";
  let names = ref [] in
  List.iter all_nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    line out "  %s.get_all ~dbh >>= fun %s ->" (String.capitalize name) name;
    names := name :: !names;
  | Function (name, args, result) ->
    line out "  %s.get_all ~dbh >>= fun %s ->" (String.capitalize name) name;
    names := name :: !names;
  | Volume (_, _) -> ()
  );
  line out "return {version; file_system; %s}" (String.concat ~sep:"; " !names);

  line out "let insert_dump ~dbh ?(check_version=true) dump =";
  line out "  if check_version && \
                    dump.version <> Hitscore_conf_values.version then (";
  line out "    error (`Layout (`Dump, `wrong_version (dump.version, \
                          Hitscore_conf_values.version)))";
  line out "  ) else (";
  List.iter all_nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    line out "    of_list_sequential dump.%s \
                  (%s.insert_value_unsafe ~dbh)" name (String.capitalize name);
    line out "    >>= fun (_: unit list) ->";
  | Function (name, args, result) ->
    line out "    of_list_sequential dump.%s \
                      (%s.insert_evaluation_unsafe ~dbh)"
      name (String.capitalize name);
    line out "    >>= fun (_: unit list) ->";
  | Volume (_, _) -> ()
  );
  line out "    of_list_sequential dump.file_system \
                  (Volume.insert_volume_unsafe ~dbh)";
  line out "    >>= fun (_: unit list) ->";
  line out "    return ()\n  )";


  line out "end";

  line out "module Classy = struct";
  line out "open Access";
  line out "open Flow";
  ocaml_classy_access ~out dsl;
  line out "end";


  ocaml_meta_module ~out dsl ~raw_dsl;
  ()

