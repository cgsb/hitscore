open Core.Std

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
              error (`Layout ((%s: error_location), \
                              (e : error_cause))))" error_location;
  ()
      
let ocaml_identify_something ~out ~error_location something =
  line out "let identify_%s ~dbh id =" something;
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.identify_%s id in" something;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun r ->";
    line out "  begin match r with";
    line out "  | [] -> return None";
    line out "  | [[Some one]] -> return (Some one)";
    line out "  | more -> error (`result_not_unique more)";
    line out "  end"
  )

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

  doc out "Get the value pointed by [pointer].";
  line out "let get ~dbh pointer =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.get_value_sexp ~record_name:%S pointer.id in"
      name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun r -> of_result (Sql_query.should_be_single r)";
    line out "  >>= fun r -> of_result (Sql_query.parse_value r)";
    line out "  >>= fun r -> of_result (of_value r)";
  );
  
  doc out "Get all the values of this record (previous versions used
    to return pointers, now they are full values).";
  line out "let get_all ~dbh =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.get_all_values_sexp ~record_name:%S in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun results ->";
    line out "  while_sequential results ~f:(fun row ->";
    line out "    of_result Result.(Sql_query.parse_value row >>= of_value))";
  );

  doc out "Update the 'value' (one {b should not} modify the [g_*] \
    fields, this function won't touch them anyway).";
  line out "let update ~dbh t =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.update_value_sexp \
                            ~record_name:%S t.g_id (sexp_of_value t.g_value) in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ -> return ()";
  );

  doc out "Delete the value from the DB; this {b does not check any pointer}.";
  line out "let delete_value_unsafe ~dbh v =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.delete_value_sexp \
                            ~record_name:%S v.g_id in" name;
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ -> return ()";
  );

  doc out "Insert a “full” value into the DB, [and_update_sequence] \
    defaults to [true]; it means updating the sequence of IDs and \
    ensuring that the next index is [max + 1].";
  line out "let insert_value_unsafe ?(and_update_sequence=true) ~dbh v =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.insert_value (to_value v) in";
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ ->";
    line out "  if and_update_sequence then (";
    line out "    Backend.query ~dbh Sql_query.update_sequence >>= fun _ ->";
    line out "    return ())";
    line out "  else return ()";
  );

  line out "end";

  ()

let ocaml_function_access_module ~out name result_type args =
  line out "module %s = struct" (String.capitalize name);
  line out "  open Function_%s" name;

  let error_location = sprintf "`Function %S" name in

  line out "let add_evaluation";
  List.iter args (function
  | (n, Option t) -> line out "   ?%s" n;
  | (n, t) -> line out "   ~%s" n;
  );
  line out " ~dbh =\n   let v = { ";
  List.iter args (function (n, _) -> raw out "%s; " n);
  line out "} in";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.add_evaluation_sexp ~function_name:%S \
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
    line out "  while_sequential results ~f:(fun row ->";
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

  line out "let insert_evaluation_unsafe ?(and_update_sequence=true) ~dbh v =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.insert_evaluation (to_evaluation v) in";
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ ->";
    line out "  if and_update_sequence then (";
    line out "    Backend.query ~dbh Sql_query.update_sequence >>= fun _ ->";
    line out "    return ())";
    line out "  else return ()";
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
    line out "  while_sequential results ~f:(fun row ->";
    line out "    of_result Result.(Sql_query.parse_volume row \
                    >>= of_volume))";
  );

  line out "let delete_volume_unsafe ~dbh v =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.delete_volume_sexp v.g_id in";
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ -> return ()";
  );

  line out "let insert_volume_unsafe ?(and_update_sequence=true) ~dbh v =";
  ocaml_encapsulate_layout_errors out ~error_location (fun out ->
    line out "  let query = Sql_query.insert_volume (to_volume v) in";
    line out "  Backend.query ~dbh query";
    line out "  >>= fun _ ->";
    line out "  if and_update_sequence then (";
    line out "    Backend.query ~dbh Sql_query.update_sequence >>= fun _ ->";
    line out "    return ())";
    line out "  else return ()";
  );

  line out "end"

let ocaml_classy_access ~out dsl =
  line out "
class ['pointer, 'pointed, 'error ] pointer
  (layout_get: 'pointer -> ('pointed, 'error) Sequme_flow.t)
  (id: 'pointer -> int)
  (p: 'pointer) =
object
  constraint 'error = [> `Layout of error_location * error_cause ]
  method id = id p
  method pointer = p
  method get = layout_get p
end";

  let make_pointer_object p tget class_name modname =
    sprintf "(new pointer (%s : %s.pointer -> ('error %s, 'error) Sequme_flow.t) \
                         (fun p -> p.%s.id) %s : \
                (%s.pointer, 'error %s, 'error) pointer)"
      tget modname class_name modname p modname class_name
  in
  let record_get s =
    sprintf "fun p -> %s.get ~dbh p >>| new %s_element dbh"
      (String.capitalize s) s in
  let function_get s =
    sprintf "fun p -> %s.get ~dbh p >>| new %s_element dbh"
      (String.capitalize s) s in
  let volume_get = "fun p -> Volume.get ~dbh p >>| new volume_element dbh" in
  let ocaml_object_transform_field modname field =
    function
    | Record_name s ->
      make_pointer_object
        (sprintf "%s.(%s)" modname field)
        (record_get s)
        (sprintf "%s_element" s)
        (sprintf "Record_%s" s)
    | Option (Record_name s) ->
      sprintf "Option.map %s.(%s) ~f:(fun p -> %s)" modname field
        (make_pointer_object "p"
           (record_get s)
           (sprintf "%s_element" s)
           (sprintf "Record_%s" s))
    | Array (Record_name s) ->
      sprintf "Array.map %s.(%s) ~f:(fun p -> %s)" modname field
        (make_pointer_object "p"
           (record_get s)
           (sprintf "%s_element" s)
           (sprintf "Record_%s" s))
    | Volume_name s ->
      make_pointer_object 
        (sprintf "%s.(%s)" modname field)
        volume_get
        "volume_element"
        "File_system"
    | Option (Volume_name s) ->
      sprintf "Option.map %s.(%s) ~f:(fun p -> %s)" modname field
        (make_pointer_object "p"
           volume_get
           "volume_element"
           "File_system")
    | Array (Volume_name s) ->
      sprintf "Array.map %s.(%s) ~f:(fun p -> %s)" modname field
        (make_pointer_object "p"
           volume_get
           "volume_element"
           "File_system")
    | _ -> sprintf "%s.(%s)" modname field
  in

  let add_method ?(method_name="add") add_function fields tget class_name modname =
    line out "  method %s " method_name;
    List.iter fields (function
    | (n, Option t) -> line out "   ?%s" n
    | (n, t) -> line out "   ~%s" n
    );
    line out "    () : ((%s.pointer, 'error %s, 'error) pointer, 'error) Sequme_flow.t ="
      modname class_name;
    line out "    %s ~dbh " add_function;
    List.iter fields (function
    | (n, Option t) -> line out "   ?%s" n
    | (n, t) -> line out "   ~%s" n
    );
    line out "    >>= fun p ->";
    line out "    return (%s)"
      (make_pointer_object "p" tget class_name modname)
  in
  

  line out "class ['error] volume_element dbh t = object";
 (* line out "  constraint 'error = \
               [> `Layout of Layout.error_location * Layout.error_cause ]"; *)
  line out "  val mutable t = t";
  line out "  method g_id = t.File_system.g_id";
  line out "  method g_kind = t.File_system.g_kind";
  line out "  method g_content = t.File_system.g_content";
  line out "  method g_pointer = File_system.(pointer t)";
  line out "  method re_get : (unit, 'error) Sequme_flow.t = \n\
                  Volume.get ~dbh (File_system.pointer t) >>= fun new_t -> \n\
                  t <- new_t; return ()";
  line out "  end";
  List.iter dsl.nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    let modname = sprintf "%s" (String.capitalize name) in
    let types_module = sprintf "Record_%s" name in
    line out "class ['error] %s_element dbh t = object (self)" name;
    (*line out "  constraint 'error = \
               [> `Layout of Layout.error_location * Layout.error_cause ]";*)
    line out "  val mutable t = t";
    line out "  method g_pointer = Record_%s.(pointer t)" name;
    line out "  method g_t = t";
    List.iter record_standard_fields (fun (n, t) ->
      line out "  method %s = t.Record_%s.%s" n name n;
    );
    List.iter fields (fun (n, t) ->
      line out "  method %s = %s" n
        (ocaml_object_transform_field
           (sprintf "Record_%s" name) (sprintf "t.g_value.%s" n) t);
      line out "  method set_%s v =" n;
      if List.length fields = 1 then (
        line out "    let g_v = { %s.%s = v } in" types_module n
      ) else (
        line out "    let g_v = { t.%s.g_value with %s.%s = v } in"
          types_module types_module n
      );
      line out "    t <- { t with %s.g_value = g_v };\n\
               \    (Access.%s.update ~dbh t: (unit, 'error) Sequme_flow.t)"
        types_module modname;
    );
    line out "  method re_get : (unit, 'error) Sequme_flow.t = \n\
                  %s.(get ~dbh (%s.pointer t)) >>= fun new_t -> \n\
                  t <- new_t; return ()" modname types_module;
    line out "end"
  | Function (name, args, result) ->
    line out "class ['error] %s_element dbh t = object(self)" name;
    let modname = sprintf "%s" (String.capitalize name) in
    let types_module = sprintf "Function_%s" name in
    line out "val mutable t = t";
    line out "  method g_pointer = Function_%s.(pointer t)" name;
    line out "  method g_t = t";
    List.iter (function_standard_fields result) (fun (n, t) ->
      line out "  method %s = %s" n
        (ocaml_object_transform_field types_module (sprintf "t.%s" n) t);
    );
    List.iter args (fun (n, t) ->
      line out "  method %s = %s" n
        (ocaml_object_transform_field types_module (sprintf "t.g_evaluation.%s" n) t);
    );
    line out "  method re_get : (unit, 'error) Sequme_flow.t = \n\
                  %s.(get ~dbh (%s.pointer t)) >>= fun new_t -> \n\
                  t <- new_t; return ()" modname types_module;
    line out "  method set_started : (unit, 'error) Sequme_flow.t  = \n\
                  %s.(set_started ~dbh (%s.pointer t)) >>= fun () -> \n\
                  self#re_get" modname types_module;
    line out "  method set_failed  : (unit, 'error) Sequme_flow.t = \n\
                  %s.(set_failed ~dbh (%s.pointer t)) >>= fun () -> \n\
                  self#re_get" modname types_module;
    line out "  method set_succeeded result  : (unit, 'error) Sequme_flow.t = \n\
                  %s.(set_succeeded ~result ~dbh (%s.pointer t)) \n\
                  >>= fun () -> \n\
                  self#re_get" modname types_module;
    line out "  end";
  | Volume (_, _) -> ()
  );


  line out "
class ['pointer, 'pointed, 'error ] collection
  (layout_get: 'pointer -> ('pointed, 'error) Sequme_flow.t)
  (layout_get_all: unit -> ('pointed list, 'error) Sequme_flow.t)
  (unsafe_cast: int -> 'pointer)
 =
object
method all = (layout_get_all ())
method get p = layout_get p
method get_unsafe i = layout_get (unsafe_cast i)
end";
  let make_collection access_mod types_mod class_name =
    sprintf 
      "new collection \n\
      \  ((fun p -> %s.get ~dbh p >>| new %s dbh): \
         %s.pointer -> ('error %s, 'error) Sequme_flow.t)\n\
      \  (fun () -> %s.get_all ~dbh >>| List.map ~f:(new %s dbh)) \n\
      %s.unsafe_cast"
      access_mod class_name types_mod class_name access_mod class_name types_mod
  in
  line out "class ['error] layout dbh = object";
  line out "method file_system = %s"
    (make_collection "Volume" "File_system" "volume_element");
  add_method ~method_name:"add_volume"
    "Volume.add_volume " ["kind", Int; "content", Int]
    volume_get "volume_element" "File_system";
  add_method ~method_name:"add_tree_volume"
    "Volume.add_tree_volume "
    ["kind", Int; "hr_tag", Option String; "files", Int]
    volume_get "volume_element" "File_system";
  add_method ~method_name:"add_link_volume"
    "Volume.add_link_volume "
    ["kind", Int; "pointer", Int]
    volume_get "volume_element" "File_system";
  List.iter dsl.nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    let types_module = sprintf "Record_%s" name in
    let access_module = String.capitalize name in
    let class_name = sprintf "%s_element" name in
    line out "  method %s = %s " name
      (make_collection access_module (types_module)
         (sprintf "%s_element" name));
    add_method ~method_name:(sprintf "add_%s" name)
      (sprintf "%s.add_value" access_module)
      fields (record_get name) class_name types_module
  | Function (name, args, result) ->
    let types_module = sprintf "Function_%s" name in
    let access_module = String.capitalize name in
    let class_name = sprintf "%s_element" name in
    line out "  method %s = %s " name
      (make_collection access_module (types_module)
         (sprintf "%s_element" name));
    add_method ~method_name:(sprintf "add_%s" name)
      (sprintf "%s.add_evaluation" access_module)
      args (function_get name) class_name types_module
  | Volume (_, _) -> ()
  );

  line out "end";


  line out "let make (dbh : Backend.db_handle) = new layout dbh";

  
  ()


let ocaml_dependency_graph_module ~out all_nodes =
  doc out "Explore the dependencies in the Layout";
  line out "module Dependency_graph = struct";

  doc out "Report the list of pointers referenced by a given
          function or record (volumes always return [\\[\\]]).";
  line out "  let get_children ~dbh (universal_pointer: Universal.pointer) =";
  line out "    begin match universal_pointer with";
  List.iter all_nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    line out "  | `%s_pointer p ->" name;
    line out "    Access.%s.get ~dbh p >>= fun t ->" (String.capitalize name);
    line out "    return (List.concat [";
    List.iter fields (function
    | n, Record_name r
    | n, Volume_name r ->
      line out "      [(`%s_pointer Record_%s.(t.g_value.%s) : \
                            Universal.pointer)];" r name n;
    | n, Option (Record_name r)
    | n, Option (Volume_name r) ->
      line out "      Option.value_map ~default:[] Record_%s.(t.g_value.%s) \
                          ~f:(fun o -> [(`%s_pointer  o : Universal.pointer)]);"
        name n r;
    | n, Array (Record_name r)
    | n, Array (Volume_name r) ->
      line out "      List.map (Array.to_list Record_%s.(t.g_value.%s)) (fun e -> \
                       \ `%s_pointer e);" name n r;
    | _ -> ());
    line out "    ])";
  | Function (name, args, result) ->
    line out "  | `%s_pointer p ->" name;
    line out "    Access.%s.get ~dbh p >>= fun t ->" (String.capitalize name);
    line out "    return (List.concat [";
    List.iter args (function
    | n, Record_name r
    | n, Volume_name r ->
      line out "      [(`%s_pointer Function_%s.(t.g_evaluation.%s) : \
                            Universal.pointer)];" r name n;
    | n, Option (Record_name r)
    | n, Option (Volume_name r) ->
      line out "      Option.value_map ~default:[] Function_%s.(t.g_evaluation.%s) \
                          ~f:(fun o -> [(`%s_pointer  o : Universal.pointer)]);"
        name n r;
    | n, Array (Record_name r)
    | n, Array (Volume_name r) ->
      line out "      List.map (Array.to_list Function_%s.(t.g_evaluation.%s)) \
                     \  (fun e -> `%s_pointer e);" name n r;
    | _ -> ());
    line out "      Option.value_map ~default:[] Function_%s.(t.g_result) \
                          ~f:(fun o -> [(`%s_pointer  o : Universal.pointer)]);"
      name result;
    line out "    ])";
  | Volume (name, _) -> 
    line out "  | `%s_pointer p -> return []" name;
  );
  line out "    end";


  doc out "Get the records and functions that point to a given record
    of volume (functions always return [\\[\\]]).";
  line out "let get_parents ~dbh (unip : Universal.pointer) =";
  line out "  begin match unip with";
  List.iter all_nodes (function
  | Enumeration (_, _) -> ()
  | Record (name, _) ->
    line out "  | `%s_pointer p ->" name;
    let recognized_parents = ref [] in
    List.iter all_nodes (function
    | Enumeration (_, _) -> ()
    | Record (name_parent, fields) ->
      List.iter fields (function
      | n, Record_name r when r = name ->
        line out "Access.%s.get_all ~dbh" (String.capitalize name_parent);
        line out ">>| List.filter_map ~f:(fun t -> ";
        line out "      let open Record_%s in" name_parent;
        line out "      if t.g_value.%s = p" n;
        line out "      then Some (`%s_pointer (pointer t))" name_parent;
        line out "      else None)";
        let list_name = sprintf "all_%s_%s" name_parent n in
        line out ">>= fun %s ->" list_name;
        recognized_parents := list_name :: !recognized_parents;
      | n, Option (Record_name r) when r = name ->
        line out "Access.%s.get_all ~dbh" (String.capitalize name_parent);
        line out ">>| List.filter_map ~f:(fun t -> ";
        line out "      let open Record_%s in" name_parent;
        line out "      if t.g_value.%s = Some p" n;
        line out "      then Some (`%s_pointer (pointer t))" name_parent;
        line out "      else None)";
        let list_name = sprintf "all_%s_%s" name_parent n in
        line out ">>= fun %s ->" list_name;
        recognized_parents := list_name :: !recognized_parents;
      | n, Array (Record_name r) when r = name ->
        line out "Access.%s.get_all ~dbh" (String.capitalize name_parent);
        line out ">>| List.filter_map ~f:(fun t -> ";
        line out "      let open Record_%s in" name_parent;
        line out "      if Array.exists t.g_value.%s ((=) p)" n;
        line out "      then Some (`%s_pointer (pointer t))" name_parent;
        line out "      else None)";
        let list_name = sprintf "all_%s_%s" name_parent n in
        line out ">>= fun %s ->" list_name;
        recognized_parents := list_name :: !recognized_parents;
      | _ -> ())
    | Function (name_parent, args, result) ->
      List.iter args (function
      | n, Record_name r when r = name ->
        line out "Access.%s.get_all ~dbh" (String.capitalize name_parent);
        line out ">>| List.filter_map ~f:(fun t -> ";
        line out "      let open Function_%s in" name_parent;
        line out "      if t.g_evaluation.%s = p" n;
        line out "      then Some (`%s_pointer (pointer t))" name_parent;
        line out "      else None)";
        let list_name = sprintf "all_%s_%s" name_parent n in
        line out ">>= fun %s ->" list_name;
        recognized_parents := list_name :: !recognized_parents;
      | n, Option (Record_name r) when r = name ->
        line out "Access.%s.get_all ~dbh" (String.capitalize name_parent);
        line out ">>| List.filter_map ~f:(fun t -> ";
        line out "      let open Function_%s in" name_parent;
        line out "      if t.g_evaluation.%s = Some p" n;
        line out "      then Some (`%s_pointer (pointer t))" name_parent;
        line out "      else None)";
        let list_name = sprintf "all_%s_%s" name_parent n in
        line out ">>= fun %s ->" list_name;
        recognized_parents := list_name :: !recognized_parents;
      | n, Array (Record_name r) when r = name ->
        line out "Access.%s.get_all ~dbh" (String.capitalize name_parent);
        line out ">>| List.filter_map ~f:(fun t -> ";
        line out "      let open Function_%s in" name_parent;
        line out "      if Array.exists t.g_evaluation.%s ((=) p)" n;
        line out "      then Some (`%s_pointer (pointer t))" name_parent;
        line out "      else None)";
        let list_name = sprintf "all_%s_%s" name_parent n in
        line out ">>= fun %s ->" list_name;
        recognized_parents := list_name :: !recognized_parents;
      | _ -> ());
      if result = name then (
        line out "Access.%s.get_all ~dbh" (String.capitalize name_parent);
        line out ">>| List.filter_map ~f:(fun t -> ";
        line out "      let open Function_%s in" name_parent;
        line out "      if t.g_result = Some p";
        line out "      then Some (`%s_pointer (pointer t))" name_parent;
        line out "      else None)";
        let list_name = sprintf "all_%s_g_result" name_parent in
        line out ">>= fun %s ->" list_name;
        recognized_parents := list_name :: !recognized_parents;
      );
    | Volume (name, _) -> ());
    line out "    return (List.concat [%s] |! List.dedup)"
      (String.concat ~sep:";\n" !recognized_parents)
  | Function (name, _, _) ->
    line out "  | `%s_pointer p -> return []" name;
  | Volume (name, _) -> 
    line out "  | `%s_pointer p ->" name;
    let recognized_parents = ref [] in
    List.iter all_nodes (function
    | Enumeration (_, _) -> ()
    | Record (name_parent, fields) ->
      List.iter fields (function
      | n, Volume_name r when r = name ->
        line out "Access.%s.get_all ~dbh" (String.capitalize name_parent);
        line out ">>| List.filter_map ~f:(fun t -> ";
        line out "      let open Record_%s in" name_parent;
        line out "      if t.g_value.%s = p" n;
        line out "      then Some (`%s_pointer (pointer t))" name_parent;
        line out "      else None)";
        let list_name = sprintf "all_%s_%s" name_parent n in
        line out ">>= fun %s ->" list_name;
        recognized_parents := list_name :: !recognized_parents;
      | n, Option (Volume_name r) when r = name ->
        line out "Access.%s.get_all ~dbh" (String.capitalize name_parent);
        line out ">>| List.filter_map ~f:(fun t -> ";
        line out "      let open Record_%s in" name_parent;
        line out "      if t.g_value.%s = Some p" n;
        line out "      then Some (`%s_pointer (pointer t))" name_parent;
        line out "      else None)";
        let list_name = sprintf "all_%s_%s" name_parent n in
        line out ">>= fun %s ->" list_name;
        recognized_parents := list_name :: !recognized_parents;
      | n, Array (Volume_name r) when r = name ->
        line out "Access.%s.get_all ~dbh" (String.capitalize name_parent);
        line out ">>| List.filter_map ~f:(fun t -> ";
        line out "      let open Record_%s in" name_parent;
        line out "      if Array.exists t.g_value.%s ((=) p)" n;
        line out "      then Some (`%s_pointer (pointer t))" name_parent;
        line out "      else None)";
        let list_name = sprintf "all_%s_%s" name_parent n in
        line out ">>= fun %s ->" list_name;
        recognized_parents := list_name :: !recognized_parents;
      | _ -> ())
    | Function (name_parent, args, result) ->
      List.iter args (function
      | n, Volume_name r when r = name ->
        line out "Access.%s.get_all ~dbh" (String.capitalize name_parent);
        line out ">>| List.filter_map ~f:(fun t -> ";
        line out "      let open Function_%s in" name_parent;
        line out "      if t.g_evaluation.%s = p" n;
        line out "      then Some (`%s_pointer (pointer t))" name_parent;
        line out "      else None)";
        let list_name = sprintf "all_%s_%s" name_parent n in
        line out ">>= fun %s ->" list_name;
        recognized_parents := list_name :: !recognized_parents;
      | n, Option (Volume_name r) when r = name ->
        line out "Access.%s.get_all ~dbh" (String.capitalize name_parent);
        line out ">>| List.filter_map ~f:(fun t -> ";
        line out "      let open Function_%s in" name_parent;
        line out "      if t.g_evaluation.%s = Some p" n;
        line out "      then Some (`%s_pointer (pointer t))" name_parent;
        line out "      else None)";
        let list_name = sprintf "all_%s_%s" name_parent n in
        line out ">>= fun %s ->" list_name;
        recognized_parents := list_name :: !recognized_parents;
      | n, Array (Volume_name r) when r = name ->
        line out "Access.%s.get_all ~dbh" (String.capitalize name_parent);
        line out ">>| List.filter_map ~f:(fun t -> ";
        line out "      let open Function_%s in" name_parent;
        line out "      if Array.exists t.g_evaluation.%s ((=) p)" n;
        line out "      then Some (`%s_pointer (pointer t))" name_parent;
        line out "      else None)";
        let list_name = sprintf "all_%s_%s" name_parent n in
        line out ">>= fun %s ->" list_name;
        recognized_parents := list_name :: !recognized_parents;
      | _ -> ());
      if result = name then (
        line out "Access.%s.get_all ~dbh" (String.capitalize name_parent);
        line out ">>| List.filter_map ~f:(fun t -> ";
        line out "      let open Function_%s in" name_parent;
        line out "      if t.g_result = Some p";
        line out "      then Some (`%s_pointer (pointer t))" name_parent;
        line out "      else None)";
        let list_name = sprintf "all_%s_g_result" name_parent in
        line out ">>= fun %s ->" list_name;
        recognized_parents := list_name :: !recognized_parents;
      );
    | Volume (name, _) -> ());
    line out "    return (List.concat [%s] |! List.dedup : Universal.pointer list)"
      (String.concat ~sep:";\n" !recognized_parents)
  );
  line out "  end";

  doc out "Get the {i grand-grand-*-children} of a given [Universal.pointer].";
  line out "let rec get_descendants ~dbh (p : Universal.pointer) =";
  line out "  get_children ~dbh p >>= fun children ->";
  line out "  while_sequential children (get_descendants ~dbh)";
  line out "  >>= fun grand_and_more_children ->";
  line out "  return (children @ List.concat grand_and_more_children)";

  doc out "Get the {i grand-grand-*-parents} of a given [Universal.pointer].";
  line out "let rec get_ascendants ~dbh (p : Universal.pointer) =";
  line out "  get_parents ~dbh p >>= fun parents ->";
  line out "  while_sequential parents (get_ascendants ~dbh)";
  line out "  >>= fun grand_and_more_parents ->";
  line out "  return (parents @ List.concat grand_and_more_parents)";

  doc out "Do one step in the retrieval of the {i family} of the result of a
    function, that is, get all the ascendants of the descendants of the
    result of a function with the result itself and without the function
    itself (returns [\\[\\]] for record-values and volumes).";
  line out "let follow_function_once ~dbh f =";
  line out "  begin match f with";
  List.iter all_nodes (function
  | Enumeration (_, _) -> ()
  | Record (name, _) ->
    line out "  | `%s_pointer p -> return []" name;
  | Function (name, args, result) ->
    line out "  | `%s_pointer p ->" name;
    line out "     Access.%s.get ~dbh p >>= fun { Function_%s.g_result; _ } ->"
      (String.capitalize name) name;
    line out "     map_option  g_result ~f:(fun r -> \
                     get_descendants ~dbh (`%s_pointer r) \n\
                     >>= fun desc -> return (`%s_pointer r :: desc))" result result;
    line out "     >>| Option.value ~default:[]";
    line out "     >>= fun result_family ->";
    line out "     while_sequential result_family (get_ascendants ~dbh)";
    line out "     >>| List.concat";
    line out "     >>| List.filter ~f:((<>) f)";
    line out "     >>= fun ascend ->";
    line out "     return (List.dedup (ascend @ result_family))"
  | Volume (name, _) -> 
    line out "  | `%s_pointer p -> return []" name;
  );
  line out "  end";
  
  line out "end"

    
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

  line out "type error_location = [";
  line out "| `Dump";
  line out "| `Identification";
  line out "| `Function of string";
  line out "| `Record of string";
  line out "| `File_system";
  line out "]";
  line out "type error_cause = [";
  line out "| `db_backend_error of Backend.error"; 
  line out "| `wrong_add_value";
  line out "| `parse_sexp_error of Hitscore_std.Sexp.t * exn";
  line out "| `parse_value_error of Hitscore_db_backend.Backend.result_item * exn";
  line out "| `parse_volume_error of Hitscore_db_backend.Backend.result_item * exn";
  line out "| `result_not_unique of Hitscore_db_backend.Backend.result";
  line out "| `parse_evaluation_error of \
                  Hitscore_db_backend.Backend.result_item * exn";
  line out "| `wrong_version of string * string";
  line out "]";


  
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
    line out "| s -> raise (Failure (sprintf \"%s %%S\" s))"
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
  
  doc out "Universal types wrapping everything in the Layout.";
  line out "module Universal = struct";
  line out "type pointer = [";
  List.iter all_nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    line out "| `%s_pointer of Record_%s.pointer" name name;
  | Function (name, args, result) ->
    line out "| `%s_pointer of Function_%s.pointer" name name;
  | Volume (name, _) -> 
    line out "| `%s_pointer of File_system.pointer" name;
  );
  line out "] with sexp";

  line out "type value = [";
  List.iter all_nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    line out "| `%s_value of Record_%s.t" name name;
  | Function (name, args, result) ->
    line out "| `%s_evaluation of Function_%s.t" name name;
  | Volume (name, _) -> 
    line out "| `%s_volume of File_system.t" name;
  );
  line out "] with sexp";

  line out "end";

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
  line out "    let and_update_sequence = false in";
  List.iter all_nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    line out "    while_sequential dump.%s \
                  (%s.insert_value_unsafe ~and_update_sequence ~dbh)"
      name (String.capitalize name);
    line out "    >>= fun (_: unit list) ->";
  | Function (name, args, result) ->
    line out "    while_sequential dump.%s \
                      (%s.insert_evaluation_unsafe ~and_update_sequence ~dbh)"
      name (String.capitalize name);
    line out "    >>= fun (_: unit list) ->";
  | Volume (_, _) -> ()
  );
  line out "    while_sequential dump.file_system \
                  (Volume.insert_volume_unsafe ~and_update_sequence ~dbh)";
  line out "    >>= fun (_: unit list) ->";
  line out "    Backend.query ~dbh Sql_query.update_sequence >>= fun _ ->";
  line out "    return ()\n  )";


  ocaml_identify_something ~out ~error_location:"`Identification" "value_type";
  ocaml_identify_something ~out ~error_location:"`Identification" "evaluation_type";
  ocaml_identify_something ~out ~error_location:"`Identification" "volume_kind";
  line out "let identify ~dbh id: (Universal.pointer, _) Sequme_flow.t =";
  line out "  identify_value_type ~dbh id >>= fun value_type_opt ->";
  line out "  begin match value_type_opt with";
  List.iter all_nodes (function
  | Record (name, _) ->
    line out "  | Some %S -> return (`%s_pointer Record_%s.(unsafe_cast id))"
      name name name;
  | _ -> ());
  line out "  | Some n -> error (`unknown_value_type n)";
  line out "  | None ->";
  line out "    identify_evaluation_type ~dbh id >>= fun evaluation_type_opt ->";
  line out "    begin match evaluation_type_opt with";
  List.iter all_nodes (function
  | Function (name, _, _) ->
    line out "    | Some %S -> return (`%s_pointer Function_%s.(unsafe_cast id))"
      name name name;
  | _ -> ());
  line out "    | Some n -> error (`unknown_evaluation_type n)";
  line out "    | None ->";
  line out "      identify_volume_kind ~dbh id >>= fun volume_kind_opt ->";
  line out "      begin match volume_kind_opt with";
  List.iter all_nodes (function
  | Volume (name, _) ->
    line out "      | Some %S -> return (`%s_pointer File_system.(unsafe_cast id))"
      name name;
  | _ -> ());
  line out "      | Some n -> error (`unknown_volume_kind n)";
  line out "      | None -> error (`id_not_found id)";
  line out "      end";
  line out "    end";
  line out "  end";

  line out
    "let get_universal ~dbh upointer: (Universal.value, _) Sequme_flow.t =";
  line out "  match upointer with";
  List.iter all_nodes (function
  | Record (name, _) ->
    line out "  | `%s_pointer p -> %s.get ~dbh p >>= fun v -> `%s_value v |! return"
      name (String.capitalize name) name
  | Function (name, _, _) ->
    line out "  | `%s_pointer p -> %s.get ~dbh p >>= fun v -> `%s_evaluation v |! return"
      name (String.capitalize name) name
  | Volume (name, _) -> 
    line out "  | `%s_pointer p -> Volume.get ~dbh p >>= fun v -> `%s_volume v |! return"
      name  name
  | _ -> ()
  );

  
  line out "end"; (* module Access *)

  doc out "Object-based access to the layout";
  line out "module Classy = struct";
  line out "open Access";
  ocaml_classy_access ~out dsl;
  line out "end";

  line out "module Verify_layout = struct";
  let call_fields name fields =
    List.iter fields (function
    | n, Record_name _
    | n, Volume_name _ ->
      line out "    %s#%s#get >>= fun _ ->" name n;
    | n, Option (Record_name _)
    | n, Option (Volume_name _) ->
      line out "    map_option %s#%s (fun o -> o#get) >>= fun _ ->" name n;
    | n, Array (Record_name _)
    | n, Array (Volume_name _) ->
      line out "    while_sequential (Array.to_list %s#%s) \
                       (fun o -> o#get) >>= fun _ ->" name n;
    | _ -> ()) in 
  line out "let all_pointers ~dbh =";
  line out "  let layout = Classy.make dbh in";
  List.iter all_nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, fields) ->
    line out "  layout#%s#all >>= fun all_%s ->" name name;
    line out "  while_sequential all_%s (fun %s ->" name name;
    call_fields name fields;
    line out "  return ())\n  >>= fun (_ : unit list) ->";
  | Function (name, args, result) ->
    line out "  layout#%s#all >>= fun all_%s ->" name name;
    line out "  while_sequential all_%s (fun %s ->" name name;
    call_fields name (("g_result", Option (Record_name result)) :: args);
    line out "  return ())\n  >>= fun (_ : unit list) ->";
  | Volume (_, _) -> ()
  );
  line out "  return ()";

  doc out "Check that an S-Expression respects the syntax for a given \
    type-name (value, evaluation, or volume)";
  line out "let check_sexp type_name sexp =";
  line out "  let open Layout in";
  line out "  match type_name with";
  List.iter all_nodes (function
  | Enumeration (name, fields) -> ()
  | Record (name, _) ->
    line out "| %S -> begin try let _ = Record_%s.value_of_sexp sexp in Ok () \
              with e -> Error (`parse_sexp_error (sexp, e)) end" name name;
  | Function (name, _, _) ->
    line out "| %S -> begin try let _ = Function_%s.evaluation_of_sexp sexp in Ok () \
              with e -> Error (`parse_sexp_error (sexp, e)) end" name name;
  | Volume (name, _) -> 
    line out "| %S -> begin try let _ = File_system.content_of_sexp sexp in Ok () \
              with e -> Error (`parse_sexp_error (sexp, e)) end" name;
  );
  line out "| s -> Error (`parse_sexp_error (sexp,
                      Failure (sprintf \"Unknown type: %%s\" type_name)))";

  
  line out "end";


  ocaml_dependency_graph_module ~out all_nodes;
  
  ocaml_meta_module ~out dsl ~raw_dsl;
  ()

