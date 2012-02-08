open Core.Std
let (|>) x f = f x


type fashion = [`implementation|`interface]

module Psql = Hitscoregen_psql
module Sx = Sexplib.Sexp

open Hitscoregen_layout_dsl

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
  | Record_name "g_file" -> sprintf "file_pointer"
  | Record_name "g_volume" -> sprintf "volume_pointer"
  | Record_name s -> sprintf "Record_%s.pointer" s
  | Volume_name s -> sprintf "File_system.volume_pointer"

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
  line out ("Printf.ksprintf Result_IO.IO.log_error \"%s\" " ^^ fmt ^^ ";") metafmt

let line_mli ~fashion out fmt =
  match fashion with
  | `interface -> line out fmt
  | `implementation -> line ignore fmt

let line_ml ~fashion out fmt =
  match fashion with
  | `interface -> line ignore fmt
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
  | `interface -> ()

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
      
  let define ?fashion t out = 
    hide ?fashion out (fun out ->
      line out "exception Local_exn_%s of %s" 
        t.name (String.concat ~sep:" * " t.types);
    )
      
  let throw t out what =
    line out "(Result_IO.IO.fail (Local_exn_%s %s))" t.name what
  
  let transform_local t =
    let vals = 
      (String.concat ~sep:", " (List.mapi t.types (fun i x -> sprintf "x%d" i)))
    in
    sprintf 
      "`pg_exn (Local_exn_%s (%s)) -> \
        `layout_inconsistency (`%s, `%s (%s))"
      t.name vals (String.lowercase t.module_name) t.name vals

  let transform_global t =
    let vals = 
      (String.concat ~sep:", " (List.mapi t.types (fun i x -> sprintf "x%d" i))) in
    sprintf 
      "`pg_exn (%s.Local_exn_%s (%s)) -> \
        `layout_inconsistency (`%s, `%s (%s))"
      t.module_name t.name vals (String.lowercase t.module_name) t.name vals
      
  let poly_type_local tl = 
    sprintf "`layout_inconsistency of [> `%s] * [> %s]"
      (String.concat ~sep:"\n | `"
         (List.dedup (List.map tl (fun t -> String.lowercase t.module_name))))      
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

let ocaml_poly_result_io out left right =
  line out "(%s, [> %s ]) Result_IO.monad" 
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

let pgocaml_to_result_io ?(fashion=`implementation) out ?transform_exceptions f =
  match fashion with
  | `implementation ->
    line out "let _exn_version () =";
    f out;
    line out "in";
    begin match transform_exceptions with
    | None ->
      line out "catch_pg_exn _exn_version ()";
    | Some l ->
      line out "Result_IO.bind_on_error (catch_pg_exn _exn_version ())";
      line out "  (fun e -> Result_IO.error \
                    (match e with %s | `pg_exn e -> `pg_exn e))"
        (String.concat ~sep:"\n   | " l);
    end
  | `interface -> ()

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
  line out "       | l -> Result_IO.IO.fail (Failure \
                           \"Postgres is itself inconsistent\")))";
  ()

let ocaml_start_module ~out ~name = function
  | `implementation -> raw out "module %s = struct\n" name
  | `interface -> raw out "module %s : sig\n" name

let ocaml_sexped_type ~out ~name ?param ~fashion value = 
  match fashion with
  | `implementation -> 
    line out "type %s%s = %s with sexp"
      (Option.value_map param ~default:"" ~f:(sprintf "%s "))
      name value
  | `interface -> 
    line out "type %s%s = %s"
      (Option.value_map param ~default:"" ~f:(sprintf "%s "))
      name value;
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
  raw
    (ocaml_sexped_type ~out ~name:"t" ~fashion)
    "[%s]"
    (List.map fields (sprintf "`%s") |> String.concat ~sep:" | ");

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

let ocaml_record_module ~out ~fashion name fields = 
  let out_ml = 
    match fashion with | `implementation -> out | `interface -> ignore in
  let out_mli = 
    match fashion with | `implementation -> ignore | `interface -> out in

  ocaml_start_module ~out ~name:(sprintf "Record_%s" name) fashion;

  doc out "Type [pointer] should be used like a private type, access to \
          the [id] field is there for hackability/emergency purposes.";
  ocaml_sexped_type ~out ~name:"pointer" ~fashion "{ id: int32 }";

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
  ocaml_poly_result_io out "pointer" [
    (OCaml_hiden_exception.poly_type_local [wrong_add_value]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_result_io out_ml ~transform_exceptions:[
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
  ocaml_poly_result_io out "pointer list" ["`pg_exn of exn";];
  line out_ml " =";
  pgocaml_to_result_io out_ml (fun out ->
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
  ocaml_poly_result_io out "t" [
    (OCaml_hiden_exception.poly_type_local [wrong_select_one]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_result_io out_ml ~transform_exceptions:[
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
  doc out "Deletes a values with its internal id.";
  line out_mli "val _delete_value_by_id: dbh:db_handle -> pointer \
                 -> unit PGOCaml.monad";
  line out_ml "let _delete_value_by_id %s (p:pointer) =" pgocaml_db_handle_arg;
  line out_ml "  let id = p.id in";
  line out_ml "  PGSQL (dbh)";
  line out_ml "    \"DELETE FROM %s WHERE g_id = $id\"\n" name;


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
  ocaml_poly_result_io out "pointer" [
    (OCaml_hiden_exception.poly_type_local [wrong_cache_insert]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_result_io out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_cache_insert;
  ] (fun out ->
    line out "(insert_value_exn ~dbh v)";
  );

  line out "\nend (* %s *)\n" name;
  ()

let ocaml_function_module ~out ~fashion name args result =
  let out_ml = 
    match fashion with | `implementation -> out | `interface -> ignore in
  let out_mli = 
    match fashion with | `implementation -> ignore | `interface -> out in

  ocaml_start_module ~out ~name:(sprintf "Function_%s" name) fashion;

  ocaml_sexped_type ~out ~fashion ~param:"'capabilities" 
    ~name:"pointer" "{ id: int32 }";

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
  ocaml_poly_result_io out "[ `can_start | `can_complete ] pointer" [
    (OCaml_hiden_exception.poly_type_local [wrong_add]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_result_io out_ml ~transform_exceptions:[
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
  ocaml_poly_result_io out "[`can_complete] pointer" ["`pg_exn of exn";];
  line out_ml " =";
  pgocaml_to_result_io out_ml (fun out ->
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
  ocaml_poly_result_io out "[ `can_get_result] pointer" ["`pg_exn of exn";];
  line out_ml " =";
  pgocaml_to_result_io out_ml (fun out ->
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
  ocaml_poly_result_io out "[ `can_nothing ] pointer" ["`pg_exn of exn";];
  line out_ml " =";
  pgocaml_to_result_io out_ml (fun out ->
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
  ocaml_poly_result_io out "'any t" [
    (OCaml_hiden_exception.poly_type_local [wrong_select_one]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_result_io out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_select_one;
  ] (fun out ->
    line out "get_evaluation_exn ~dbh p";
  );

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
      ocaml_poly_result_io out (sprintf "%s pointer list" phamtom) ["`pg_exn of exn"];
      line out_ml " =";
      pgocaml_to_result_io out_ml (fun out ->
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
  ocaml_poly_result_io out "[`can_nothing] pointer list" [ "`pg_exn of exn"];
  line out_ml " =";
  pgocaml_to_result_io out_ml (fun out ->
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
  ocaml_poly_result_io out "[`can_nothing] pointer" [
    (OCaml_hiden_exception.poly_type_local [wrong_insert_cache]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_result_io out_ml ~transform_exceptions:[
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
    | `interface -> ignore in
  let out_mli = 
    match fashion with
    | `implementation -> ignore
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
  ocaml_poly_result_io out "value list" ["`pg_exn of exn";];
  line out_ml " =";
  (* FUTURE: (OCaml_hiden_exception.(poly_type_global (all_gets ()))) *)
  pgocaml_to_result_io out_ml 
    ~transform_exceptions:
    (OCaml_hiden_exception.(List.map ~f:transform_global (all_gets ())))
    (fun out ->
      line out "get_all_values_exn ~dbh";
    );

  doc out "Get all the evaluations in the database.";
  line out_mli "val get_all_evaluations: dbh:db_handle ->";
  line out_ml "let get_all_evaluations %s:" pgocaml_db_handle_arg;
  ocaml_poly_result_io out "[`can_nothing] evaluation list" ["`pg_exn of exn";];
  line out_ml " =";
  (* FUTURE: (OCaml_hiden_exception.(poly_type_global (all_gets ()))) *)
  pgocaml_to_result_io out_ml 
    ~transform_exceptions:
    (OCaml_hiden_exception.(List.map ~f:transform_global (all_gets ())))
    (fun out ->
      line out "get_all_evaluations_exn ~dbh";
    );

  ()

    
let ocaml_file_system_module ~fashion ~out dsl =

  let out_ml = 
    match fashion with
    | `implementation -> out
    | `interface -> ignore in
  let out_mli = 
    match fashion with
    | `implementation -> ignore
    | `interface -> out in

  let id_field = "g_id", Int in
  let file_fields = [
    "g_name", String;
    "g_type", Enumeration_name "file_type";
    "g_content", Array Int; (* A bit hackish, duck-typing. *)
  ] in
  let volume_fields = [
    "g_toplevel",  String;
    "g_hr_tag", Option String;
    "g_content", Array Int;
  ] in
  let file_ocaml_field f = "f_" ^ (String.chop_prefix_exn f ~prefix:"g_") in
  let volume_ocaml_field f = "v_" ^ (String.chop_prefix_exn f ~prefix:"g_") in

  ocaml_start_module ~out ~name:"File_system" fashion;

  ocaml_sexped_type ~out ~name:"volume_pointer" ~fashion "{ id : int32 }";
  ocaml_sexped_type ~out ~name:"file_pointer" ~fashion "{ inode: int32 }";
  line out "type tree = ";
  line out "  | File of string * Enumeration_file_type.t";
  line out "  | Directory of string * Enumeration_file_type.t * tree list";
  line out "  | Opaque of string * Enumeration_file_type.t";

  doc out "Pointers to files";
  line
    (ocaml_sexped_type ~out ~name:"file_entry" ~fashion)
    "{%s}"
    (List.map (id_field :: file_fields) (fun (s, t) ->
      sprintf "  %s: %s;" (file_ocaml_field s) (ocaml_type t))
      |! String.concat ~sep:"");

  doc out "Pointers to volumes";
  line
    (ocaml_sexped_type ~out ~name:"volume_entry" ~fashion)
    "{%s}"
    (List.map (id_field :: volume_fields) (fun (s, t) ->
      sprintf "  %s: %s;" (volume_ocaml_field s) (ocaml_type t))
        |! String.concat ~sep:"");

  doc out "A whole volume is the volume entry and a \
              list of files.";
  ocaml_sexped_type ~out ~name:"volume" ~fashion
    "{\n    volume_entry: volume_entry; \n    \
            files: file_entry list}";


  let wrong_insert = 
    OCaml_hiden_exception.make "File_system" "add_did_not_return_one"
      [ "string"; "int32 list" ] in
  let on_not_one_id ~out ~table_name  ~returned  =
    OCaml_hiden_exception.throw wrong_insert out
      (sprintf "(%S, %s)" table_name returned) in

  OCaml_hiden_exception.define ~fashion wrong_insert out;

  hide ~fashion out (fun out ->
    line out "let rec _add_tree_exn %s = function" pgocaml_db_handle_arg;
    line out "  | File (name, t) | Opaque (name, t) -> begin";
    line out "    let type_str = Enumeration_file_type.to_string t in";
    line out "    let empty_array = [| |] in";
    pgocaml_add_to_database_exn "g_file"
      (List.map file_fields fst)
      [ "$name"; "$type_str" ; "$empty_array" ]
      ~out ~on_not_one_id ~id:"inode";
    line out "    end";
    line out "  | Directory (name, t, pl) -> begin";
    line out "    let type_str = Enumeration_file_type.to_string t in";
    line out "    let added_file_list_monad = map_s ~f:(_add_tree_exn ~dbh) pl in";
    line out "    pg_bind (added_file_list_monad) (fun file_list -> \n";
    line out "       let pg_inodes = array_map (list_to_array file_list) \
                       (fun { inode } ->  inode) in";
    pgocaml_add_to_database_exn "g_file" 
      (List.map file_fields fst)
      [ "$name"; "$type_str" ; "$pg_inodes" ]
      ~out ~on_not_one_id ~id:"inode";
    line out "  )  end";
  );

  doc out "Register a new file, directory (with its contents), or opaque \
        directory in the DB.";
  line out_ml "let add_volume %s \
                ~(kind:Enumeration_volume_kind.t) \
                ?(hr_tag: string option) \
                ~(files:tree list) : " 
    pgocaml_db_handle_arg;
  line out_mli "val add_volume: dbh: db_handle -> \
                 kind:Enumeration_volume_kind.t -> \
                ?hr_tag: string -> \
                files:tree list -> ";
  ocaml_poly_result_io out "volume_pointer" [
    (OCaml_hiden_exception.poly_type_local [wrong_insert]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_result_io out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_insert;
  ] (fun out ->
    line out "let added_file_list_monad = map_s ~f:(_add_tree_exn ~dbh) files in";
    line out "pg_bind (added_file_list_monad) (fun file_list -> \n";
    line out "     let pg_inodes = array_map (list_to_array file_list) \n\
                       (fun { inode } ->  inode) in";
    line out " let toplevel = match kind with";
    List.iter dsl.nodes (function
      | Volume (n, t) -> line out "    | `%s -> %S" n t
      | _ -> ());
    line out " in";
    pgocaml_add_to_database_exn "g_volume" 
      (List.map volume_fields fst)
      [ "$toplevel" ; "$?hr_tag"; "$pg_inodes" ]
      ~out ~on_not_one_id ~id:"id";
    line out ")";
  );

  doc out "Add a tree to an existing volume.";
  line out_mli "val add_tree_to_volume: \
              dbh:db_handle -> volume_pointer -> tree list -> ";
  ocaml_poly_result_io out_mli "unit" [
    (OCaml_hiden_exception.poly_type_local [wrong_insert]);
    "`pg_exn of exn";
  ];
  line out_ml "let add_tree_to_volume %s (volume: volume_pointer) tree =" 
    pgocaml_db_handle_arg;
  pgocaml_to_result_io out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_insert;
  ] (fun out ->
    line out "let added_file_list_monad = map_s ~f:(_add_tree_exn ~dbh) tree in";
    line out "pg_bind (added_file_list_monad) (fun file_list -> \n";
    line out "     let pg_inodes = array_map (list_to_array file_list) \n\
                       (fun { inode } ->  inode) in";
    line out "     let vol_id = volume.id in";
    line out "     PGSQL (dbh)";
    line out "       %S)"
      "UPDATE g_volume \
      SET g_content = ARRAY_CAT(g_content, $pg_inodes) \
      WHERE g_id = $vol_id";
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
            volume_pointer list PGOCaml.monad =" pgocaml_db_handle_arg;
    pgocaml_do_get_all_ids ~out ~id:"id" "g_volume";
  );
  
  doc out "Get all the volumes.";
  line out_ml "let get_all %s: " pgocaml_db_handle_arg;
  line out_mli "val get_all: dbh:db_handle ->";
  ocaml_poly_result_io out "volume_pointer list" ["`pg_exn of exn";];
  line out_ml " =";
  pgocaml_to_result_io out_ml (fun out ->
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
    doc out "Retrieve a file from the DB.";
    line out "let get_file_exn (file: file_pointer) %s: \
                  file_entry PGOCaml.monad =\n" pgocaml_db_handle_arg;
    line out "  let tuple_m =";
    pgocaml_select_from_one_by_id_exn 
      ~out ~on_not_one_id ~get_id:"file.inode" "g_file";
    line out "  in";
    line out " pg_bind tuple_m (fun (%s,%s) -> " (fst id_field)
      (List.map file_fields ~f:fst |! String.concat ~sep:", ");
    line out "    pg_return {";
    List.iter (id_field :: file_fields) (fun (v, t) ->
      line out "      %s = %s;" (file_ocaml_field v) (convert_pgocaml_type (v, t)));
    line out "    })\n";
  );

  hide out_ml (fun out ->     
    raw out "let get_volume_entry_exn (volume_pointer: volume_pointer) %s: \
                  volume_entry PGOCaml.monad =\n" pgocaml_db_handle_arg;
    line out "let tuple_m =";
    pgocaml_select_from_one_by_id_exn 
      ~out ~on_not_one_id ~get_id:"volume_pointer.id" "g_volume";
    line out "in";
    line out "pg_bind tuple_m (fun (%s,%s) -> " (fst id_field)
      (List.map volume_fields ~f:fst |! String.concat ~sep:", ");
    line out "    pg_return {";
    List.iter (id_field :: volume_fields) (fun (v, t) ->
      line out "      %s = %s;" (volume_ocaml_field v)
        (convert_pgocaml_type (v, t)));
    line out "    })\n";
  );

  doc out "Retrieve the \"entry\" part of a volume from the DB.";
  line out_ml "let get_volume_entry (volume: volume_pointer) %s:" 
    pgocaml_db_handle_arg;
  line out_mli "val get_volume_entry: volume_pointer -> dbh:db_handle ->";
  ocaml_poly_result_io out "volume_entry" [
    (OCaml_hiden_exception.poly_type_local [wrong_cache_select]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_result_io out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_cache_select;
  ] (fun out ->
    line out "get_volume_entry_exn ~dbh volume";
  );

  hide out_ml (fun out ->
    doc out "[get_volume_exn] is for internal use only.";
    line out "let get_volume_exn %s (volume: volume_pointer) : \
              volume Result_IO.IO.t ="
      pgocaml_db_handle_arg;
    line out "  let entry_m = get_volume_entry_exn ~dbh volume in";
    line out "  pg_bind entry_m (fun volume_entry ->";
    line out "    let files = ref ([] : file_entry list) in";
    line out "    let contents = volume_entry.v_content in";
    line out "    let rec get_contents arr =";
    line out "      let contents = array_to_list arr in";
    line out "      let f inode = get_file_exn ~dbh { inode } in";
    line out "      pg_bind (map_s ~f contents) (fun fs ->";
    line out "        files := list_append fs !files;";
    line out "        let todo = list_map fs (fun t -> t.f_content) in";
    line out "        pg_bind (map_s ~f:get_contents todo)";
    line out "          (fun _ -> pg_return ())) in";
    line out "    pg_bind (map_s ~f:get_contents [ contents ])";
    line out "      (fun _ -> pg_return {volume_entry; files = !files}))";
  );

  doc out "Retrieve a \"whole\" volume.";
  line out_ml "let get_volume %s (volume_pointer: volume_pointer) :"
    pgocaml_db_handle_arg;
  line out_mli "val get_volume: dbh:db_handle -> volume_pointer ->";
  ocaml_poly_result_io out "volume" [
    (OCaml_hiden_exception.poly_type_local [wrong_cache_select]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_result_io out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_cache_select;
  ] (fun out ->
    line out "get_volume_exn ~dbh volume_pointer";
  );
  hide out_ml (fun out -> line out "exception Inode_not_found of int32");

  doc out "Get a list of trees `known' for a given volume.";
  line out_ml "let volume_trees (vol : volume) : \
            (tree list, [> `inconsistency_inode_not_found of int32 \
                         | `cannot_recognize_file_type of string ]) \
             Core.Std.Result.t =";
  line out_mli "val volume_trees: volume -> \
            (tree list, [> `inconsistency_inode_not_found of int32 \
                         | `cannot_recognize_file_type of string ]) \
             Core.Std.Result.t";
  line out_ml "  let files = vol.files in";
  line out_ml "  let find_file i = \
            match list_find files (fun t -> t.f_id = i) with";
  line out_ml "    | Some f -> f";
  line out_ml "     | None -> raise (Inode_not_found i)";
  line out_ml " in\n";
  line out_ml "  let rec go_through_cache inode =";
  line out_ml "    match find_file inode with";
  (* For the future: when the file types are customizable, 
     this will have to change.  *)
  line out_ml "    | {f_name; f_type = `opaque; f_content = [||]} -> \
                    Opaque (f_name, `opaque)";
  line out_ml "    | {f_name; f_type = `directory; f_content} ->";
  line out_ml "      let l = array_to_list f_content in\n \
                    Directory (f_name, `directory, \
                      (list_map l go_through_cache))";
  line out_ml "    | {f_name; f_type = `blob; f_content = [||]} ->";
  line out_ml "        File (f_name, `blob)";
  line out_ml "    | {f_name; _} -> raise (Inode_not_found inode)";
  line out_ml "  in";
  line out_ml "  let vol_content = list_map vol.files (fun f -> f.f_id) in";
  line out_ml "  begin try Core.Std.Ok (list_map vol_content go_through_cache)";
  line out_ml "  with\n  | Inode_not_found inode ->";
  line out_ml "    Core.Std.Error (`inconsistency_inode_not_found inode)";
  line out_ml "  | Enumeration_file_type.Of_string_error s ->";
  line out_ml "    Core.Std.Error (`cannot_recognize_file_type s)";
  line out_ml "  end";

  doc out "{3 Low-level Access}";

  let wrong_cache_insert = 
    OCaml_hiden_exception.make ~in_loads:true "File_system" 
      "insert_tuple_did_not_return_one_id"
      [ "string"; "int32 list" ] in
  let on_not_one_id ~out ~table_name  ~returned  =
    OCaml_hiden_exception.throw wrong_cache_insert out
      (sprintf "(%S, %s)" table_name returned) in
  OCaml_hiden_exception.define wrong_cache_insert out_ml;

  doc out "Load a volume in the database (More {b Unsafe!}
          than for records and functions: if one fails the ones already
          successful won't be cleaned-up).";
  let insert_cache_exn out =
    line out "  let inserted_files_monad = map_s ~f:(fun f ->";

    List.iter (id_field :: file_fields) ~f:(fun (s, t) ->
      line out "  let %s = f.%s in" s (file_ocaml_field s);
      line out "  %s" (let_in_typed_value (s,t))
    );
    let all_field_names = List.map (id_field :: file_fields) fst in
    let all_field_names_prefixed =
      let prefix (n, t) =
        match type_is_option t with 
          `yes -> sprintf "$?%s" n | `no -> sprintf "$%s" n in 
      List.map (id_field :: file_fields) prefix in
    pgocaml_insert_in_db_with_id_exn
      ~out ~on_not_one_id "g_file" all_field_names all_field_names_prefixed;

    line out "      )  vol.files in";
    line out "  pg_bind inserted_files_monad (fun _ ->";

    List.iter (id_field :: volume_fields) ~f:(fun (s, t) ->
      line out "  let %s = vol.volume_entry.%s in" s (volume_ocaml_field s);
      line out "  %s" (let_in_typed_value (s,t))
    );
    let all_field_names = List.map (id_field :: volume_fields) fst in
    let all_field_names_prefixed =
      let prefix (n, t) =
        match type_is_option t with 
          `yes -> sprintf "$?%s" n | `no -> sprintf "$%s" n in 
      List.map (id_field :: volume_fields) prefix in
    pgocaml_insert_in_db_with_id_exn
      ~out ~on_not_one_id "g_volume" all_field_names all_field_names_prefixed;

    line out ")";
  in
  line out_ml "let insert_volume %s (vol: volume):" pgocaml_db_handle_arg;
  line out_mli "val insert_volume: dbh:db_handle -> volume ->";
  ocaml_poly_result_io out "volume_pointer" [
    (OCaml_hiden_exception.poly_type_local [wrong_cache_insert]);
    "`pg_exn of exn";
  ];
  line out_ml " =";
  pgocaml_to_result_io out_ml ~transform_exceptions:[
    OCaml_hiden_exception.transform_local wrong_cache_insert;
  ] insert_cache_exn;
  
  hide out_ml (fun out ->
    line out "let insert_volume_exn %s (vol: volume): \
            volume_pointer PGOCaml.monad = " pgocaml_db_handle_arg;
    insert_cache_exn out;
  );

  hide out_ml (fun out ->
    line out "let delete_volume_exn %s (vol: volume) =" pgocaml_db_handle_arg;
    line out "  let deleted_files = map_s vol.files ~f:(fun f ->";
    line out "    let inode = f.f_id in";
    line out "    PGSQL(dbh)";
    line out "      %S) in"  "DELETE FROM g_file WHERE g_id = $inode";
    line out "   pg_bind deleted_files (fun _ ->";
    line out "     let id = vol.volume_entry.v_id in";
    line out "    PGSQL(dbh)";
    line out "      %S)" "DELETE FROM g_volume WHERE g_id = $id";
  );
  doc out "Delete a cached volume with its contents ({b UNSAFE:} does \
            not verify if there is a link to it).";
  line out_ml "let delete_volume %s (vol: volume) =" pgocaml_db_handle_arg;
  line out_mli "val delete_volume: dbh:db_handle -> volume ->";
  ocaml_poly_result_io out_mli "unit" [
    "`pg_exn of exn";
  ];
  pgocaml_to_result_io out_ml ~transform_exceptions:[] (fun out ->
    line out "  delete_volume_exn ~dbh vol";
  );


  let unix_sep = "/" in
  doc out "{3 Unix paths }";
  doc out "Create a Unix directory path for a volume-entry (relative
            to a `root').";
  line out_mli "val entry_unix_path: volume_entry -> string";
  line out_ml  "let entry_unix_path (ve: volume_entry): string =";
  line out_ml "  Printf.sprintf \"%%s/%%09ld%%s\" ";
  line out_ml "    ve.v_toplevel ve.v_id";
  line out_ml "    (option_value_map ~default:\"\" ve.v_hr_tag ~f:((^) \"_\"))";

  doc out "Convert a bunch of trees to a list of Unix paths.";
  line out_mli "val trees_to_unix_paths: tree list -> string list";
  line out_ml "let trees_to_unix_paths trees =";
  line out_ml "  let paths = ref [] in";
  line out_ml "  let rec descent parent = function";
  line out_ml "  | File (n, _) -> paths := (parent ^ n) :: !paths";
  line out_ml "  | Opaque (n, _) -> paths := (parent ^ n ^ %S) :: !paths" unix_sep;
  line out_ml "  | Directory (n, _, l) -> list_iter l (descent (parent ^ n ^ %S))"
    unix_sep;
  line out_ml "  in";
  line out_ml "  list_iter trees (descent \"\");";
  line out_ml "  !paths";

  line out "end (* File_system *)";
  ()


let ocaml_dump_and_reload ~out ~fashion dsl =
  let out_ml = 
    match fashion with
    | `implementation -> out
    | `interface -> ignore in
  let out_mli = 
    match fashion with
    | `implementation -> ignore
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
  ocaml_poly_result_io out "dump" [
    (OCaml_hiden_exception.(poly_type_global (all_dumps ())));
    "`pg_exn of exn";
  ];
  line out_ml " =";
  
  pgocaml_to_result_io out_ml
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
    ocaml_poly_result_io out "unit" [
    "`wrong_version of string * string";
    (OCaml_hiden_exception.(poly_type_global (all_loads ())));
    "`pg_exn of exn";
  ];
  line out_ml " =";

  line out_ml "  if dump.version <> Hitscore_conf_values.version then (";
  line out_ml "    Result_IO.error (`wrong_version (dump.version, \
                          Hitscore_conf_values.version))";
  line out_ml "  ) else";
  pgocaml_to_result_io out_ml 
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
      line out_mli " (%s list, [> `pg_exn of exn]) Result_IO.monad"
        (ocaml_type record_type);
      pgocaml_to_result_io out_ml ~transform_exceptions:[] (fun out ->
        line out "  %s_exn ~dbh %s"
          fun_name (String.concat ~sep:" " (List.map fields (sprintf "%s")));
      );

  );
  line out "end (* module Search *)";
  ()



let toplevel_generic_types out =
  doc out "PG'OCaml's connection handle.";
  raw out "type db_handle = (string, bool) Hashtbl.t PGOCaml.t\n\n";
  doc out "Access rights on [Function_*.{t,cache}].";
  raw out "type function_capabilities= [\n\
                \  | `can_nothing\n  | `can_start\n  | `can_complete\n\
                | `can_get_result\n]\n";
  ()

let file_system_section (fashion: fashion) out dsl = 

  let volume_kinds = 
    (List.filter_map dsl.nodes 
       (function | Volume (n, _) -> Some n | _ -> None)) in
  let file_types =  ["blob"; "directory"; "opaque"] in

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


let ocaml_interface dsl output_string =
  let out = output_string in
  doc out "Autogenerated module.";
  line out "module type LAYOUT = sig";
  line out "module Result_IO : Hitscore_interfaces.RESULT_IO";
  line out "module PGOCaml: PGOCaml_generic.PGOCAML_GENERIC ";
  line out "module Timestamp: sig type t = Core.Std.Time.t end";

  toplevel_generic_types out;
  file_system_section `interface out dsl;

  functions_and_records ~fashion:`interface dsl ~out;

  ocaml_search_module ~out ~fashion:`interface dsl;

  ocaml_toplevel_values_and_types ~out ~fashion:`interface dsl;

  ocaml_dump_and_reload ~out ~fashion:`interface dsl;


  line out "end";
  ()

let ocaml_code ?(with_interface=true) dsl output_string =
  let out = output_string in
  doc out "Autogenerated module.";
  doc out "To make the DB-accesses thread-agnostic we need an \
        \"Outside word model\".";
  raw out "\
module Make \
(Result_IO : Hitscore_interfaces.RESULT_IO)";
  if with_interface then (
    line out ": Hitscore_layout_interface.LAYOUT\n";
    line out "  with module Result_IO = Result_IO\n";
    line out "  with type 'a PGOCaml.monad = 'a Result_IO.IO.t";
  );
  raw out "\
 = struct\n\
  module PGOCaml = PGOCaml_generic.Make(Result_IO.IO)
  module Result_IO = Result_IO
module Timestamp = struct 
  include Core.Std.Time
  let () = use_new_string_and_sexp_formats ()
 end
";

  toplevel_generic_types out;
  hide out (fun out ->
    raw out "\
open Sexplib.Conv\n\
(* Legacy stuff: *)\n\
let err = Result_IO.IO.log_error\n\n\
let map_s = Result_IO.IO.map_sequential\n\
let catch_pg_exn f x =\n\
 Result_IO.(bind_on_error (catch_io f x)\n\
  (fun e -> error (`pg_exn e)))\n\n\
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
  let fashion = `implementation in
  file_system_section `implementation out dsl; 

  functions_and_records ~fashion ~out dsl;

  ocaml_search_module ~out ~fashion dsl;
  ocaml_toplevel_values_and_types ~out ~fashion dsl;
  ocaml_dump_and_reload ~out ~fashion dsl;
  line out "end (* Make *)";
  ()

