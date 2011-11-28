exception Parse_error of string
type dsl_runtime_description

val parse_sexp : Sexplib.Sexp.t -> dsl_runtime_description
val parse_str : string -> dsl_runtime_description

val to_db : dsl_runtime_description -> Hitscoregen_psql.table list

val digraph :
  dsl_runtime_description -> ?name:string -> (string -> unit) -> unit

val ocaml_code : dsl_runtime_description -> (string -> unit) -> unit

val testing_inserts :
  dsl_runtime_description -> int -> (string -> 'a) -> unit
