exception Parse_error of string
type dsl_type =
    Bool
  | Timestamp
  | Int
  | Real
  | String
  | Option of dsl_type
  | Array of dsl_type
  | Record_name of string
  | Enumeration_name of string
  | Function_name of string
type typed_value = string * dsl_type
type dsl_runtime_type =
    Enumeration of string * string list
  | Record of string * typed_value list
  | Function of string * typed_value list * string
type dsl_runtime_description = {
  nodes : dsl_runtime_type list;
  types : dsl_type list;
}

val parse_sexp : Sexplib.Sexp.t -> dsl_runtime_description
val parse_str : string -> dsl_runtime_description

val to_db : dsl_runtime_description -> Psql.table list

val digraph :
  dsl_runtime_description -> ?name:string -> (string -> unit) -> unit

val ocaml_code : dsl_runtime_description -> (string -> unit) -> unit

val testing_inserts :
  dsl_runtime_description -> int -> (string -> 'a) -> unit
