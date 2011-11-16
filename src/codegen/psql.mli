exception Parse_error of string
type basic_type =
    Timestamp
  | Identifier
  | Text
  | Pointer of string * string
  | Integer
  | Real
  | Bool
type property = Array | Not_null | Unique
type properties = property list
type table = {
  name : string;
  fields : (string * basic_type * properties) list;
}

type db = table list

val init_db_postgres: table list -> (string -> unit) -> unit

val clear_db_postgres: table list -> (string -> unit) -> unit

val verify : table list -> (string -> unit) -> unit
  
val digraph : table list -> ?name:string -> (string -> unit) -> unit
  
val parse_sexp : Sexplib.Sexp.t -> table list
  
val parse_str : string -> table list
