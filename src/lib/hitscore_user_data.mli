open Hitscore_std

  
type t

val to_string: t -> string
val of_string: string -> (t, [> `sexp_parsing_error of exn]) Result.t

(** Register an “uploaded” file: [add_upload ~dbh ~person_id:42 "filename.pdf"]
*)
val add_upload :
  dbh:Hitscore_db_backend.Backend.db_handle ->
  person_id:int ->
  string ->
  (unit,
   [> `user_data of
       string *
         [> `Layout of Hitscore_layout.Layout.error_location * Hitscore_layout.Layout.error_cause
         | `sexp_parsing_error of exn ] ]) Hitscore_std.t
