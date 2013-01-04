open Hitscore_std

  
type t

val to_string: t -> string
val of_string: string -> (t, [> `sexp_parsing_error of exn]) Result.t

(** Register an “uploaded” file: [add_upload ~dbh ~person_id:42 "filename.pdf"]
    (the function does not care about duplicates, it's a “set”).
*)
val add_upload :
  dbh:Hitscore_db_backend.Backend.db_handle ->
  person_id:int ->
  filename:string ->
  (unit,
   [> `user_data of
       string *
         [> `Layout of Hitscore_layout.Layout.error_location * Hitscore_layout.Layout.error_cause
         | `sexp_parsing_error of exn ] ]) Hitscore_std.t

(** Unregister an “uploaded” file:
    [remove_upload ~dbh ~person_id:42 "filename.pdf"]
    (if the upload does no exists, it does not fail).
*)
val remove_upload :
  dbh:Hitscore_db_backend.Backend.db_handle ->
  person_id:int ->
  filename:string ->
  (unit,
   [> `user_data of
       string *
         [> `Layout of Hitscore_layout.Layout.error_location * Hitscore_layout.Layout.error_cause
         | `sexp_parsing_error of exn ] ]) Hitscore_std.t

(** Check if a filename as been registered for this user. *) 
val find_upload :
  dbh:Hitscore_db_backend.Backend.db_handle ->
  person_id:int ->
  filename:string ->
  (bool,
   [> `user_data of
       string *
         [> `Layout of Hitscore_layout.Layout.error_location * Hitscore_layout.Layout.error_cause
         | `sexp_parsing_error of exn ] ]) Hitscore_std.t
