open Hitscore_std

(** (filename, original-filename, creation-date) *) 
type upload_info = (string * string * Time.t) with sexp

(** Register an “uploaded” file: [add_upload ~dbh ~person_id:42 "filename.pdf"]
    (the function does not care about duplicates, it's a “set”).
*)
val add_upload :
  dbh:Hitscore_db_backend.Backend.db_handle ->
  person_id:int ->
  filename:string ->
  original:string ->
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
  (upload_info option,
   [> `user_data of
       string *
         [> `Layout of Hitscore_layout.Layout.error_location * Hitscore_layout.Layout.error_cause
         | `sexp_parsing_error of exn ] ]) Hitscore_std.t

(** Get all uploads for the user [person_id] *)
val all_uploads:
  dbh:Hitscore_db_backend.Backend.db_handle ->
  person_id:int ->
  (upload_info list,
   [> `user_data of
       string *
         [> `Layout of Hitscore_layout.Layout.error_location * Hitscore_layout.Layout.error_cause
         | `sexp_parsing_error of exn ] ]) Hitscore_std.t
