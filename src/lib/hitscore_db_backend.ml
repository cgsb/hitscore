
open Hitscore_std

  
module Timestamp = struct 
  include Core.Std.Time
  let () = write_new_string_and_sexp_formats__read_both ()
end

module Sql_query = struct
  type t = string
  type result = string option list list
    
  let escape_sql = 
    String.Escaping.escape ~escapeworthy:['\''] ~escape_char:'\''
    |! Staged.unstage

  let create_record: t =
    "CREATE TABLE record (\n\
      \  id serial PRIMARY KEY NOT NULL,\n\
      \  type text NOT NULL,\n\
      \  created text NOT NULL,\n\
      \  last_modified text NOT NULL,\n\
      \  sexp text NOT NULL)"
  let check_record: t =
    "SELECT (id, type, created, last_modified, sexp)\
     FROM record WHERE id = 0"

  let create_function: t =
    "CREATE TABLE function (\n\
      \  id serial PRIMARY KEY NOT NULL,\n\
      \  type text NOT NULL,\n\
      \  result integer,\n\
      \  recomputable bool NOT NULL,\n\
      \  recompute_penalty real NOT NULL,\n\
      \  inserted text NOT NULL,\n\
      \  started text,\n\
      \  completed text,\n\
      \  status text NOT NULL,\n\
      \  sexp text NOT NULL)"
  let check_function: t =
    "SELECT * FROM function WHERE id = 0"

  let create_volume: t =
    "CREATE TABLE volume (\n\
      \  id serial PRIMARY KEY NOT NULL,\n\
      \  kind text NOT NULL,\n\
      \  sexp text NOT NULL)"    
  let check_volume: t =
    "SELECT (id, kind, sexp) FROM volume WHERE id = 0"
        
  let add_value_sexp ~record_name sexp : t =
    let now = Timestamp.(to_string (now ()))  |! escape_sql in
    let str_sexp = Sexp.to_string_hum sexp |! escape_sql in
    let str_type = escape_sql record_name in
    sprintf "INSERT INTO record (type, created, last_modified, sexp)\n\
      \ VALUES ('%s', '%s', '%s', '%s') RETURNING id" str_type now now str_sexp

  let single_id_of_result = function
    | [[ Some i ]] -> (try Some (Int.of_string i) with e -> None)
    | _ -> None

end
  
module Make (Flow: Sequme_flow_monad.FLOW_MONAD) = struct

  module PG = PGOCaml_generic.Make(Flow.IO)
    (* with type 'a monad = 'a Flow.IO.t *)
  module Flow = Flow
  open Flow
    
  type db_handle = {
    mutable connection: (string, bool) Hashtbl.t PG.t;
    host: string option;
    port: int option;
    database: string option;
    user: string option;
    password: string option;
    log: (string -> unit) option
  }
  type error = [
  | `exn of exn
  | `connection of exn
  | `disconnection of exn
  | `query of (string * exn)
  ]
  type result = string option list list
    
  let connect ?host ?port ?database ?user ?password ?log () :
      (db_handle, [> `db_backend_error of [> error ] ]) monad =
    bind_on_error
      (catch_io (PG.connect
                   ?host ?port ?database ?user ?password) ())
      (fun e -> error (`db_backend_error (`connection e)))
    >>= fun connection ->
    return {log; connection; host; port; database; user; password;}

  let disconnect ~(dbh: db_handle) = 
    bind_on_error (catch_io PG.close dbh.connection)
      (fun e -> error (`db_backend_error (`disconnection e)))

  let reconnect ~dbh =
    disconnect dbh >>= fun () ->
    let { host; port; database; user; password; _ } = dbh in
    bind_on_error
      (catch_io (PG.connect
                   ?host ?port ?database ?user ?password) ())
      (fun e -> error (`db_backend_error (`connection e)))
    >>= fun connection ->
    dbh.connection <- connection;
    return dbh
    
  let logf dbh fmt =
    ksprintf (fun s ->
      match dbh.log with
      | Some f -> f s
      | None -> ()) ("DB:" ^^ fmt)
      
  let query ~(dbh: db_handle) (query:Sql_query.t) =
    let name = sprintf "%f" (Random.float 100.) in
    let work_m = 
      wrap_io (PG.prepare ~name ~query dbh.connection) ()
      >>= fun () ->
      wrap_io (PG.execute ~name ~params:[] dbh.connection) ()
      >>= fun result ->
      wrap_io (PG.close_statement dbh.connection ~name) ()
      >>= fun () ->
      return (result: result)
    in
    double_bind work_m
      ~ok:(fun r ->
        logf dbh "QUERY: %S : SUCCESS" query;
        return r)
      ~error:(function
      | `io_exn e ->
        logf dbh "QUERY: %S : ERROR: %s" query (Exn.to_string e);
        error (`db_backend_error (`query (query, e))))
        

  let check_db ~(dbh: db_handle) =
    double_bind (query ~dbh Sql_query.check_record)
      ~error:(fun e ->
        logf dbh "TABLE 'record' FAILS → CREATING …\n%!";
        reconnect ~dbh >>= fun dbh ->
        query ~dbh Sql_query.create_record)
      ~ok:return
    >>= fun _ ->
    double_bind (query ~dbh Sql_query.check_function)
      ~error:(fun e ->
        logf dbh "TABLE 'function' FAILS → CREATING …\n%!";
        reconnect ~dbh >>= fun dbh ->
        query ~dbh Sql_query.create_function)
      ~ok:return
    >>= fun _ ->
    double_bind (query ~dbh Sql_query.check_volume)
      ~error:(fun e ->
        logf dbh "TABLE 'volume' FAILS → CREATING …\n%!";
        reconnect ~dbh >>= fun dbh ->
        query ~dbh Sql_query.create_volume)
      ~ok:return
    >>= fun _ ->
    return ()
end
