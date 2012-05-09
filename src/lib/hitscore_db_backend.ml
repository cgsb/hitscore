
open Hitscore_std

  
module Timestamp = struct 
  include Core.Std.Time
  let () = write_new_string_and_sexp_formats__read_both ()
end

module Sql_query = struct
  type t = string
  type result = string option list list

  type status = [ `Failed | `Inserted | `Started | `Succeeded ] 
  let status_to_string : status -> string = function
    | `Started -> "Started"
    | `Inserted -> "Inserted"
    | `Failed -> "Failed"
    | `Succeeded -> "Succeeded"

  let status_of_string_exn: string -> status = function
    | "Started" -> `Started
    | "Inserted" -> `Inserted
    | "Failed" -> `Failed
    | "Succeeded" -> `Succeeded
    | s -> raise (Failure "Sql_query.status_of_string_exn")

  let status_of_string s =
    try Core.Std.Ok (status_of_string_exn s) with e -> Core.Std.Error s

  type record_value =
    { r_id: int;
      r_type: string;
      r_created: Timestamp.t;
      r_last_modified: Timestamp.t;
      r_sexp: Sexp.t }
      
  type function_evaluation =
    {f_id:int;
     f_type: string;
     f_result: int option;
     f_recomputable: bool;
     f_recompute_penalty: float;
     f_inserted: Timestamp.t;
     f_started: Timestamp.t option;
     f_completed: Timestamp.t option;
     f_status: status;
     f_sexp: Sexp.t; }

  type volume = {
    v_id: int;
    v_kind: string;
    v_sexp: Sexp.t;
  }
      
  let escape_sql = 
    String.Escaping.escape ~escapeworthy:['\''] ~escape_char:'\''
    |! Staged.unstage

  let wipe_out: t list = [
    "drop table record;";
    "drop table function;";
    "drop table volume";
    "drop sequence id_sequence;";
  ]
        
  let create_sequence: t =
    "CREATE SEQUENCE id_sequence"
  let id_type =
    "integer DEFAULT nextval('id_sequence') PRIMARY KEY NOT NULL" 
  let check_sequence : t = "select last_value from id_sequence"
  let update_sequence : t =
    "select setval('id_sequence', \
     (select greatest (record.id, function.id, volume.id) \
      from record,volume,function limit 1))"

  let create_record: t =
    sprintf
      "CREATE TABLE record (\n\
      \  id %s ,\n\
      \  type text NOT NULL,\n\
      \  created text NOT NULL,\n\
      \  last_modified text NOT NULL,\n\
      \  sexp text NOT NULL)" id_type
  let check_record: t =
    "SELECT (id, type, created, last_modified, sexp)\
     FROM record WHERE id = 0"

  let create_function: t =
    sprintf "CREATE TABLE function (\n\
      \  id %s,\n\
      \  type text NOT NULL,\n\
      \  result integer,\n\
      \  recomputable bool NOT NULL,\n\
      \  recompute_penalty real NOT NULL,\n\
      \  inserted text NOT NULL,\n\
      \  started text,\n\
      \  completed text,\n\
      \  status text NOT NULL,\n\
      \  sexp text NOT NULL)" id_type
  let check_function: t =
    "SELECT * FROM function WHERE id = 0"

  let create_volume: t =
    sprintf "CREATE TABLE volume (\n\
      \  id %s,\n\
      \  kind text NOT NULL,\n\
      \  sexp text NOT NULL)"  id_type
  let check_volume: t =
    "SELECT (id, kind, sexp) FROM volume WHERE id = 0"
        
  let add_value_sexp ~record_name sexp : t =
    let now = Timestamp.(to_string (now ()))  |! escape_sql in
    let str_sexp = Sexp.to_string_hum sexp |! escape_sql in
    let str_type = escape_sql record_name in
    sprintf "INSERT INTO record (type, created, last_modified, sexp)\n\
      \ VALUES ('%s', '%s', '%s', '%s') RETURNING id" str_type now now str_sexp

  let add_evaluation_sexp
      ~function_name ~recomputable ~recompute_penalty ~status sexp : t =
    let now = Timestamp.(to_string (now ()))  |! escape_sql in
    let str_sexp = Sexp.to_string_hum sexp |! escape_sql in
    let str_type = escape_sql function_name in
    let status_str = escape_sql status in
    sprintf "INSERT INTO function \
      \ (type, recomputable, recompute_penalty, inserted, status, sexp)\n\
      \ VALUES ('%s', %b, %f, '%s', '%s', '%s') RETURNING id"
      str_type recomputable recompute_penalty now status_str str_sexp
    
  let add_volume_sexp ~kind sexp : t =
    let str_sexp = Sexp.to_string_hum sexp |! escape_sql in
    let str_type = escape_sql kind in
    sprintf "INSERT INTO volume (kind, sexp)\n\
      \ VALUES ('%s','%s') RETURNING id" str_type str_sexp
      
  let insert_value v : t =
    let id            = v.r_id in
    let typ           = v.r_type |! escape_sql in
    let created       = Timestamp.to_string v.r_created   |! escape_sql     in
    let last_modified = Timestamp.to_string v.r_last_modified |! escape_sql in
    let sexp          = Sexp.to_string v.r_sexp |! escape_sql in
    sprintf "INSERT INTO record (id, type, created, last_modified, sexp)\n\
      \ VALUES (%d, '%s', '%s', '%s', '%s')"
      id typ created last_modified sexp

  let insert_evaluation v : t =
     let id = v.f_id in
     let typ = v.f_type |! escape_sql in
     let result = match v.f_result with None -> "null" | Some s -> Int.to_string s in
     let recomputable = v.f_recomputable in
     let recompute_penalty = v.f_recompute_penalty in
     let inserted = Timestamp.to_string v.f_inserted |! escape_sql in
     let started =
       Option.map v.f_started Timestamp.to_string |! Option.map ~f:escape_sql in
     let completed =
       Option.map v.f_completed Timestamp.to_string |! Option.map ~f:escape_sql in
     let status = v.f_status |! status_to_string |! escape_sql in
     let sexp = v.f_sexp |! Sexp.to_string_hum |! escape_sql in
     sprintf "INSERT INTO function \
        (id , type , result , recomputable , recompute_penalty ,
        inserted , started , completed , status , sexp)
        VALUES (%d, '%s', %s, %b, %f, '%s', %s, %s, '%s', '%s') "
       id typ result recomputable recompute_penalty inserted 
       (Option.value_map started ~default:"NULL" ~f:(sprintf "'%s'"))
       (Option.value_map completed ~default:"NULL" ~f:(sprintf "'%s'"))
       status sexp 
     
  let insert_volume v : t =
    let str_sexp = Sexp.to_string_hum v.v_sexp |! escape_sql in
    let str_type = escape_sql v.v_kind in
    sprintf "INSERT INTO volume (id, kind, sexp)\n\
      \ VALUES (%d, '%s','%s')" v.v_id str_type str_sexp

  let single_id_of_result = function
    | [[ Some i ]] -> (try Some (Int.of_string i) with e -> None)
    | _ -> None

  let get_value_sexp ~record_name id =
    let str_type = escape_sql record_name in
    sprintf "SELECT * FROM record WHERE type = '%s' AND id = %d" str_type id

  let get_evaluation_sexp ~function_name id =
    let str_type = escape_sql function_name in
    sprintf "SELECT * FROM function WHERE type = '%s' AND id = %d" str_type id

  let get_volume_sexp id =
    sprintf "SELECT * FROM volume WHERE id = %d" id

  let should_be_single = function
    | [one] -> Ok one
    | more -> Error (`result_not_unique more)
      
  let parse_value sol =
    match sol with
    | [ Some i; Some t; Some creat; Some last; Some sexp ] ->
      Result.(
        try_with (fun () -> Int.of_string i) >>= fun r_id ->
        try_with (fun () -> Timestamp.of_string creat) >>= fun r_created ->
        try_with (fun () -> Timestamp.of_string last) >>= fun r_last_modified ->
        try_with (fun () -> Sexp.of_string sexp) >>= fun r_sexp ->
        return {r_id; r_type = t; r_created; r_last_modified; r_sexp})
      |! Result.map_error ~f:(fun e -> `parse_value_error (sol, e))
    | _ -> Error (`parse_value_error (sol, Failure "Wrong format"))

  let parse_evaluation sol =
    let parse_bool = function
      | "f" | "false" | "False" | "FALSE" -> false
      | "t" | "true" | "True" | "TRUE" -> true
      | s -> failwithf "parse_bool: %S" s () in
    match sol with
    | [ Some id; Some typ; result_opt;
        Some recomputable; Some recompute_penalty;
        Some inserted; started_opt; completed_opt;
        Some status; Some sexp ] ->
      Result.(
        try_with (fun () -> Int.of_string id) >>= fun f_id ->
        return typ >>= fun f_type ->
        try_with (fun () -> Option.map ~f:Int.of_string result_opt)
                            >>= fun f_result ->
        try_with (fun () -> parse_bool recomputable) >>= fun f_recomputable ->
        try_with (fun () -> Float.of_string recompute_penalty)
                            >>= fun f_recompute_penalty ->
        try_with (fun () -> Timestamp.of_string inserted) >>= fun f_inserted ->
        try_with (fun () -> Option.map ~f:Timestamp.of_string started_opt)
                            >>= fun f_started ->
        try_with (fun () -> Option.map ~f:Timestamp.of_string completed_opt)
                            >>= fun f_completed ->
        try_with (fun () -> status_of_string_exn status) >>= fun f_status ->
        try_with (fun () -> Sexp.of_string sexp) >>= fun f_sexp ->
        return {f_id; f_type; f_result; f_recomputable; f_recompute_penalty;
                f_inserted; f_started; f_completed; f_status; f_sexp; })
      |! Result.map_error ~f:(fun e -> `parse_evaluation_error (sol, e))
    | _ -> Error (`parse_evaluation_error (sol, Failure "Wrong format"))

  let parse_volume sol =
    match sol with
    | [ Some i; Some k; Some sexp ] ->
      Result.(
        try_with (fun () -> Int.of_string i) >>= fun v_id ->
        try_with (fun () -> Sexp.of_string sexp) >>= fun v_sexp ->
        return {v_id; v_kind = k; v_sexp})
      |! Result.map_error ~f:(fun e -> `parse_value_error (sol, e))
    | _ -> Error (`parse_volume_error (sol, Failure "Wrong format"))

  let get_all_values_sexp ~record_name =
    let str_type = escape_sql record_name in
    sprintf "SELECT * FROM record WHERE type = '%s'" str_type
  let get_all_evaluations_sexp ~function_name =
    let str_type = escape_sql function_name in
    sprintf "SELECT * FROM function WHERE type = '%s'" str_type
  let get_all_volumes_sexp () = "SELECT * FROM volume"

  let delete_value_sexp ~record_name id =
    let str_type = escape_sql record_name in
    sprintf "DELETE FROM record WHERE type = '%s' AND id = %d" str_type id
  let delete_evaluation_sexp ~function_name id =
    let str_type = escape_sql function_name in
    sprintf "DELETE FROM function WHERE type = '%s' AND id = %d" str_type id
  let delete_volume_sexp id =
    sprintf "DELETE FROM volume WHERE id = %d" id



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
        
  let wipe_out ~dbh =
    of_list_sequential Sql_query.wipe_out (fun q ->
      bind_on_error (query ~dbh q) (fun e -> return []))
    >>= fun _ ->
    return ()

  let check_db ~(dbh: db_handle) =
    double_bind (query ~dbh Sql_query.check_sequence)
      ~error:(fun e ->
        logf dbh "SEQUENCE FAILS → CREATING …\n%!";
        reconnect ~dbh >>= fun dbh ->
        query ~dbh Sql_query.create_sequence)
      ~ok:return
    >>= fun _ ->
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
