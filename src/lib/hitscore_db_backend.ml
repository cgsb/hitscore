
open Hitscore_std

  
module Timestamp = struct 
  include Core.Std.Time
  let () = write_new_string_and_sexp_formats__read_only_new ()
end

module Bytea = struct

  let is_first_oct_digit c = c >= '0' && c <= '3'
  let is_oct_digit c = c >= '0' && c <= '7'
  let oct_val c = Char.to_int c - 0x30

  let is_hex_digit = function '0'..'9' | 'a'..'f' | 'A'..'F' -> true | _ -> false

  let hex_val c =
    let offset = match c with
      | '0'..'9' -> 0x30
      | 'a'..'f' -> 0x57
      | 'A'..'F' -> 0x37
      | _	       -> failwith "hex_val"
    in Char.to_int c - offset

  (* Deserialiser for the new 'hex' format introduced in PostgreSQL 9.0. *)
  let bytea_of_string_hex str =
    let len = String.length str in
    let buf = Buffer.create ((len-2)/2) in
    let i = ref 3 in
    while !i < len do
      let hi_nibble = str.[!i-1] in
      let lo_nibble = str.[!i] in
      i := !i+2;
      if is_hex_digit hi_nibble && is_hex_digit lo_nibble
      then begin
        let byte = ((hex_val hi_nibble) lsl 4) + (hex_val lo_nibble) in
        Buffer.add_char buf (Char.of_int_exn byte)
      end
    done;
    Buffer.contents buf

  (* Deserialiser for the old 'escape' format used in PostgreSQL < 9.0. *)
  let bytea_of_string_escape str =
    let len = String.length str in
    let buf = Buffer.create len in
    let i = ref 0 in
    while !i < len do
      let c = str.[!i] in
      if c = '\\' then (
        incr i;
        if !i < len && str.[!i] = '\\' then (
	  Buffer.add_char buf '\\';
	  incr i
        ) else if !i+2 < len &&
	    is_first_oct_digit str.[!i] &&
	    is_oct_digit str.[!i+1] &&
	    is_oct_digit str.[!i+2] then (
	      let byte = oct_val str.[!i] in
	      incr i;
	      let byte = (byte lsl 3) + oct_val str.[!i] in
	      incr i;
	      let byte = (byte lsl 3) + oct_val str.[!i] in
	      incr i;
	      Buffer.add_char buf (Char.of_int_exn byte)
	    )
      ) else (
        incr i;
        Buffer.add_char buf c
      )
    done;
    Buffer.contents buf

  (* PostgreSQL 9.0 introduced the new 'hex' format for binary data.
     We must therefore check whether the data begins with a magic sequence
     that identifies this new format and if so call the appropriate parser;
     if it doesn't, then we invoke the parser for the old 'escape' format.
  *)
  let bytea_of_string str =
    if String.is_prefix str ~prefix:"\\x"
    then bytea_of_string_hex str
    else bytea_of_string_escape str

  let string_of_bytea b =
    let len = String.length b in
    let buf = Buffer.create (len * 2) in
    for i = 0 to len - 1 do
      let c = b.[i] in
      let cc = Char.to_int c in
      if  cc < 0x20 || cc > 0x7e || c = '\'' || c = '"' || c = '\\'
      then
        Buffer.add_string buf (sprintf "\\\\%03o" cc) (* non-print -> \ooo *)
      else 
        Buffer.add_char buf c (* printable *)
    done;
    sprintf "E'%s'::bytea" (Buffer.contents buf)

      
  let to_db_input = string_of_bytea
  let of_db_output = bytea_of_string

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
     f_inserted: Timestamp.t;
     f_started: Timestamp.t option;
     f_completed: Timestamp.t option;
     f_status: status;
     f_sexp: Sexp.t; }

  type volume = {
    v_id: int;
    v_kind: string;
    v_sexp: Sexp.t;
    v_last_modified: Timestamp.t;
  }
      
  let escape_sql s = Bytea.to_db_input s
  let unescape s =
    (* eprintf "Unescaping: %S\n--> %S%!" s (Bytea.of_db_output s); *)
    Bytea.of_db_output s

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
      from record,volume,function ORDER BY 1 DESC LIMIT 1))"

  let create_record: t =
    sprintf
      "CREATE TABLE record (\n\
      \  id %s ,\n\
      \  type bytea NOT NULL,\n\
      \  created bytea NOT NULL,\n\
      \  last_modified bytea NOT NULL,\n\
      \  sexp bytea NOT NULL)" id_type
  let check_record: t =
    "SELECT (id, type, created, last_modified, sexp)\
     FROM record WHERE id = 0"

  let create_function: t =
    sprintf "CREATE TABLE function (\n\
      \  id %s,\n\
      \  type bytea NOT NULL,\n\
      \  result integer,\n\
      \  inserted bytea NOT NULL,\n\
      \  started bytea,\n\
      \  completed bytea,\n\
      \  status bytea NOT NULL,\n\
      \  sexp bytea NOT NULL)" id_type
  let check_function: t =
    "SELECT * FROM function WHERE id = 0"

  let create_volume: t =
    sprintf "CREATE TABLE volume (\n\
      \  id %s,\n\
      \  last_modified bytea NOT NULL,\n\
      \  kind bytea NOT NULL,\n\
      \  sexp bytea NOT NULL)"  id_type
  let check_volume: t =
    "SELECT (id, last_modified, kind, sexp) FROM volume WHERE id = 0"
        
  let last_modified_value ~record_name : t =
    sprintf 
      "select last_modified from record where type = %s ORDER BY 1 DESC LIMIT 1"
      (escape_sql record_name)
      
  let last_modified_evaluation ~function_name : t =
    sprintf "select greatest (inserted, started, completed)
             from function where type = %s ORDER BY 1 DESC LIMIT 1"
      (escape_sql function_name)

  let last_modified_volume () : t =
    "select last_modified from volume ORDER BY 1 DESC LIMIT 1"
      

  let add_value_sexp ~record_name sexp : t =
    let now = Timestamp.(to_string (now ()))  |! escape_sql in
    let str_sexp = Sexp.to_string_hum sexp |! escape_sql in
    let str_type = escape_sql record_name in
    sprintf "INSERT INTO record (type, created, last_modified, sexp)\n\
      \ VALUES (%s, %s, %s, %s) RETURNING id" str_type now now str_sexp

  let add_evaluation_sexp ~function_name ~status sexp : t =
    let now = Timestamp.(to_string (now ()))  |! escape_sql in
    let str_sexp = Sexp.to_string_hum sexp |! escape_sql in
    let str_type = escape_sql function_name in
    let status_str = escape_sql status in
    sprintf "INSERT INTO function \
      \ (type, inserted, status, sexp)\n\
      \ VALUES (%s, %s, %s, %s) RETURNING id"
      str_type now status_str str_sexp
    
  let add_volume_sexp ~kind sexp : t =
    let now = Timestamp.(to_string (now ()))  |! escape_sql in
    let str_sexp = Sexp.to_string_hum sexp |! escape_sql in
    let str_type = escape_sql kind in
    sprintf "INSERT INTO volume (last_modified, kind, sexp)\n\
      \ VALUES (%s, %s,%s) RETURNING id" now str_type str_sexp
      
  let insert_value v : t =
    let id            = v.r_id in
    let typ           = v.r_type |! escape_sql in
    let created       = Timestamp.to_string v.r_created   |! escape_sql     in
    let last_modified = Timestamp.to_string v.r_last_modified |! escape_sql in
    let sexp          = Sexp.to_string v.r_sexp |! escape_sql in
    sprintf "INSERT INTO record (id, type, created, last_modified, sexp)\n\
      \ VALUES (%d, %s, %s, %s, %s)"
      id typ created last_modified sexp

  let insert_evaluation v : t =
     let id = v.f_id in
     let typ = v.f_type |! escape_sql in
     let result = match v.f_result with None -> "null" | Some s -> Int.to_string s in
     let inserted = Timestamp.to_string v.f_inserted |! escape_sql in
     let started =
       Option.map v.f_started Timestamp.to_string |! Option.map ~f:escape_sql in
     let completed =
       Option.map v.f_completed Timestamp.to_string |! Option.map ~f:escape_sql in
     let status = v.f_status |! status_to_string |! escape_sql in
     let sexp = v.f_sexp |! Sexp.to_string_hum |! escape_sql in
     sprintf "INSERT INTO function \
        (id , type , result , inserted , started , completed , status , sexp)
        VALUES (%d, %s, %s, %s, %s, %s, %s, %s) "
       id typ result inserted 
       (Option.value started ~default:"NULL")
       (Option.value completed ~default:"NULL")
       status sexp 
     
  let insert_volume v : t =
    let str_sexp = Sexp.to_string_hum v.v_sexp |! escape_sql in
    let str_type = escape_sql v.v_kind in
    let last_modified = Timestamp.to_string v.v_last_modified |! escape_sql in
    sprintf "INSERT INTO volume (id, last_modified, kind, sexp)\n\
      \ VALUES (%d, %s, %s, %s)" v.v_id last_modified str_type str_sexp

  let single_id_of_result = function
    | [[ Some i ]] -> (try Some (Int.of_string i) with e -> None)
    | _ -> None

  let get_value_sexp ~record_name id =
    let str_type = escape_sql record_name in
    sprintf "SELECT * FROM record WHERE type = %s AND id = %d" str_type id

  let get_evaluation_sexp ~function_name id =
    let str_type = escape_sql function_name in
    sprintf "SELECT * FROM function WHERE type = %s AND id = %d" str_type id

  let get_volume_sexp id =
    sprintf "SELECT * FROM volume WHERE id = %d" id


  let update_value_sexp ~record_name id sexp =
    let now = Timestamp.(to_string (now ()))  |! escape_sql in
    let str_sexp = Sexp.to_string_hum sexp |! escape_sql in
    let str_type = escape_sql record_name in
    sprintf "UPDATE record SET last_modified = %s, sexp = %s \
             WHERE id = %d AND type = %s" now str_sexp id str_type
     
  let update_volume_sexp ~kind id sexp : t =
    let now = Timestamp.(to_string (now ()))  |! escape_sql in
    let str_sexp = Sexp.to_string_hum sexp |! escape_sql in
    let str_type = escape_sql kind in
    sprintf "UPDATE volume SET last_modified = %s, sexp = %s\n\
      \ WHERE id = %d AND kind = %s" now str_sexp id str_type

  let set_evaluation_started ~function_name id =
    let now = Timestamp.(to_string (now ()))  |! escape_sql in
    let str_type = escape_sql function_name in
    sprintf "UPDATE function SET started = %s, status = %s \
             WHERE id = %d AND type = %s"
      now (status_to_string `Started |! escape_sql) id str_type

  let set_evaluation_failed ~function_name id =
    let now = Timestamp.(to_string (now ()))  |! escape_sql in
    let str_type = escape_sql function_name in
    sprintf "UPDATE function SET completed = %s, status = %s \
             WHERE id = %d AND type = %s"
      now (status_to_string `Failed |! escape_sql) id str_type
  let set_evaluation_succeeded ~function_name id result =
    let now = Timestamp.(to_string (now ()))  |! escape_sql in
    let str_type = escape_sql function_name in
    sprintf "UPDATE function SET completed = %s, status = %s, result = %d \
             WHERE id = %d AND type = %s"
      now (status_to_string `Succeeded |! escape_sql) result id str_type
    
      
  let should_be_single = function
    | [one] -> Ok one
    | more -> Error (`result_not_unique more)
      
      
  let parse_timestamp s =
    Result.try_with (fun () -> Timestamp.of_string (unescape s))
  let parse_timestamp_opt o =
    Result.try_with (fun () ->
      Option.map o ~f:(fun s -> Timestamp.of_string (unescape s)))

  let parse_value sol =
    match sol with
    | [ Some i; Some t; Some creat; Some last; Some sexp ] ->
      Result.(
        try_with (fun () -> Int.of_string i) >>= fun r_id ->
        parse_timestamp creat >>= fun r_created ->
        parse_timestamp last >>= fun r_last_modified ->
        try_with (fun () -> Sexp.of_string (unescape sexp)) >>= fun r_sexp ->
        return {r_id; r_type = t; r_created; r_last_modified; r_sexp})
      |! Result.map_error ~f:(fun e -> `parse_value_error (sol, e))
    | _ -> Error (`parse_value_error (sol, Failure "Wrong format"))

  let parse_evaluation sol =
    match sol with
    | [ Some id; Some typ; result_opt;
        Some inserted; started_opt; completed_opt;
        Some status; Some sexp ] ->
      Result.(
        try_with (fun () -> Int.of_string id) >>= fun f_id ->
        return typ >>= fun f_type ->
        try_with (fun () ->
          Option.map ~f:(fun s -> unescape s |! Int.of_string) result_opt
        ) >>= fun f_result ->
        parse_timestamp inserted >>= fun f_inserted ->
        parse_timestamp_opt started_opt >>= fun f_started ->
        parse_timestamp_opt completed_opt >>= fun f_completed ->
        try_with (fun () -> status_of_string_exn (unescape status)) >>= fun f_status ->
        try_with (fun () -> Sexp.of_string (unescape sexp)) >>= fun f_sexp ->
        return {f_id; f_type; f_result;
                f_inserted; f_started; f_completed; f_status; f_sexp; })
      |! Result.map_error ~f:(fun e -> `parse_evaluation_error (sol, e))
    | _ -> Error (`parse_evaluation_error (sol, Failure "Wrong format"))

  let parse_volume sol =
    match sol with
    | [ Some i; Some lm; Some k; Some sexp ] ->
      Result.(
        try_with (fun () -> Int.of_string i) >>= fun v_id ->
        try_with (fun () -> Sexp.of_string (unescape sexp)) >>= fun v_sexp ->
        parse_timestamp lm >>= fun v_last_modified ->
        return {v_id; v_last_modified; v_kind = unescape k; v_sexp})
      |! Result.map_error ~f:(fun e -> `parse_value_error (sol, e))
    | _ -> Error (`parse_volume_error (sol, Failure "Wrong format"))

  let get_all_values_sexp ~record_name =
    let str_type = escape_sql record_name in
    sprintf "SELECT * FROM record WHERE type = %s" str_type
  let get_all_evaluations_sexp ~function_name =
    let str_type = escape_sql function_name in
    sprintf "SELECT * FROM function WHERE type = %s" str_type
  let get_all_volumes_sexp () = "SELECT * FROM volume"

  let delete_value_sexp ~record_name id =
    let str_type = escape_sql record_name in
    sprintf "DELETE FROM record WHERE type = %s AND id = %d" str_type id
  let delete_evaluation_sexp ~function_name id =
    let str_type = escape_sql function_name in
    sprintf "DELETE FROM function WHERE type = %s AND id = %d" str_type id
  let delete_volume_sexp id =
    sprintf "DELETE FROM volume WHERE id = %d" id

  let identify_value_type id =
    sprintf "SELECT type FROM record WHERE id = %d" id
  let identify_evaluation_type id =
    sprintf "SELECT type FROM function WHERE id = %d" id
  let identify_volume_kind id =
    sprintf "SELECT kind FROM volume WHERE id = %d" id


end
  

module type BACKEND = sig
  type db_handle
  type error = [
  | `exn of exn
  | `connection of exn
  | `disconnection of exn
  | `query of (string * exn)
  ] with sexp_of
  type result_item = string option list with sexp
  type result = result_item list with sexp

  val connect :
    ?host:string ->
    ?port:int ->
    ?database:string ->
    ?user:string ->
    ?password:string ->
    ?log:(string -> unit) ->
    unit ->
    (db_handle,
     [> `db_backend_error of [> error ] ]) Sequme_flow.t

  val disconnect :
    dbh:db_handle ->
    (unit, [> `db_backend_error of [> `disconnection of exn ] ])
      Sequme_flow.t
  val reconnect :
    dbh:db_handle ->
    (db_handle,
     [> `db_backend_error of
         [> `connection of exn | `disconnection of exn ] ])
      Sequme_flow.t

  val query :
    dbh:db_handle ->
    Sql_query.t ->
    (result,
     [> `db_backend_error of [> `query of Sql_query.t * exn ] ]) Sequme_flow.t
  val wipe_out : dbh:db_handle -> (unit, 'a) Sequme_flow.t
  val check_db :
    dbh:db_handle ->
    (unit,
     [> `db_backend_error of
         [> `connection of exn
         | `disconnection of exn
         | `query of Sql_query.t * exn ] ])
      Sequme_flow.t
      
end
  
module Backend : BACKEND = struct
    
  type db_handle = {
    mutable connection: int PG.t; (* ensure it does not come from the
                                     syntax extension *)
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
  ] with sexp_of
  type result_item = string option list with sexp
  type result = result_item list with sexp
    
  let connect ?host ?port ?database ?user ?password ?log () :
      (db_handle, [> `db_backend_error of [> error ] ]) Sequme_flow.t =
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
    disconnect dbh
    >>< begin function
    | Ok () -> return ()
    | Error _ ->
      (* what we want here is to reconnect, so we ignore disconnection
         errors and hope for the best *)
      return ()
    end
    >>= fun () ->
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
    while_sequential Sql_query.wipe_out (fun q ->
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


module Util = struct

  let make_collection_cache ~allowed_age ~maximal_age collection =
    let cache = ref None in
    let reget () =
      collection#all
      >>= fun all ->
      cache := Some (Time.now (), all);
      return all
    in
    begin fun () ->
      match !cache with
      | Some (birthdate, v)
          when Time.(to_float (now ()) -. to_float birthdate) <= allowed_age ->
        return v
      | Some (birthdate, v)
          when Time.(to_float (now ()) -. to_float birthdate) >= maximal_age ->
        reget ()
      | Some (birthdate, v) ->
        collection#last_modified >>= fun t ->
        if birthdate < t
        then begin
          reget ()
        end
        else return v
      | None ->
        reget ()
    end

end
