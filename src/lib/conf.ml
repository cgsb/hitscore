open Batteries_uni;; open Printf
module StringMap = Map.StringMap

exception Invalid of string

type t = string StringMap.t

let read sequme_root =
  Filename.concat (Filename.concat sequme_root "etc") "sequme.conf"
  |> File.lines_of
  |> Enum.map (flip String.split "=" |- (fun (x,y) -> String.strip x, Sequme.Util.unquote (String.strip y)))
  |> Enum.fold (flip (uncurry StringMap.add)) StringMap.empty
  |> StringMap.add "sequme_root" sequme_root

type dbconf = {
  db_engine : string;
  db_name : string;
  db_user : string;
  db_password : string;
  db_host : string;
  db_port : string
}

let get_dbconf t =
  let find key map =
    try StringMap.find key map
    with Not_found -> raise (Invalid (sprintf "attribute %s not found in given configuration" key))
  in
  let engine = find "database.engine" t in
  let name = find "database.name" t in
  match engine with
    | "sqlite" | "sqlite3" ->
        let name = 
          if Filename.is_relative name then
            Filename.concat (Filename.concat (find "sequme_root" t) "etc") name
          else
            name
        in
        {
          db_engine="sqlite";
          db_name=name;
          db_user="";
          db_password="";
          db_host="";
          db_port=""
        }
    | _ -> raise (Invalid (sprintf "%s: unsupported database engine" engine))

let sqlite_exec conf ?cb stmt =
  let dbconf = get_dbconf conf in
  let open Sqlite3 in
  let db = match dbconf.db_engine with
    | "sqlite" -> db_open dbconf.db_name
    | _ -> Invalid (sprintf "%s: configuration does not include sqlite database" (StringMap.find "sequme_root" conf)) |> raise
  in
  match cb with
    | None ->
        (match exec db stmt with
          | Rc.OK -> ()
          | x -> Invalid (sprintf "sqlite error: %s" (Rc.to_string x)) |> raise
        )
    | Some cb ->
        (match exec db ~cb stmt with
          | Rc.OK -> ()
          | x -> Invalid (sprintf "sqlite error: %s" (Rc.to_string x)) |> raise
        )

let log conf =
  sprintf "INSERT INTO log (\"msg\") VALUES ('%s')" |- sqlite_exec conf
