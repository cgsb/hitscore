#camlp4o
#require "sexplib.syntax"
#require "sexplib"
  
type t = {a: int} with sexp;;
  
let _ = Lwt_io.printf " bouh !\n" |! Lwt_main.run

let pf fmt = ksprintf (fun s -> wrap_io (Lwt_io.printf "%s\n%!") s) fmt

(* Backend *)
let backend_last_modified =
  "select greatest (record.last_modified) \
    from record ORDER BY 1 DESC LIMIT 1"
    
let broker =
  with_database ~configuration (fun ~dbh ->
    Backend.query ~dbh backend_last_modified
    >>= fun lls ->
    pf "lm: %s" (String.concat ~sep:"\n" (List.concat (List.map lls (List.map ~f:(Option.value ~default:"—")))))
    >>= fun () ->
    
    return ()
      
  ) |! ok_exn

  
