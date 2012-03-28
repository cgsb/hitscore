open Hitscored_std

  
let main () =
  debug "Hello world!" >>= fun () ->
  let ssl_context = Ssl.(create_context TLSv1 Server_context) in
  let socket =
    Lwt_unix.(
      let fd = socket PF_INET SOCK_STREAM 6 in
      bind fd (ADDR_INET (Unix.inet_addr_any, 2000));
      listen fd 2000;
      fd) in
  debug "Accepting (unix)" >>= fun () ->
  wrap_io Lwt_unix.accept socket >>= fun accepted ->
  debug "Accepted (unix)" >>= fun () ->
  Flow_net.ssl_accept (fst accepted) ssl_context >>= fun ssl_accepted ->
  debug "Accepted" >>= fun () ->
  Pervasives.ignore socket;
  return ()


let () =
  Ssl.init ();
  global_log_app_name := "SERVER";
  match Lwt_main.run (main ()) with
  | Ok () -> ()
  | Error (`io_exn e) ->
    eprintf "An I/O exn was not caught: %s -- Global: %s\n%!"
      (Exn.to_string e) (Ssl.get_error_string ())
  | Error e ->
    eprintf "There were uncaught errors given to Lwt_main.run\n%!"
  
