open Hitscored_std
  
let main () =
  debug "Starting" >>= fun () ->
  system_command "_build/src/server/hitscored &" >>= fun ()-> 
  Flow_net.sleep 2. >>= fun () ->
  debug "Started server" >>= fun () ->
  let ssl_context = Ssl.(create_context TLSv1 Client_context) in
  let socket =
    Lwt_unix.(
      try
      let fd = socket PF_INET SOCK_STREAM 0 in
      fd
      with
      | Unix.Unix_error (e, s, a) as ex ->
        eprintf "Unix.Unix_error: %s %s %s\n%!" (Unix.error_message e) s a;
        raise ex
    ) in
  wrap_io (Lwt_unix.connect socket) Unix.(ADDR_INET (inet_addr_loopback, 2000))
  >>= fun () -> 
  debug "Connected (unix)" >>= fun () ->
  Flow_net.ssl_connect socket ssl_context >>= fun socket ->
  debug "Connected (ssl)" >>= fun () ->
  Flow_net.ssl_shutdown socket >>= fun () ->
  debug "Disconnected (ssl)" >>= fun () ->
  return ()


let () =
  Ssl.init ();
  global_log_app_name := "TEST";
  let main_m =
    double_bind (main ()) ~ok:return
      ~error:(fun e ->
        system_command "killall hitscored" >>= fun () ->
        error e) in
  match Lwt_main.run main_m with
  | Ok () -> ()
  | Error e ->
    eprintf "There were uncaught errors given to Lwt_main.run\n%!"
  
