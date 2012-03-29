open Hitscored_std

let dbg fmt = ksprintf debug fmt
  
let main () =
  debug "Hello world!" >>= fun () ->
  Flow_net.ssl_context (`server ("cert.pem", "privkey-unsec.pem"))
  >>= fun ssl_context ->
  Flow_net.server_socket ~port:2000 >>= fun socket ->
  debug "Accepting (unix)" >>= fun () ->
  wrap_io (Lwt_unix.accept_n socket) 10
  >>= fun (accepted_list, potential_exn) ->
  dbg "Accepted %d (unix)%s" (List.length accepted_list)
    (Option.value_map ~default:"" potential_exn
       ~f:(fun e -> sprintf ", Exn: %s" (Exn.to_string e)))
  >>= fun () ->
  of_list_sequential accepted_list (fun accepted ->
    Flow_net.ssl_accept (fst accepted) ssl_context >>= fun ssl_accepted ->
    debug "Accepted (SSL)" >>= fun () ->
    Pervasives.ignore socket;
    return ())
  >>= fun (_ : unit list) ->
  return ()


let () =
  Ssl.init ();
  global_log_app_name := "SERVER";
  match Lwt_main.run (main ()) with
  | Ok () -> ()
  | Error e ->
    eprintf "%s: There were uncaught errors given to Lwt_main.run\n%!"
      !global_log_app_name;
    print_error e
      
