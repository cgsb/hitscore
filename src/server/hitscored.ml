open Hitscored_std

type ttt = Plain | SSL of Ssl.socket
type lwt_ssl_socket = Lwt_unix.file_descr * ttt

let print_user sslsock =
  begin match snd (Obj.magic sslsock : lwt_ssl_socket) with
  | Plain ->
    dbg "PLAIN SOCKET ??????"
  | SSL s ->
    begin
      try
        let cert = Ssl.get_certificate s in
        let user = Ssl.get_subject cert in
        dbg "User: %s" user
      with
      | Ssl.Certificate_error ->
        dbg "There is no user (anonymous)"
    end
  end

let main ?ca_cert ~cert_key () =
  debug "Hello world!" >>= fun () ->
  let with_client_authentication =
    Option.map ca_cert (fun c -> `with_CA_certificate c) in
  Flow_net.ssl_server_context ?with_client_authentication cert_key
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
    print_user ssl_accepted >>= fun () ->
    let inchan = Lwt_ssl.in_channel_of_descr ssl_accepted in
    dbg "Reading..." >>= fun () ->
    wrap_io Lwt_io.(read ~count:2048) inchan
    >>= fun stuff_read ->
    dbg "Read: %S" stuff_read >>= fun () ->
    return ())
  >>= fun (_ : unit list) ->
  dbg "This Is The End." >>= fun () ->
  return ()


let () =
  Ssl.init ();
  global_log_app_name := "SERVER";
  let main_m =
    match Array.to_list Sys.argv |! List.tl_exn with
    | [ cert_key ] -> main ~cert_key () 
    | [ cert_key; ca_cert ] -> main ~cert_key ~ca_cert ()
    | _ -> failwithf "usage: %s cert_key.pem  [ca.crt]" Sys.argv.(0) ()
  in
  match Lwt_main.run main_m with
  | Ok () -> ()
  | Error e ->
    eprintf "%s: There were uncaught errors given to Lwt_main.run\n%!"
      !global_log_app_name;
    print_error e
      
