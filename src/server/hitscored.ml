open Hitscored_std



    
let main ?ca_cert ~cert_key () =
  debug "Hello world!" >>= fun () ->
  let with_client_authentication =
    Option.map ca_cert (fun c -> `CA_certificate c) in
  Flow_net.ssl_server_context ?with_client_authentication cert_key
  >>= fun ssl_context ->
  Flow_net.server_socket ~port:2000 >>= fun socket ->

  let check_client_certificate =
    Option.map ca_cert (fun _ ->
      fun c ->
        dbg "check_client_certificate:\n  Issuer: %s\n  Subject: %s"
          (Ssl.get_issuer c) (Ssl.get_subject c)
      >>= fun () ->
        return true)
  in
  Flow_net.ssl_accept_loop ?check_client_certificate ssl_context socket
    (fun client_socket client_kind ->
      begin match client_kind with
      | `invalid_client `wrong_certificate ->
        dbg "The client has a wrong certificate"
      | `invalid_client (`expired_certificate _) ->
        dbg "The client has an expired certificate"
      | `anonymous_client
      | `valid_client _ ->
        let inchan = Lwt_ssl.in_channel_of_descr client_socket in
        dbg "Reading..." >>= fun () ->
        wrap_io Lwt_io.(read ~count:2048) inchan
        >>= fun stuff_read ->
        dbg "Read: %S" stuff_read >>= fun () ->
        return ()
      end)

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
      
