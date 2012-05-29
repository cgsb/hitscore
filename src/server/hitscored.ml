open Hitscored_std

    
let main ?ca_cert ~cert_key () =
  dbg "Hello world!" >>= fun () ->
  let with_client_authentication =
    Option.map ca_cert (fun c -> `CA_certificate c) in
  Flow_net.ssl_server_context ?with_client_authentication cert_key
  >>= fun ssl_context ->
  Flow_net.server_socket ~port:2000 >>= fun socket ->

  of_option ca_cert (fun ca_cert ->
    Certificate_authority.get_index ca_cert
    >>= fun ca_index ->
    return (fun c ->
      dbg "check_client_certificate:\n  Issuer: %s\n  Subject: %s"
        (Ssl.get_issuer c) (Ssl.get_subject c) >>= fun () ->
      let subj = (Ssl.get_subject c) in
      let cn = Certificate_authority.login_of_subject subj in
      begin match Certificate_authority.find_name ca_index subj with
      | None -> return `certificate_not_found
      | Some (`valid (time, serial)) when Time.(time >. now ()) ->
        dbg "TIME: %s, CN: %s" Time.(to_string time)
          Option.(value ~default:"DID NOT PARSE" cn) >>= fun () ->
        return `ok
      | Some (`valid (time, _)) | Some (`expired (time, _)) ->
        return `expired
      | Some (`revoked _) -> return `revoked
      end))
  >>= fun check_client_certificate ->

  Flow_net.ssl_accept_loop ?check_client_certificate ssl_context socket
    (fun client_socket client_kind ->
      let inchan = Lwt_ssl.in_channel_of_descr client_socket in
      let ouchan = Lwt_ssl.out_channel_of_descr client_socket in
      begin match client_kind with
      | `invalid_client `wrong_certificate ->
        dbg "The client has a wrong certificate"
      | `invalid_client (`expired_certificate _) ->
        dbg "The client has an expired certificate"
      | `invalid_client (`revoked_certificate _) ->
        dbg "The client has a revoked certificate"
      | `invalid_client (`certificate_not_found _) ->
        dbg "The client has a not-found certificate"
      | `anonymous_client ->
        dbg "Reading..." >>= fun () ->
        Message.recv_client inchan
        >>= fun msg ->
        begin match msg with
        | `hello ->
          dbg "Got hello" >>= fun () ->
          Message.server_send ouchan (`hello `anonymous)
        end
      | `valid_client cert ->
        let login = Certificate_authority.login_of_cert cert in
        dbg "Reading... from %s" (Option.value ~default:"NOT-NAMED" login)
        >>= fun () ->
        Message.recv_client inchan
        >>= fun msg ->
        begin match msg with
        | `hello ->
          dbg "Got hello" >>= fun () ->
          Message.server_send ouchan (`hello (`authenticated []))
        end
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
      
