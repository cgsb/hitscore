
include Core.Std

  
let global_log_app_name = ref ">>"

let format_message s =
  let l = String.split s ~on:'\n' in
  List.map l (sprintf "%s: %s\n" !global_log_app_name)
  |! String.concat ~sep:"" 
      
module Lwt_config = struct
  include Lwt
  include Lwt_chan
  let map_sequential l ~f = Lwt_list.map_s f l
  let log_error s =
    let str = format_message s in
    output_string Lwt_io.stderr str >>= fun () -> flush Lwt_io.stderr
    
  exception System_command_error of Lwt_unix.process_status
  let system_command s = 
    Lwt_unix.(
      system s >>= function
      | WEXITED 0 -> return ()
      | e -> fail (System_command_error e))

  let write_string_to_file s f =
    Lwt_io.(
      with_file ~mode:output f (fun o ->
        output_string o s))

end
module Hitscore_lwt = Hitscore.Make(Lwt_config)

include Hitscore_lwt
include Flow

module Flow_net =  struct

  let ssl_accept socket ssl_context =
    wrap_io (Lwt_ssl.ssl_accept socket) ssl_context

  let sleep f =
    wrap_io Lwt_unix.sleep f

  let ssl_client_context ?verification_policy c =
    let open Ssl in
    let certificate =
      match c with
      | `anonymous -> None
      | `with_pem_certificate c -> Some c
    in
    begin
      try
        let c = create_context TLSv1 Client_context in
        Option.iter certificate (fun (cert_pk) ->
          use_certificate c cert_pk cert_pk
        );
        set_cipher_list c "TLSv1";
        Option.iter verification_policy (function
        | `client_makes_sure -> 
          Ssl.set_verify_depth c 99;
          set_verify c [Verify_peer] (Some client_verify_callback);
        | `ok_self_signed -> ()
        );
        return c
      with e -> error (`ssl_context_exn e)
    end
      
  let ssl_server_context ?with_client_authentication cert_key_file =
    let open Ssl in
    try
      let c = create_context TLSv1 Server_context in
      use_certificate c cert_key_file cert_key_file;
      set_cipher_list c "TLSv1";
      Option.iter with_client_authentication (function
      | `with_CA_certificate ca_cert ->
        set_verify c [ Verify_peer; Verify_fail_if_no_peer_cert ] None;
        set_verify_depth c 99;
        load_verify_locations c ca_cert "";
      );
      return c
    with e -> error (`ssl_context_exn e)

      
  let server_socket ~port =
    let open Lwt_unix in
    try
    let fd = socket PF_INET SOCK_STREAM 6 in
    bind fd (ADDR_INET (Unix.inet_addr_any, port));
    listen fd port;
    return fd
    with
    | e -> error (`socket_creation_exn e)

  let ssl_connect socket ssl_context =
    wrap_io (Lwt_ssl.ssl_connect socket) ssl_context

  let ssl_shutdown socket =
    wrap_io Lwt_ssl.ssl_shutdown socket

end

let epr fmt = ksprintf (fun s -> prerr_string (format_message s)) (fmt ^^ "%!")
  
let dbg fmt = ksprintf debug fmt
  
let print_error = function
  | `io_exn e ->
    epr "I/O Exception:\n  %s\n   SSL-Global: %s\n%!"
      (Exn.to_string e) (Ssl.get_error_string ())
  | `ssl_context_exn e ->
    epr "Exception while creating SSL-context:\n   %s\n   SSL-GLOBAL: %s\n%!"
      (Exn.to_string e) (Ssl.get_error_string ())
  | `socket_creation_exn e ->
    epr "System command error:\n  exn: %s\n" (Exn.to_string e)
  | `system_command_error (c, e) ->
    epr "System command error:\n  cmd: %s\n  exn: %s\n"
      c (Exn.to_string e)
