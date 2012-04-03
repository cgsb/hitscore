
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

let read_file file =
  wrap_io Lwt_io.(fun () -> with_file ~mode:input file (fun i -> read i)) ()

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
  | `not_an_ssl_socket ->
    epr "Got a plain socket when expecting an SSL one."
  | `ssl_certificate_error ->
    epr "Error while trying to get an SSL certificate (anonymous connection?):\n\
         \  %s\n." (Ssl.get_error_string ()) 
  | `wrong_CA_index_format exn ->
    epr "Error while parsing the CA index.txt:\n  Exn: %s\n"  (Exn.to_string exn)
  | `sexp_parsing_error e ->
    epr "Error while parsing a S-Exp message:\n  Exn: %s\n" (Exn.to_string e)
  | `bin_write_error e ->
    epr "Error while writing a message:\n  Exn: %s\n" (Exn.to_string e)
  | `bin_read_error e ->
    epr "Error while reading a message:\n  Exn: %s\n" (Exn.to_string e)



module Certificate_authority = struct


  module String_map = Map.Make(String)


  let parse_ASN1_UTCTIME_format_exn s =
  (* YYMMDDHHMMSSZ e.g. 220328212233Z *)
    if String.length s  <> 13
    then failwithf "invalid_ASN1_date %s" s ()
    else
      try
        let get2 i = Int.of_string (String.sub s i 2) in
        let y = 2000 + (get2 0) in
        let m = get2 2 |! Month.of_int_exn in
        let d = get2 4 in
        let hr = get2 6 in
        let mn = get2 8 in
        let sec = get2 10 in
        Time.(of_date_ofday
                Zone.utc Date.(create ~m ~y ~d) Ofday.(create ~hr ~min:mn ~sec ()))
      with
      | e -> failwithf "invalid_ASN1_date %s" s ()
        
  let common_name_of_subject subj =
    List.find_map (String.split subj ~on:'/') (function
    | s when String.prefix s 3 = "CN=" ->
      begin match String.split ~on:'-' (String.drop_prefix s 3) with
      | [ login; _ ] -> Some login
      | _ -> None
      end
    | _ -> None)
      
  (* http://old.nabble.com/Format-of-index.txt-file-td21557292.html *)
  let get_index ca_cert =
    let ca_path = Filename.dirname ca_cert in
    read_file Filename.(concat ca_path "index.txt")
    >>| String.split ~on:'\n'
    >>| List.map ~f:(String.split ~on:'\t')
    >>= fun lines ->
    let date = parse_ASN1_UTCTIME_format_exn in
    (try
       List.fold_left lines ~init:String_map.empty ~f:(fun ca_index line ->
         begin match line with
         | "V" :: exp_date :: "" :: serial :: _ :: name :: [] ->
           String_map.add ~key:name ~data:(`valid (date exp_date, serial)) ca_index
         | "R" :: exp_date :: rev_date :: serial :: _ :: name :: [] ->
           String_map.add ~key:name ~data:(`revoked (date rev_date, serial)) ca_index
         | "E" :: exp_date :: _ :: serial :: _ :: name :: [] ->
           String_map.add ~key:name
             ~data:(`expired (date exp_date, serial)) ca_index
         | [] | [""] -> ca_index
         | l ->
           failwithf "get_CA_index:cannot_parse [%s]" (String.concat ~sep:", " l) ()
         end)
       |! return
     with 
     | e -> error (`wrong_CA_index_format e))

  let find_name = String_map.find

end

      
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
      | `CA_certificate ca_cert ->
        set_verify c [ Verify_peer; Verify_fail_if_no_peer_cert ] None;
        set_verify_depth c 99;
        load_verify_locations c ca_cert "";
      | `CA_path ca_path ->
        set_verify c [ Verify_peer; Verify_fail_if_no_peer_cert ] None;
        set_verify_depth c 99;
        load_verify_locations c "" ca_path;
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

  module M_ugly_ssl_get_certificate = struct
    type ttt = Plain | SSL of Ssl.socket
    type lwt_ssl_socket = Lwt_unix.file_descr * ttt

    let get_certificate sslsock =
      begin match snd (Obj.magic sslsock : lwt_ssl_socket) with
      | Plain -> error (`not_an_ssl_socket)
      | SSL s ->
        Lwt.(
          catch
            (fun () ->
              Lwt_preemptive.detach Ssl.get_certificate s
              >>= fun cert ->
              return (Ok cert))
            (function
            | Ssl.Certificate_error ->
              return (Error `ssl_certificate_error)
            | e -> 
              return (Error (`io_exn e))))
      end
  end
  let ssl_get_certificate = M_ugly_ssl_get_certificate.get_certificate

    
  let ssl_accept_loop ?check_client_certificate ssl_context socket f =
    let handle_one accepted =
      ssl_accept (fst accepted) ssl_context >>= fun ssl_accepted ->
      debug "Accepted (SSL)" >>= fun () ->
      begin match check_client_certificate with
      | Some ccc ->
        double_bind (ssl_get_certificate ssl_accepted)
          ~error:(function
          | `ssl_certificate_error -> return (`invalid_client `wrong_certificate)
          | `not_an_ssl_socket | `io_exn _ as e -> error e)
          ~ok:(fun cert ->
            ccc cert >>= function
            | `ok -> return (`valid_client cert)
            | `expired -> return (`invalid_client (`expired_certificate cert))
            | `certificate_not_found ->
              return (`invalid_client (`certificate_not_found cert))
            | `revoked -> return (`invalid_client (`revoked_certificate cert)))
      | None -> return `anonymous_client
      end
      >>= fun client ->
      f ssl_accepted client
    in
    let rec accept_loop c =
      dbg "Accepting #%d (unix)" c >>= fun () ->
      wrap_io (Lwt_unix.accept_n socket) 10
      >>= fun (accepted_list, potential_exn) ->
      dbg "Accepted %d connections (unix)%s" (List.length accepted_list)
        (Option.value_map ~default:"" potential_exn
           ~f:(fun e -> sprintf ", Exn: %s" (Exn.to_string e)))
      >>= fun () ->
      accept_loop (c + 1) |! Lwt.ignore_result;
      Lwt.(
        Lwt_list.map_p handle_one accepted_list
        >>= fun res_l ->
        List.iter res_l (function
        | Ok () -> ()
        | Error e -> print_error e);
        return (Ok ()))
    in
    accept_loop 0

end


module Message = struct

  type client = [
  | `hello
  ] with sexp

  type server = [
  | `hello of [
    | `authenticated of Layout.Enumeration_role.t list
    | `anonymous ]
  ] with sexp

  let max_message_length = 10_000_000
    
  let bin_write_string oc =
    wrap_io ~on_exn:(fun e -> `bin_write_error e)
      Lwt.(fun s ->
        Lwt_io.BE.write_int oc (String.length s) >>= fun () ->
        Lwt_io.write oc s)

  let bin_read_string ic =
    wrap_io ~on_exn:(fun e -> `bin_read_error e)
      Lwt.(fun () ->
        Lwt_io.BE.read_int ic >>= fun c ->
        Lwt_io.read ~count:(min c max_message_length) ic >>= fun s ->
        if String.length s <> c then
          fail (Failure (sprintf "wrong length + end of file: c:%d s:%d"
                           c (String.length s)))
        else
          return s)
      ()

  let server_send oc m =
    Sexp.to_string_hum (sexp_of_server m) |! bin_write_string oc
        
  let client_send oc m =
    Sexp.to_string_hum (sexp_of_client m) |! bin_write_string oc

  let recv_server ic =
    bin_read_string ic >>= fun s ->
    wrap_io ~on_exn:(fun e -> `sexp_parsing_error e)
      (fun s -> Sexp.of_string s |! server_of_sexp |! Lwt.return) s
    
  let recv_client ic =
    bin_read_string ic >>= fun s ->
    wrap_io ~on_exn:(fun e -> `sexp_parsing_error e)
      (fun s -> Sexp.of_string s |! client_of_sexp |! Lwt.return) s
    
      
end
