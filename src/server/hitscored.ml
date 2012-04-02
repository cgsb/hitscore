open Hitscored_std

module String_map = Map.Make(String)

let read_file file =
  wrap_io Lwt_io.(fun () -> with_file ~mode:input file (fun i -> read i)) ()

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
let get_CA_index ca_cert =
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

    
let main ?ca_cert ~cert_key () =
  debug "Hello world!" >>= fun () ->
  let with_client_authentication =
    Option.map ca_cert (fun c -> `CA_certificate c) in
  Flow_net.ssl_server_context ?with_client_authentication cert_key
  >>= fun ssl_context ->
  Flow_net.server_socket ~port:2000 >>= fun socket ->

  of_option ca_cert (fun ca_cert ->
    get_CA_index ca_cert
    >>= fun ca_index ->
    return (fun c ->
      dbg "check_client_certificate:\n  Issuer: %s\n  Subject: %s"
        (Ssl.get_issuer c) (Ssl.get_subject c) >>= fun () ->
      let subj = (Ssl.get_subject c) in
      let cn = common_name_of_subject subj in
      begin match String_map.find ca_index subj with
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
      begin match client_kind with
      | `invalid_client `wrong_certificate ->
        dbg "The client has a wrong certificate"
      | `invalid_client (`expired_certificate _) ->
        dbg "The client has an expired certificate"
      | `invalid_client (`revoked_certificate _) ->
        dbg "The client has a revoked certificate"
      | `invalid_client (`certificate_not_found _) ->
        dbg "The client has a not-found certificate"
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
      
