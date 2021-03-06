

open Hitscore

open Core.Std
open Sequme_flow
open Sequme_flow_list
open Sequme_flow_app_util
module Flow_net = Sequme_flow_net


let with_profile () =
  let open Command_line.Spec in
  let default_path =
    sprintf "%s/.config/hitscore/config.sexp"
      (Option.value_exn ~message:"This environment has no $HOME !"
         (Sys.getenv "HOME")) in
  step (fun k profile -> k ~profile)
  +> flag "profile" ~aliases:["-p"] (required string)
    ~doc:"<profile> The configuration profile name to use"
  ++ step (fun k configuration_file -> k ~configuration_file)
  +> flag "configuration-file" ~aliases:["-c"]
    (optional_with_default default_path string)
    ~doc:(sprintf "<path> alternate configuration file (default %s)"
            default_path)

type ('a, 'b, 'c, 'd) global_state = {
  configuration: Configuration.local_configuration;
  libraries_info:
    unit ->
    ('a Hitscore_data_access_types.classy_libraries_information, 'b) t;
  persons_info:
    unit ->
    ('c Hitscore_data_access_types.classy_persons_information, 'd) t
}

let init ~configuration_file ~profile ~log_file ~pid_file =

  let pid = Unix.getpid () in
  Sequme_flow_sys.write_file pid_file ~content:(Core.Pid.to_string pid)
  >>= fun () ->
  Sequme_flow_net.init_tls ();
  Sequme_flow_sys.read_file configuration_file
  >>= fun contents ->
  of_result Configuration.(
    let open Result in
    parse_str contents >>= fun c -> use_profile c profile)
  >>= fun configuration ->
  begin match log_file with
  | "-" -> Messages.init_log (`channel Lwt_io.stdout)
  | f -> Messages.init_log (`file f)
  end
  >>= fun () ->
  log "Loaded profile %S from %S" profile configuration_file >>= fun () ->
  let libraries_info =
    Data_access.init_classy_libraries_information_loop
      ~loop_waiting_time:25. ~log:(log "[LibInfo] %s") ~allowed_age:60.
      ~maximal_age:1200. ~configuration in
  let persons_info =
    Data_access.init_classy_persons_information_loop
      ~loop_waiting_time:25. ~log:(log "[PplInfo] %s") ~allowed_age:60.
      ~maximal_age:1200. ~configuration in
  return { configuration; libraries_info; persons_info }

type ('a, 'b, 'c, 'd) connection_state = {
  global: ('a, 'b, 'c, 'd) global_state;
  connection:  Sequme_flow_net.connection;
  serialization_mode: Communication.Protocol.serialization_mode;
  mutable user: [ `none | `token_authenticated of Layout.Record_person.t ];
}

let recv_message state =
  Sequme_flow_io.bin_recv (Flow_net.in_channel state.connection)
  >>= fun msg ->
  let mode = state.serialization_mode in
  of_result (Communication.Protocol.up_of_string ~mode msg)

let send_message state msg =
  let mode = state.serialization_mode in
  let str =
    Communication.Protocol.string_of_down ~mode msg in
  Sequme_flow_io.bin_send (Flow_net.out_channel state.connection) str

let with_layout ~state f =
  with_database ~configuration:state.global.configuration (fun ~dbh ->
    let layout = Classy.make dbh in
    f layout)

let find_user layout user =
  layout#person#all
  >>| List.find ~f:(fun p ->
    p#login = Some user || p#email = user
            || Array.exists p#secondary_emails ~f:((=) user))

let handle_new_token ~state ~user ~password ~token_name ~token =
  with_layout ~state (fun layout ->
    find_user layout user
    >>= begin function
    | Some person ->
      Communication.Authentication.check_chained
        ~pam_service:"login" ~person ~password ()
      >>= begin function
      | true ->
        while_sequential (Array.to_list person#auth_tokens) (fun at -> at#get)
        >>| List.find ~f:(fun t -> t#name = token_name)
        >>= begin function
        | Some t ->
          t#set_hash
            (Communication.Authentication.hash_password person#g_id token)
          >>= fun () ->
          send_message state (`token_updated)
        | None ->
          layout#add_authentication_token ()
            ~name:token_name
            ~hash:(Communication.Authentication.hash_password
                     person#g_id token)
          >>= fun atp ->
          person#set_auth_tokens
            Array.(append (map person#auth_tokens ~f:(fun p -> p#pointer))
                     [| atp#pointer |])
          >>= fun () ->
          send_message state (`token_created)
        end
      | false ->
        send_message state (`error `wrong_authentication)
      end
    | None ->
      send_message state (`error `wrong_authentication)
    end)
  >>< begin function
  | Ok () -> return ()
  | Error e ->
    send_message state
      (`error (`server_error "please complain, with detailed explanations"))
    >>= fun () ->
    error e
  end

let authenticate ~state ~user ~token_name ~token =
  with_layout ~state (fun layout ->
    find_user layout user
    >>= begin function
    | Some person ->
      while_sequential (Array.to_list person#auth_tokens) (fun at -> at#get)
      >>| List.find ~f:(fun t -> t#name = token_name)
      >>= begin function
      | Some t ->
        if t#hash = Communication.Authentication.hash_password person#g_id token
        then begin
          state.user <- `token_authenticated person#g_t;
          send_message state (`authentication_successful)
        end
        else send_message state (`error `wrong_authentication)
      | None ->
        send_message state (`error `wrong_authentication)
      end
    | None ->
      send_message state (`error `wrong_authentication)
    end)
  >>< begin function
  | Ok () -> return ()
  | Error e ->
    send_message state
      (`error (`server_error "please complain, with detailed explanations"))
    >>= fun () ->
    error e
  end

let retrieve_simple_info ~state =
  begin match state.user with
  | `none -> send_message state (`error `wrong_authentication)
  | `token_authenticated t ->
    with_layout ~state (fun layout ->
      let classy_person = new Classy.person_element layout#dbh t in
      let open Communication.Protocol in
      while_sequential (Array.to_list classy_person#affiliations) (fun a ->
        a#get >>= fun aff ->
        return (Array.to_list aff#path))
      >>= fun psi_affiliations ->
      send_message state (`simple_info {
        psi_print_name = classy_person#print_name;
        psi_full_name =
          (classy_person#given_name, classy_person#middle_name,
           classy_person#nickname, classy_person#family_name);
        psi_emails =
          classy_person#email :: Array.to_list classy_person#secondary_emails;
        psi_login = classy_person#login;
        psi_affiliations;
      })
    )
  end

let list_libraries ~state query =
  let find_fastq_files lib =
    let module Bui = Hitscore_interfaces.B2F_unaligned_information in
    while_sequential lib#submissions (fun sub ->
      while_sequential sub#flowcell#hiseq_raws (fun hr ->
        while_sequential hr#demultiplexings (fun dmux ->
          let read_number =
            Data_access.get_fastq_stats lib sub dmux
            |! Option.map ~f:(fun s -> s.Bui.cluster_count) in
          map_option (Data_access.choose_delivery_for_user dmux sub)
            (fun (del, inv) ->
              let path =
                Option.(value_map ~default:"NO-DIR"
                          ~f:(fun d-> d#directory) del#client_fastqs_dir) in
              return path)
          >>= fun delivery_path ->
          begin match delivery_path with
          | None -> return None
          | Some del_path ->
            map_option dmux#unaligned (fun un ->
                let files =
                  Data_access.user_file_paths ~unaligned:un ~submission:sub lib in
                let prefix =
                  Option.value ~default:""
                    (Configuration.root_path state.global.configuration) in
                let r1s =
                  List.map files#fastq_r1s (String.chop_prefix ~prefix)
                  |> List.filter_opt in
                let r2s =
                  List.map files#fastq_r2s (String.chop_prefix ~prefix)
                  |> List.filter_opt in
                return (r1s, r2s, read_number)
              )
          end)
        >>| List.filter_opt))
    >>| List.concat
    >>| List.concat
  in
  begin match state.user with
  | `none -> send_message state (`error `wrong_authentication)
  | `token_authenticated t ->
    log "qnames: %s" (String.concat ~sep:", " query) >>= fun () ->
    state.global.libraries_info ()
    >>= fun classy_libs ->
    let libraries =
      let persons_libraries =
        List.filter classy_libs#libraries
          (fun l ->
            let people =
              List.map l#submissions (fun sub -> sub#lane#contacts)
              |! List.concat
              |! List.map ~f:(fun p -> p#g_t) in
            List.exists people ((=) t))
      in
      let pcre_matches rex str =
        try ignore (Pcre.exec ~rex str); true with _ -> false in
      let pcre_build qs =
        try Pcre.regexp qs with _ -> Pcre.regexp (String.make 42 'B') in
      if query = []
      then persons_libraries
      else
        List.map query (fun qs ->
          let rex = pcre_build qs in
          List.filter persons_libraries (fun l ->
            pcre_matches rex
              (sprintf "%s.%s"
                 (Option.value ~default:"" l#stock#project) l#stock#name)))
        |! List.concat
    in
    while_sequential libraries (fun l ->
      find_fastq_files l >>= fun fastq_data ->
      let li_name = l#stock#name in
      let li_project =  l#stock#project in
      let li_description =  l#stock#description in
      let li_barcoding =
        List.map l#barcoding ~f:(List.map ~f:(fun b ->
            "TODO")) in
      let li_sample =
        (Option.(l#sample >>= fun s -> return s#sample#name),
         Option.(l#sample >>= fun s ->
                 s#organism >>= fun o -> o#name)) in
      let li_fastq_files =
        List.filter_map fastq_data (function
        | ([], _, c) -> None
        | (r1 :: _, [], c) -> Some (r1, None, c)
        | (r1 :: _, r2 :: _, c) -> Some (r1, Some r2, c)) in
      return {Hitscore_communication.Protocol.
              li_name; li_project; li_description;
              li_barcoding; li_sample; li_fastq_files; })
    >>= fun result ->
    send_message state (`libraries result)
  end

let with_auth ~state =
  begin match state.user with
  | `none ->
    send_message state (`error `wrong_authentication) >>= fun () ->
    error `stop
  | `token_authenticated t -> return t
  end

let list_tokens ~state =
  with_auth ~state >>= fun logged ->
  state.global.persons_info () >>= fun info ->
  let tokens =
    List.filter_map info#persons (fun p ->
      if p#t#g_id = logged.Layout.Record_person.g_id
      then Some (List.map p#tokens (fun t -> t#name))
      else None) |! List.concat  in
  send_message state (`tokens tokens)

let revoke_token ~state token =
  with_auth ~state >>= fun logged ->
  state.global.persons_info () >>= fun info ->
  let search =
    List.find_map info#persons (fun p ->
      if p#t#g_id = logged.Layout.Record_person.g_id
      then Some (p, List.find p#tokens (fun t -> t#name = token))
      else None) in
  begin match search with
  | Some (p, Some t) ->
    let tokens =
      Array.filter_map p#t#auth_tokens (fun ptok ->
        if ptok#id = t#g_id then None else Some ptok#pointer) in
    p#t#set_auth_tokens tokens >>= fun () ->
    t#unsafe_delete >>= fun () ->
    send_message state (`token_updated)
  | Some (_, None) ->
    send_message state (`error (`server_error "token not found"))
  | None ->
    send_message state (`error (`server_error "person not found"))
  end


let rec handle_up_message ~state =
  recv_message state
  >>= begin function
  | `log s ->
    log "Client wants to log: %S" s
    >>= fun () ->
    begin match state.user with
    | `none ->
      send_message state (`user_message "you can't!")
    | `token_authenticated p ->
      log "and the client was right …"
    end
  | `new_token (user, password, token_name, token) ->
    handle_new_token ~state ~user ~password ~token_name ~token
  | `list_tokens -> list_tokens ~state
  | `revoke_token t -> revoke_token ~state t
  | `authenticate (user, token_name, token) ->
    authenticate ~state ~user ~token ~token_name
  | `get_simple_info ->
    log "get_simple_info" >>= fun () ->
    retrieve_simple_info ~state
  | `get_libraries names_rexes -> list_libraries ~state names_rexes
  | `terminate ->
    log "Terminating connection" >>= fun () ->
    error `stop
  end
  >>= fun () ->
  handle_up_message ~state

type error_in_connection =
[ `bin_recv of [ `exn of exn | `wrong_length of int * string ]
| `bin_send of [ `exn of exn | `message_too_long of string ]
| `io_exn of exn
| `Layout of Hitscore.Layout.error_location * Hitscore.Layout.error_cause
| `db_backend_error of [ Hitscore.Backend.error ]
| `message_serialization of
    Communication.Protocol.serialization_mode * string * exn
| `root_directory_not_configured
] with sexp_of

let handle_connection_error ~state e =
  begin match e with
  | `stop -> return ()
  | #error_in_connection as subset ->
    let to_log = Sexp.to_string_hum (sexp_of_error_in_connection subset) in
    log "Error in connection:\n%s\nSSL: %s" to_log (Ssl.get_error_string ())
  end
  >>= fun () ->
  Flow_net.shutdown state.connection

let handle_connection ~global ~mode connection =
  let state =
    { connection; global;
      serialization_mode = mode;
      user = `none } in
  bind_on_error
    (handle_up_message ~state)
    (handle_connection_error ~state)

let start_server ~port ~global ~cert_key ~mode =
  let on_error = function
    | `accept_exn e ->
      log "ERROR: Accept-exception: %s" (Exn.to_string e)
    | `io_exn e ->
      log "ERROR: I/O-exception: %s" (Exn.to_string e)
    | `not_an_ssl_socket ->
      log "ERROR: Not an SSL socket"
    | `tls_accept_error e  ->
      log "ERROR: TLS-accept-exception: %s" (Exn.to_string e)
    | `bin_send  (`exn e) ->
      log "ERROR: bin-send-exception: %s" (Exn.to_string e)
    | `bin_send (`message_too_long s) ->
      log "ERROR: bin-send: message too long (%d bytes)" (String.length s)
    | `wrong_subject_format s ->
      log "ERROR: TLS: wrong_subject_format: %s" s
  in
  Sequme_flow_net.tls_server ~on_error ~port ~cert_key
    (handle_connection ~mode ~global) >>= fun () ->
  log "Server started on port: %d" port


type error =
[ `configuration_parsing_error of exn
| `io_exn of exn
| `profile_not_found of string
| `read_file_timeout of string * float
| `read_file_error of string * exn
| `write_file_error of string * exn
| `socket_creation_exn of exn
| `tls_context_exn of exn ]
with sexp_of

let string_of_error e =
  Sexp.to_string_hum (sexp_of_error e)


let command =
  let open Command_line in
  let default_pid_file = "/tmp/hitscored.pid" in
  basic ~summary:"Hitscore's server application"
    Spec.(
      with_profile ()
      ++ step (fun k log_file -> k ~log_file)
      +> flag "log-file" ~aliases:["-L"] (optional_with_default "-" string)
        ~doc:"<path> Log to file <path> (or “-” for stdout; the default)"
      ++ step (fun k pid_file -> k ~pid_file)
      +> flag "pid-file" (optional_with_default default_pid_file string)
        ~doc:(sprintf
                "<path> write PID to a file (default: %s)" default_pid_file)
      ++ Communication.Protocol.serialization_mode_flag ()
      +> flag "cert" (required string) ~doc:"SSL certificate"
      +> flag "key" (required string) ~doc:"SSL private key"
      +> anon ("PORT" %: int)
    )
    (fun ~profile ~configuration_file ~log_file ~pid_file ~mode cert key port () ->
      run_flow ~on_error:(fun e ->
        eprintf "End with ERRORS: %s" (string_of_error e))
        begin
          init ~profile ~configuration_file ~log_file ~pid_file
          >>= fun global ->
          start_server ~cert_key:(cert, key) ~port ~global ~mode
          >>= fun () ->
          wrap_io (fun () -> Lwt.(let (t,_) = wait () in t)) ()
          >>= fun _ ->
          return ()
        end)


let () =
  Command_line.(run ~version:"0" command)
