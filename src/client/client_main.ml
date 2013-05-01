

open Hitscore
open Core.Std
open Sequme_flow
open Sequme_flow_list
open Sequme_flow_app_util
module Flow_net = Sequme_flow_net


let msg fmt =
  ksprintf (fun s -> printf "%s\n%!" s; return ()) fmt

let debug = ref false
let dbg fmt =
  ksprintf (fun s -> if !debug then printf "DEBUG: %s\n%!" s; return ()) fmt


module Configuration = struct

  type configuration_v0 = {
    host: string;
    port: int;
    user_name: string;
    auth_token_name: string;
    auth_token:  string;
  } with sexp
  type t = [
  | `gencore of [
    | `v0 of configuration_v0
  ]
  ] with sexp

  let make ~host ~port ~auth_token ~auth_token_name ~user_name =
    `gencore (`v0 {host; port; auth_token; auth_token_name; user_name})

  let to_string c =
    Sexp.to_string_hum (sexp_of_t c)
  let of_string_exn s =
    Sexp.of_string s |! t_of_sexp
  let of_string s =
    try Ok (of_string_exn s) with e -> Error (`parse_configuration e)

  let of_file file =
    Sequme_flow_sys.read_file file
    >>| String.strip
    >>= fun s ->
    of_result (of_string s)

  let host = function
    | `gencore (`v0 {host; _})  -> host
  let port = function
    | `gencore (`v0 {port; _})  -> port
  let user_name = function
    | `gencore (`v0 {user_name; _})  -> user_name
  let token = function
    | `gencore (`v0 {auth_token; _})  -> auth_token
  let token_name = function
    | `gencore (`v0 {auth_token_name; _})  -> auth_token_name
end

type connection_state = {
  connection:  Sequme_flow_net.connection;
  serialization_mode: Communication.Protocol.serialization_mode;
  configuration: Configuration.t option;
}
let state  ~mode ?configuration connection =
  { connection; serialization_mode = mode; configuration}

let send_message ~state message =
  Sequme_flow_io.bin_send (Flow_net.out_channel state.connection)
    (Communication.Protocol.string_of_up ~mode:state.serialization_mode message)

let recv_message ~state =
  Sequme_flow_io.bin_recv (Flow_net.in_channel state.connection)
  >>= fun m ->
  of_result (Communication.Protocol.down_of_string
               ~mode:state.serialization_mode m)

let cmdf fmt = ksprintf Sequme_flow_sys.system_command fmt

type common_error =
[ `bin_recv of [ `exn of exn | `wrong_length of int * string ]
| `bin_send of [ `exn of exn | `message_too_long of string ]
| `io_exn of exn
| `parse_configuration of exn
| `system_command_error of
    string *
      [ `exited of int
      | `exn of exn
      | `signaled of int
      | `stopped of int ]
| `write_file_error of string * exn
| `read_file_timeout of string * float
| `read_file_error of string * exn
| `message_serialization of
    Communication.Protocol.serialization_mode * string * exn
| `unexpected_message of Communication.Protocol.down
| `tls_context_exn of exn ]
with sexp_of

type init_error = [
| common_error
| `no_password
]
with sexp_of

let connect ~host ~port =
  Sequme_flow_net.init_tls ();
  Sequme_flow_net.connect
    ~address:Unix.(ADDR_INET (Inet_addr.of_string_or_getbyname host, port))
    (`tls (`anonymous, `allow_self_signed))

let create_token () =
  Random.self_init ();
  String.init 128 (fun i -> Random.int 64 + 32 |! Char.of_int_exn)

let run_init_protocol ~state ~token_name ~host ~port ~configuration_file
    ~user_name =
  Read_line.password ()
    ~prompt:(sprintf "Password for %S (like the one for the website): "
               user_name)
  >>= begin function
  | Some s -> return s
  | None -> error `no_password
  end
  >>= fun password ->
  let token = create_token () in
  send_message ~state (`new_token (user_name, password, token_name, token))
  >>= fun () ->
  recv_message ~state
  >>= begin function
  | `user_message _
  | `simple_info _
  | `authentication_successful
  | `libraries _
  | `tokens _
  | `error `not_implemented as m -> error (`unexpected_message m)
  | `token_updated
  | `token_created ->
    msg "The Authentication Token has been validated by the server \\o/"
    >>= fun () ->
    cmdf "mkdir -p %S" Filename.(dirname configuration_file)
    >>= fun () ->
    let config =
      Configuration.make ~host ~port ~auth_token:token
        ~auth_token_name:token_name ~user_name in
    cmdf "chmod -f 600 %S; true" configuration_file
    >>= fun () ->
    Sequme_flow_sys.write_file configuration_file
      ~content:(Configuration.to_string config ^ "\n")
    >>= fun () ->
    cmdf "chmod 400 %S" configuration_file
    >>= fun () ->
    msg "The configuration file as been successfully written, Happy Hacking!"
  | `error `wrong_authentication ->
    msg "The server has rejected your authentication; \
         the username or password must be wrong"
  | `error (`server_error s) ->
    msg "The server has run into trouble; you mya try again a bit later, \
         or if the problem persists call for help."
  end

module Flag = struct

  let with_config_file () =
    let default_config_file =
      let home = Sys.getenv "HOME" |! Option.value ~default:"/tmp/" in
      Filename.concat home ".config/gencore/client.conf" in
    Command_line.Spec.(
      step (fun k configuration_file -> k ~configuration_file)
      +> flag "-configuration-file" ~aliases:["c"]
        (optional_with_default default_config_file string)
        ~doc:(sprintf "<path> Use this configuration file (default: %s)"
                default_config_file)
    )

  let with_user_name () =
    let login = Unix.getlogin () in
    Command_line.Spec.(
      step (fun k user_name -> k ~user_name)
      +> flag "-user" ~aliases:["U"] (optional_with_default login string)
        ~doc:(sprintf "<name/email> Set the username (default: %s)" login)
    )

end

let init_command =
  let open Command_line in
  let default_token_name =
    let login = Unix.getlogin () in
    let host = Unix.gethostname () in
    sprintf "%s@%s" login host in
  basic ~summary:"Configure the application and get an authentication token"
    Spec.(
      Communication.Protocol.serialization_mode_flag ()
      ++ Flag.with_config_file ()
      ++ Flag.with_user_name ()
      +> flag "-token-name"  (optional_with_default default_token_name string)
        ~doc:(sprintf
                "<name> Give a name to the authentication token (default: %s)"
                default_token_name)
      +> anon ("HOST" %: string)
      +> anon ("PORT" %: int)
    )
    (fun ~mode  ~configuration_file ~user_name token_name host port () ->
      run_flow ~on_error:(fun e ->
        eprintf "Client ends with Errors: %s"
          (Sexp.to_string_hum (sexp_of_init_error e)))
        begin
          connect ~host ~port
          >>= fun connection ->
          let state = state connection ~mode in
          run_init_protocol ~state ~token_name ~host ~port ~configuration_file
            ~user_name
          >>= fun () ->
          send_message state `terminate >>= fun () ->
          Flow_net.shutdown connection
        end)

let connect_and_authenticate ~configuration_file ~mode =
  Configuration.of_file configuration_file
  >>= fun configuration ->
  connect ~host:(Configuration.host configuration)
    ~port:(Configuration.port configuration)
  >>= fun connection ->
  let state = state connection ~mode ~configuration in
  send_message state
    (`authenticate (Configuration.user_name configuration,
                    Configuration.token_name configuration,
                    Configuration.token configuration))
  >>= fun () ->
  recv_message state
  >>= begin function
  | `authentication_successful -> dbg "Authentication successful !¡!"
  | `error `wrong_authentication ->
    msg "Authentication failed …" >>= fun () ->
    error `stop
  | e -> error (`unexpected_message e)
  end
  >>= fun () ->
  return state

let terminate_and_disconnect ~state =
  send_message state `terminate >>= fun () ->
  Flow_net.shutdown state.connection
  >>< begin function
  | Ok () -> return ()
  | Error e ->
    let str = <:sexp_of< [ `io_exn of exn ] >> e |> Sexp.to_string in
    dbg "small error while shutting down: %s" str
  end

type info_error = [
| common_error
]
with sexp_of

let info ~state =
  dbg "Getting info …" >>= fun () ->
  send_message state `get_simple_info >>= fun () ->
  dbg "Waiting?" >>= fun () ->
  recv_message state
  >>= begin function
  | `simple_info psi ->
    let open Communication.Protocol in
    msg "Got your information:\n%s"
      (String.concat ~sep:"" [
        Option.value_map ~default:""
          psi.psi_print_name ~f:(sprintf "Print name: %s\n");
        begin
          let (g, m, n, f) = psi.psi_full_name in
          sprintf "Full name(s): %s %s%s%s\n"
            g (Option.value ~default:" " m)
            (Option.value_map ~default:"" n ~f:(sprintf "“%s” ")) f
        end;
        "Emails: "; String.concat ~sep:", " psi.psi_emails; "\n";
        Option.value_map ~default:"" psi.psi_login ~f:(sprintf "Login: %s\n");
        begin
          let aff = String.concat ~sep:"/" in
          match psi.psi_affiliations with
          | [] -> ""
          | [one] -> sprintf "Affiliation: %s\n" (aff one)
          | some ->
            sprintf "Affiliations: %s\n"
              (String.concat ~sep:", " (List.map some aff))
        end
      ])
  | m -> error (`unexpected_message m)
  end

let self_test_command =
  let open Command_line in
  basic ~summary:"Test your connection/authentication setting by getting \
                  information about you from the server"
    Spec.(
      Communication.Protocol.serialization_mode_flag ()
      ++ Flag.with_config_file ()
    ) (fun ~mode ~configuration_file () ->
      run_flow ~on_error:(function
      | `stop -> printf "Stopping\n%!"
      | #info_error as e ->
        eprintf "Client ends with Errors: %s"
          (Sexp.to_string_hum (sexp_of_info_error e)))
        begin
          connect_and_authenticate ~configuration_file ~mode
          >>= fun state ->
          info ~state
          >>= fun () ->
          terminate_and_disconnect ~state
        end)

type list_libraries_error = [
| common_error
] with sexp_of

let list_libraries ~state ~separator ~query ~spec =
  send_message state (`get_libraries query) >>= fun () ->
  dbg "Waiting?" >>= fun () ->
  recv_message state
  >>= begin function
  | `libraries table ->
    let output fmt = ksprintf (fun s -> wrap_io (Lwt_io.printf "%s%!") s) fmt in
    while_sequential table (fun row ->
      let open Hitscore_communication.Protocol in
      let barcoding_to_string l =
        let one s = s in
        match l with
        | [[o]] -> one o
        | [l] -> String.concat ~sep:" AND " (List.map l one)
        | l ->
          String.concat ~sep:") OR (" (List.map l (fun la ->
            String.concat ~sep:" AND " (List.map la one))) |! sprintf "(%s)"
      in
      begin match spec with
      | `all ->
        let row_string =
          String.concat ~sep:separator [
            row.li_name;
            Option.value ~default:"" row.li_project;
            Option.value ~default:"" row.li_description;
            barcoding_to_string row.li_barcoding;
            Option.value ~default:"" (fst row.li_sample);
            Option.value ~default:"" (snd row.li_sample);
            (let l = List.length row.li_fastq_files in
             sprintf "%d FASTQ deliver%s: %s reads"
               l (if l = 1 then "y" else "ies")
               (List.map row.li_fastq_files
                  (fun (_, _, fo) ->
                    Option.value_map ~default:"?" fo ~f:(sprintf "%.0f"))
                |! String.concat ~sep:", "));
          ] in
        output "%s" row_string
      | `fastq_read (n, m) ->
        begin match List.nth row.li_fastq_files (n - 1) with
        | Some (r1, r2o, _) ->
          begin match m, r2o with
          | 1, _ -> output "%s" r1
          | 2, Some r2 -> output "%s" r2
          | _ -> output "NO-READ-%d" m
          end
        | None -> output "%dth-READS-NOT-FOUND" n
        end
      end
      >>= fun () ->
      output "\n")
    >>= fun _ ->
    return ()
  | m -> error (`unexpected_message m)
  end

let list_libraries_command =
  let open Command_line in
  basic ~summary:"List your libraries"
    Spec.(
      Communication.Protocol.serialization_mode_flag ()
      ++ Flag.with_config_file ()
      ++ step (fun k fastq_read -> k ~fastq_read)
      +> flag "-read" ~aliases:["R"] (optional int)
        ~doc:(sprintf "<number> Get the FASTQ file path for read <number>")
      +> anon (sequence ("QUERY-REGULAR-EXPRESSIONS" %: string))
    ) (fun ~mode ~configuration_file ~fastq_read query () ->
      run_flow ~on_error:(function
      | `stop -> printf "Stopping\n%!"
      | #info_error as e ->
        eprintf "Client ends with Errors: %s\n"
          (Sexp.to_string_hum (sexp_of_list_libraries_error e)))
        begin
          connect_and_authenticate ~configuration_file ~mode
          >>= fun state ->
          let spec =
            match fastq_read with
            | Some n -> `fastq_read (1, n)
            | None -> `all in
          list_libraries ~state ~spec ~separator:", " ~query
          >>= fun () ->
          terminate_and_disconnect ~state
        end)

type auth_list_tokens_error = common_error with sexp_of
type auth_revoke_token_error = common_error with sexp_of

let auth_command =
  let open Command_line in
  let list_tokens_command =
    basic ~summary:"List your authentication token names"
      Spec.(
        Communication.Protocol.serialization_mode_flag ()
        ++ Flag.with_config_file ()
      ) (fun ~mode ~configuration_file () ->
        run_flow ~on_error:(function
        | `stop -> printf "Stopping\n%!"
        | #auth_list_tokens_error as e ->
          eprintf "Client ends with Errors: %s\n"
            (Sexp.to_string_hum (sexp_of_auth_list_tokens_error e)))
          begin
            let output fmt = ksprintf (fun s -> wrap_io Lwt_io.print s) fmt in
            connect_and_authenticate ~configuration_file ~mode
            >>= fun state ->
            send_message state `list_tokens
            >>= fun () ->
            dbg "Sent query, waiting for response" >>= fun () ->
            recv_message state
            >>= begin function
            | `tokens l ->
              output "Tokens:\n" >>= fun () ->
              while_sequential l (output "  * %S\n")
              >>= fun _ ->
              output "%!"
            | m -> error (`unexpected_message m)
            end
            >>= fun () ->
            terminate_and_disconnect ~state
          end)
  in
  let revoke_token_command =
    basic ~summary:"Revoke a given token by name"
      Spec.(
        Communication.Protocol.serialization_mode_flag ()
        ++ Flag.with_config_file ()
        +> anon ("NAME" %: string)
      ) (fun ~mode ~configuration_file token_name  () ->
        run_flow ~on_error:(function
        | `stop -> printf "Stopping\n%!"
        | #auth_revoke_token_error as e ->
          eprintf "Client ends with Errors: %s\n"
            (Sexp.to_string_hum (sexp_of_auth_list_tokens_error e)))
          begin
            connect_and_authenticate ~configuration_file ~mode
            >>= fun state ->
            send_message state (`revoke_token token_name)
            >>= fun () ->
            dbg "Sent query, waiting for response" >>= fun () ->
            recv_message state
            >>= begin function
            | `token_updated ->
              msg "Done"
            | m -> error (`unexpected_message m)
            end
            >>= fun () ->
            terminate_and_disconnect ~state
          end)
  in
  group ~summary:"Manage you authentication tokens" [
    ("list", list_tokens_command);
    ("revoke", revoke_token_command);
  ]

let manual () =
  let module S = struct
    include Core_extended.Extended_string
    let indent ?(indentation=4) s =
      String.split ~on:'\n' s
      |> List.map ~f:(sprintf "%s%s" (String.make indentation ' '))
      |> String.concat ~sep:"\n"
    let underline s char =
      let without_utf8 =
(* http://stackoverflow.com/questions/4063146/getting-the-actual-length-of-a-utf-8-encoded-stdstring *)
        String.filter s (fun c -> Char.to_int c land 0xc0 <> 0x80) in
      let under = String.make (String.length without_utf8) char in
      sprintf "%s\n%s\n" s under

  end in
  let outbuf = Buffer.create 42 in
  let outs s = Buffer.add_string outbuf s in
  let out fmt = ksprintf outs fmt in
  let par fmt =
    ksprintf (fun s ->
        S.squeeze s |> S.word_wrap ~hard_limit:72 |> out "%s\n\n") fmt in
  let code fmt =
    ksprintf (fun s -> out "%s\n\n" (S.indent s)) fmt in
  let links = ref [] in
  let link name url =
    links := (name, url) :: !links;
    sprintf "[%s]" name in
  let out_links () =
    List.iter (List.rev !links) (fun (n, u) -> out "[%s]: %s\n" n u);
    if List.length !links > 0 then out "\n";
    links := [];
  in
  let section fmt =
    out_links ();
    ksprintf (fun s -> out "\n%s\n" (S.underline s '=')) fmt in
  let subsection fmt =
    out_links ();
    ksprintf (fun s -> out "\n%s\n" (S.underline s '-')) fmt in
  section "Welcome to Gencore's Command Line Application";
  par "This manual describes the basic usage of `gencore` with examples. \
       More details are available through the `-help` options of every \
       sub-command.";
  subsection "General Usage";
  par "The application uses a hierarchy of commands which you can explore \
      using:";
  code "gencore help -r";
  par "Note that every sub-command name can be shortened as long as it \
       is not ambiguous, i.e. `gencore authentication` is equivalent to \
       `gencore auth`.";
  subsection "Initialization";
  par "The application uses a configuration file which can be created with the \
      `gencore configuration initialize` sub-command:";
  code "gencore config init <host> <port>";
  par "where <host> and <port> are the location of the Gencore server.";
  par "If the username on your machine is not your NetID you need to provide \
       the option `-user <netID>`.";
  par "`gencore` will contact the server and ask you to authenticate \
       yourself through your NetID (or with your Gencore password if any). \
       Then it will request an authentication token (independent of your \
       password) and store it together with other options in the \
       configuration file (i.e. your password is never stored).";
  par "Your can specify an alternate configuration file to use (option `-c`) \
       but then you have to specify it every time.";
  par "You can give a name to the authentication token (option `-token-name`) \
       to simplify later your auth-token management (sub-command \
       `authentication`).";
  par "After you have successfully configured `gencore` you can test the \
       connection settings with:";
  code "gencore self-test";
  subsection "The “Libraries” Command";
  par "The idea here is to query our database about your libraries. By “your” \
       we mean the libraries you have been associated with, i.e. the ones that \
       show up in the %s section of the website."
    (link "/libraries"
       "https://gencore.bio.nyu.edu/libraries");
  par "The querying is for now fairly limited, let's see a few examples.";
  subsection "About this document";
  let markdown = link "Markdown" "http://en.wikipedia.org/wiki/Markdown" in
  let pandoc = link "Pandoc" "http://johnmacfarlane.net/pandoc/" in
  par "You may view this document with the command `gencore manual` or, more \
       progressively, with `gencore manual | less`, but if you have a \
       %s processor like %s you may view the manual in your favorite \
       web-browser:" markdown pandoc;
  code "gencore manual | pandoc -s -o gencore-manual.html";
  out_links ();
  Buffer.contents outbuf

let () =
  Command_line.(
    run ~version:"0"
      (group ~summary:"Gencore's command-line application" [
          ("configuration",
           group ~summary:"Manage the configuration of the application" [
             ("initialize", init_command);
           ]);
          ("self-test", self_test_command);
          ("libraries", list_libraries_command);
          ("authentication", auth_command);
          ("manual", basic ~summary:"Display the user manual"
             Spec.(empty)
             (fun () -> printf "%s\n" (manual ())));
        ]))
