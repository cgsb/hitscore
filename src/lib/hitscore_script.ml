(** Module useful for OCaml scripting with [#require "hitscore"]. *)
open Hitscore_std
open Hitscore_configuration
open Hitscore_db_backend
open Hitscore_layout
  

(** Functions to get database handles (exported to [Hitscore]). *)
module Database = struct
    
  let db_connect ?log t =
    let open Configuration in
    let host, port, database, user, password =
      db_host t, db_port t, db_database t, db_username t, db_password t in
    Backend.connect ?host ?port ?database ?user ?password ?log ()
      
  let db_disconnect t dbh = 
    Backend.disconnect ~dbh

  let with_database ~configuration ?log f =
    db_connect ?log configuration 
    >>= fun dbh ->
    let m = f ~dbh in
    double_bind m
      ~ok:(fun x -> db_disconnect configuration dbh >>= fun () -> return x)
      ~error:(fun x -> db_disconnect configuration dbh >>= fun () -> error x)
end
open Database
  
(** Get the default configuration file path. *)
let default_config_file () =
  let home = Sys.getenv_exn "HOME" in
  sprintf "%s/.config/hitscore/config.sexp" home
      
(** Get the configuration from a string like in the command line:
[[config-file:]profile]. *)
let configuration profile =
  let config_file, profile_name =
    match String.split profile ~on:':' with
    | [ one ] ->
      (default_config_file (), one)
    | [ one; two ] ->
      (one, two)
    | _ -> failwithf "Can't understand: %s" profile ()
  in
  let config = In_channel.(with_file config_file ~f:input_all) in
  let hitscore_config =
    let open Hitscore_configuration.Configuration in
    match Result.(parse_str config >>= fun c -> use_profile c profile_name) with
    | Ok c -> c
    | Error (`configuration_parsing_error e) ->
      eprintf "Error while parsing configuration: %s\n" (Exn.to_string e);
      failwith "STOP"
    | Error (`profile_not_found s) ->
      eprintf "Profile %S not found in config-file\n" s;
      failwith "STOP"
  in
  hitscore_config

(**/**)
(** Global variable containing the “default” configuration (please use only in
throw-away scripts). *)
let global_configuration =
  ref (None : Hitscore_configuration.Configuration.local_configuration option)
(**/**)

(** Set a global “default” configuration (please use only in
throw-away scripts). *)
let configure_global profile =
  global_configuration := Some (configuration profile)
     
(** {3 Error Handling} *)
(** For a given type error provide an exceptionful function:
    [Runner.run: ('a, error) Flow.t -> 'a] (it uses [Lwt_main.run]). *)
module Make_runner
  (E : sig type error val sexp_of_error: error -> Sexp.t end) =
struct
  let error_to_string e = Sexp.to_string_hum (E.sexp_of_error e)
  let run m =
    match Lwt_main.run m with
    | Ok o -> o
    | Error e -> failwithf "Runner_error: %s" (error_to_string e) ()
end
(**
[Make_runner] can be used like that (requires camlp4 working):

{[
    module Runner = struct
      type error = [
      | `io_exn of exn
      | `some_other_error
      ] with sexp_of
    end
]}
*)

(** A silent “runner” ([Silent.run] won't give details about the error). *)
module Silent = struct
  let run m =
    match Lwt_main.run m with
    | Ok o -> o
    | Error e -> failwithf "Run_error: NO INFO" ()
end

(** A runner that forces the type [error] to be [string]. *)
module String_error = Make_runner (struct type error = string with sexp_of end)

(** [String_error] can be used like:

{[
String_error.run (
    do_something ()
    >>< begin function
    | Ok o -> return o
    | Error `one -> error "one"
    | Error (`two s) -> ksprintf error "two: %s" s
    | Error `etc -> error "…"
    end
]}

*)

(** {3 Utilities } *)
  
(** Add a new user to the database. The default [configuration] is the
global one, the default roles are [[`user]]. *)
let create_user ?configuration ?(roles=[`user]) ?net_id ~email (given, family) =
  let configuration =
    let open Option in
    match configuration with
    | Some s -> s
    | None ->
      match !global_configuration with
      | Some s -> s
      | None -> failwith "Not configured"
  in
  with_database configuration (fun ~dbh ->
    let layout = Classy.make dbh in
    layout#add_person 
      ~email ~roles:(Array.of_list roles)
      ~secondary_emails:[| |]
      ~auth_tokens:[| |]
      ~affiliations:[| |]
      ?login:net_id ~given_name:given ~family_name:family ()
    >>= fun p ->
    return ()
  )
