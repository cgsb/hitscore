open Hitscore_std
  
open Hitscore_layout
open Hitscore_access_rights
open Hitscore_db_backend
open Hitscore_common
open Hitscore_configuration

module Protocol = struct

  type up = [
  | `log of string
  | `new_token of string * string * string * string
  | `authenticate of string * string * string
  | `get_simple_info
  ]
  with bin_io, sexp

  type person_simple_info = {
    psi_print_name: string option;
    psi_full_name: string * string option * string option * string;
    psi_emails: string list;
    psi_login: string option;
    psi_affiliations: string list list;
  }
  with bin_io, sexp

  type down = [
  | `user_message of string
  | `token_updated
  | `token_created
  | `authentication_successful
  | `simple_info of person_simple_info
  | `error of [
    | `not_implemented
    | `server_error of string
    | `wrong_authentication
  ]
  ]
  with bin_io, sexp
    
  type serialization_mode = [ `binary | `s_expression ] with sexp

  let string_of_message ~mode ~bin_writer ~sexp_of msg =
    match mode with
    | `binary ->
      (* let open Bin_prot.Common in *)
      let buf = Bin_prot.Utils.bin_dump ~header:true bin_writer msg in
      let len = Bigarray.Array1.dim buf in
      let buffer = String.make len 'B' in 
      Bin_prot.Common.blit_buf_string buf buffer ~len;
      buffer
    | `s_expression ->
      Sexp.to_string_hum (sexp_of msg)

  let message_of_string ~mode ~bin_reader ~of_sexp msg =
    match mode with
    | `binary ->
      let read_pos = ref 0 in
      Bin_prot.Utils.bin_read_stream ~max_size:(String.length msg * 30)
        ~read:(fun buf ~pos ~len ->
          (* eprintf "======== READ: %d %d (%S)\n%!" pos len *)
          (* (String.sub msg ~pos:!read_pos ~len); *)
          let s = 
            Bin_prot.Common.blit_string_buf msg buf
              ~src_pos:!read_pos ~dst_pos:pos ~len in
          read_pos := !read_pos + len;
          s)
        bin_reader
    | `s_expression ->
      Sexp.of_string msg |! of_sexp
      
  let string_of_up ~mode up =
    string_of_message ~mode ~bin_writer:bin_writer_up ~sexp_of:sexp_of_up up
  let hum_string_of_up up =
    string_of_message ~mode:`s_expression
      ~bin_writer:bin_writer_up ~sexp_of:sexp_of_up up

  let string_of_down ~mode down =
    string_of_message ~mode ~bin_writer:bin_writer_down ~sexp_of:sexp_of_down down
  let hum_string_of_down down =
    string_of_message ~mode:`s_expression
      ~bin_writer:bin_writer_down ~sexp_of:sexp_of_down down

  let up_of_string_exn ~mode up =
    message_of_string ~mode ~bin_reader:bin_reader_up ~of_sexp:up_of_sexp up
  let down_of_string_exn ~mode down =
    message_of_string ~mode ~bin_reader:bin_reader_down ~of_sexp:down_of_sexp down

  type serialization_error = 
  [ `message_serialization of serialization_mode * string * exn ]

  let up_of_string ~mode up =
    try Ok (up_of_string_exn ~mode up)
    with e -> Error (`message_serialization (mode, up, e))
  let down_of_string ~mode down =
    try Ok (down_of_string_exn ~mode down)
    with e -> Error (`message_serialization (mode, down, e))

  let serialization_mode_flag () =
    let open Sequme_flow_app_util.Command_line in
    let open Spec in
    step (fun k ->
      function
      | true -> k ~mode:`s_expression
      | false -> k ~mode:`binary)
    ++ flag "sexp-messages" no_arg
      ~doc:" exchange S-Expressions instead of binary blobs (for debugging)"
end


module Authentication = struct

  let hash_password person_id password =
    let open Cryptokit in
    let to_hash = sprintf "gencore:%d:%s" person_id password in
    transform_string (Hexa.encode ()) (hash_string (Hash.sha256 ()) to_hash)

  let check_chained ?(pam_service="") ~person ~password () =
    if person#password_hash = Some (hash_password person#g_id password)
    then return true
    else begin
      match person#login with
      | Some s ->
        let wrap_pam f a = try f a; Ok true with e -> Ok false in
        let auth () =
          wrap_pam (Simple_pam.authenticate pam_service s) (password) 
        in
        Lwt_preemptive.detach auth ()
      | None  -> return false
    end



end
