open Hitscore_std
  
open Hitscore_layout
open Hitscore_access_rights
open Hitscore_db_backend
open Hitscore_common
open Hitscore_configuration

module Protocol = struct

  type up = [
  | `log of string
  ]
  with bin_io, sexp

  type down = [
  | `user_message of string
  ]
  with bin_io, sexp
    
    
  type serialization_mode = [ `binary | `s_expression ]

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
      Bin_prot.Utils.bin_read_stream ~max_size:(String.length msg)
        ~read:(fun buf ~pos ~len ->
          Bin_prot.Common.blit_string_buf msg buf
            ~src_pos:pos ~dst_pos:pos ~len)
        bin_reader
    | `s_expression ->
      Sexp.of_string msg |! of_sexp
      
  let string_of_up ~mode up =
    string_of_message ~mode ~bin_writer:bin_writer_up ~sexp_of:sexp_of_up up
  let string_of_down ~mode down =
    string_of_message ~mode ~bin_writer:bin_writer_down ~sexp_of:sexp_of_down down

  let up_of_string_exn ~mode up =
    message_of_string ~mode ~bin_reader:bin_reader_up ~of_sexp:up_of_sexp up
  let down_of_string_exn ~mode down =
    message_of_string ~mode ~bin_reader:bin_reader_down ~of_sexp:down_of_sexp down

end


module Authentication = struct

  let hash_password person_id password =
    let open Cryptokit in
    let to_hash = sprintf "gencore:%d:%s" person_id password in
    transform_string (Hexa.encode ()) (hash_string (Hash.sha256 ()) to_hash)



end
