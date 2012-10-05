

open Hitscore
open Core.Std
open Sequme_flow
open Sequme_flow_app_util
module Flow_net = Sequme_flow_net



let send_and_recv connection =
  let message = `log "I'm connected !!" in
  Sequme_flow_io.bin_send (Flow_net.out_channel connection)
    (Communication.Protocol.string_of_up ~mode:`s_expression message)
  >>= fun () ->
  Sequme_flow_io.bin_recv (Flow_net.in_channel connection)
  >>= fun response ->
  msg "Got %S from server"  response

type info_error =
[ `bin_recv of [ `exn of exn | `wrong_length of int * string ]
| `bin_send of [ `exn of exn | `message_too_long of string ]
| `io_exn of exn
| `tls_context_exn of exn ]
with sexp_of
  
let info_command =
  let open Command_line in
  basic ~summary:"Get information"
    Spec.(
      anon (sequence "ARGS" string)
    )
    (fun args ->
      run_flow ~on_error:(fun e ->
        eprintf "Client ends with Errors: %s"
          (Sexp.to_string_hum (sexp_of_info_error e)))
        begin
          Sequme_flow_net.init_tls ();
          Sequme_flow_net.connect
            ~address:Unix.(ADDR_INET (Inet_addr.localhost, 4002))
            (`tls (`anonymous, `allow_self_signed))
          >>= fun connection ->
          dbg "Anonymously Connected on 4002 " >>= fun () ->
          send_and_recv connection
          >>= fun () ->
          Flow_net.shutdown connection
          >>= fun () ->
          dbg "command not implemented"
        end)

    
let () =
  Command_line.(
    run ~version:"0"
      (group ~summary:"Gencore's command-line application" [
        ("info", info_command);
      ]))
