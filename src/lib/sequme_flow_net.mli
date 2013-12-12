(** High-level TCP + TLS connection handling. *)

(**
To see this module in action check
   [src/test/flow_net_test.ml]
   (on 
   {{:https://github.com/agarwal/sequme/blob/master/src/test/flow_net_test.ml
     }Github\@master}).


*)

(** {3 TLS Initialization} *)

val init_tls :  unit -> unit
(** Initialize the SSL library. *)

(** {3 Generic Connection Handle} *)

(** A connection is full duplex and can be shut down. *)
type connection
val in_channel: connection -> Lwt_io.input_channel 
val out_channel: connection -> Lwt_io.output_channel 

(**  Note that [shutdown connection] closes the channels. *) 
val shutdown : connection -> (unit, [> `io_exn of exn ]) Sequme_flow.t

(** {3 Client Connection} *)

type connection_specification = [
| `tls of
    [ `anonymous | `with_certificate of string * string ]
  * [ `verify_server | `allow_self_signed ]
| `plain
]
(** Specification of the kind of connection (for the function [connect]). *)
    
val connect: address:Lwt_unix.sockaddr -> connection_specification ->
  (connection, [> `io_exn of exn | `tls_context_exn of exn ]) Sequme_flow.t
(** Connect to the server at [address]. *)

(** {3 Server Establishment} *)

val plain_server :
  ?on_error:(([> `accept_exn of Core.Std.Exn.t
              | `io_exn of exn
              | `not_an_ssl_socket
              | `tls_accept_error of exn ]
                 as 'errors) ->
             (unit, [> `io_exn of exn ]) Sequme_flow.t) ->
  port:int ->
  (connection -> (unit, 'errors) Sequme_flow.t) ->
  (unit, [> `io_exn of exn | `socket_creation_exn of exn ]) Sequme_flow.t
(** Start a “plain” TCP server on port [port]. This function returns
    immediately, the “accept-loop” runs in {i Lwt} threads.

    The function [on_error] is an error handler, it will be called on
    "acceptation" errors (c.f.
    {{:http://ocsigen.org/lwt/api/Lwt_unix#VALaccept_n}Lwt_unix.accept_n})
    {b and} on the remaining errors of the handler (but errors of the
    [on_error] function itself will be ignored).

    Example: {[
    plain_server ~port:4242
      ~on_error:(fun e -> logf "Error: %s" (string_of_error e))
      (fun connection ->
         Sequme_flow_io.bin_send connection#out_channel "Hello !!"
         >>= fun () ->
         connection#shutdown)
    ]}
*)

val tls_server :
  ?on_error:(([> `accept_exn of Core.Std.Exn.t
              | `io_exn of exn
              | `not_an_ssl_socket
              | `tls_accept_error of exn ]
                 as 'a) ->
             (unit, [> `io_exn of exn ]) Sequme_flow.t) ->
  port:int ->
  cert_key:string * string ->
  (connection -> (unit, 'a) Sequme_flow.t) ->
  (unit, [> `socket_creation_exn of exn | `tls_context_exn of exn ])
           Sequme_flow.t
(** Like [plain_server] but with a TLS layer, the server will be
    authenticated with [~cert_key:("cert.crt", "k.key")]. *)

type client_check_result =
[ `expired of string * Core.Std.Time.t
| `not_found of string
| `revoked of string * Core.Std.Time.t
| `valid of string ]
(** The result type expected from [check_client_certificate] functions. *)

type client_kind =
[ `anonymous_client
| `invalid_client of
    [ `expired of string * Core.Std.Time.t
    | `not_found of string
    | `revoked of string * Core.Std.Time.t
    | `wrong_certificate ]
| `valid_client of string ]
(** The different kinds of clients that a authenticating TLS server
    handler has to treat separately.

    In the current implementation, the case [`anonymous_client] is
    never used, clients without certificate are seen as
    [`invalid_client `wrong_certificate].
*)

val authenticating_tls_server :
  ca_certificate:string ->
  check_client_certificate:(Ssl.certificate ->
                            (client_check_result,
                             [> `accept_exn of Core.Std.Exn.t
                             | `io_exn of exn
                             | `not_an_ssl_socket
                             | `tls_accept_error of exn ]
                               as 'a)
                              Sequme_flow.t) ->
  ?on_error:('a -> (unit, [> `io_exn of exn ]) Sequme_flow.t) ->
  port:int ->
  cert_key:string * string ->
  (connection -> client_kind ->
   (unit, 'a) Sequme_flow.t) ->
  (unit, [> `socket_creation_exn of exn | `tls_context_exn of exn ])
    Sequme_flow.t
(** Start an authenticating TLS server,  the [ca_certificate] is used to check
    client certificates, and the function [check_client_certificate] to
    decide about the CA-validity of the certificate once it has been validated
    with respect to the TLS protocol. *)

val authenticating_tls_server_with_ca :
  ca:Sequme_flow_certificate_authority.t ->
  ?on_error:(([> `accept_exn of Core.Std.Exn.t
              | `io_exn of exn
              | `not_an_ssl_socket
              | `tls_accept_error of exn
              | `wrong_subject_format of string ]
                 as 'a) ->
             (unit, [> `io_exn of exn ]) Sequme_flow.t) ->
  port:int ->
  cert_key:string * string ->
  (connection -> client_kind ->
   (unit, 'a) Sequme_flow.t) ->
  (unit, [> `socket_creation_exn of exn | `tls_context_exn of exn ])
    Sequme_flow.t
(** Do like [authenticating_tls_server] but use a
    [Sequme_flow_certificate_authority.t] to provide the
    [ca_certificate] and the [check_client_certificate] function. *)

