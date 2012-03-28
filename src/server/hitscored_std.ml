
include Core.Std

  
let global_log_app_name = ref ">>"

module Lwt_config = struct
  include Lwt
  include Lwt_chan
  let map_sequential l ~f = Lwt_list.map_s f l
  let log_error s =
    let str = sprintf "%s: %s\n" !global_log_app_name s in
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

  let ssl_connect socket ssl_context =
    wrap_io (Lwt_ssl.ssl_connect socket) ssl_context

  let ssl_shutdown socket =
    wrap_io Lwt_ssl.ssl_shutdown socket

end
