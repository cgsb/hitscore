include Core.Std

module Lwt_config = struct
  include Lwt
  include Lwt_chan
  let map_sequential l ~f = Lwt_list.map_s f l
  let log_error s = output_string Lwt_io.stderr s >>= fun () -> flush Lwt_io.stderr
    
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

module Flow = Sequme_flow_monad.Make(Lwt_config)
include Flow
  
module PG = PGOCaml_generic.Make(Flow.IO)
(* let (|>) x f = f x *)

