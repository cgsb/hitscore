open Hitscore_std


module Preemptive_threading_config : 
  Hitscore_interfaces.IO_CONFIGURATION with type 'a t = 'a = struct
    include PGOCaml.Simple_thread
    let map_sequential l ~f = List.map ~f l
    let log_error = eprintf "%s"
    let catch f e =
      try f () with ex -> e ex
end



module Make (IO_configuration : Hitscore_interfaces.IO_CONFIGURATION) = struct

  module Result_IO = Hitscore_result_IO.Make(IO_configuration)

  module Layout = Hitscore_db_access.Make(Result_IO)

  module Configuration = Hitscore_configuration

  module ACL = Hitscore_acl.Make (Configuration) (Result_IO) (Layout)
           
  module Assemble_sample_sheet = 
    Hitscore_assemble_sample_sheet.Make
      (Configuration) (Result_IO) (Layout) (ACL)

  module Bcl_to_fastq =
    Hitscore_bcl_to_fastq.Make (Configuration) (Result_IO) (Layout) (ACL)

  module Hiseq_raw = Hitscore_hiseq_raw
    
  module B2F_unaligned = Hitscore_b2f_unaligned

  let db_connect t =
    let open Configuration in
    match db_host t, db_port t, db_database t, db_username t, db_password t with
    | Some host, Some port, Some database, Some user, Some password ->
      Result_IO.(bind_on_error
                   (catch_io (Layout.PGOCaml.connect
                                ~host ~port ~database ~user ~password) ())
                   (fun e -> error (`pg_exn e)))
    | _ -> 
      Result_IO.(bind_on_error
                   (catch_io Layout.PGOCaml.connect ())
                   (fun e -> error (`pg_exn e)))
        
  let db_disconnect t dbh = 
    Result_IO.(bind_on_error
                 (catch_io Layout.PGOCaml.close dbh )
                 (fun e -> error (`pg_exn e)))

end

