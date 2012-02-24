open Hitscore_std


module Preemptive_threading_config : 
  Hitscore_interfaces.IO_CONFIGURATION with type 'a t = 'a = struct
    include PGOCaml.Simple_thread
    let map_sequential l ~f = List.map ~f l
    let log_error = eprintf "%s"
    let catch f e =
      try f () with ex -> e ex

    exception Wrong_status of Unix.Process_status.t
    let system_command s =
      let status = Unix.system s in
      if (Unix.Process_status.is_ok status) then ()
      else raise (Wrong_status status)

    let write_string_to_file content file =
      Out_channel.(with_file file ~f:(fun o ->
        output_string o content))
      

end



module Make (IO_configuration : Hitscore_interfaces.IO_CONFIGURATION) = struct

  module Result_IO = Hitscore_result_IO.Make(IO_configuration)

  module Layout = Hitscore_db_access.Make(Result_IO)

  module Configuration = Hitscore_configuration

  module Access_rights = 
    Hitscore_access_rights.Make (Configuration) (Result_IO) (Layout)
           
  module Common = 
    Hitscore_common.Make
      (Configuration) (Result_IO) (Layout) (Access_rights)

  module Assemble_sample_sheet = 
    Hitscore_assemble_sample_sheet.Make
      (Configuration) (Result_IO) (Layout) (Access_rights)

  module Bcl_to_fastq =
    Hitscore_bcl_to_fastq.Make 
      (Configuration) (Result_IO) (Layout) (Access_rights)

  module Unaligned_delivery =
    Hitscore_unaligned_delivery.Make 
      (Configuration) (Result_IO) (Layout) (Access_rights)

  module Hiseq_raw = Hitscore_hiseq_raw
    
  module B2F_unaligned = Hitscore_b2f_unaligned

  module Delete_intensities =
    Hitscore_delete_intensities.Make 
      (Configuration) (Result_IO) (Layout) (Access_rights)

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

  let with_database ~configuration ~f =
    let open Result_IO in
    db_connect configuration 
    >>= fun dbh ->
    let m = f ~dbh in
    double_bind m
      ~ok:(fun x -> db_disconnect configuration dbh >>= fun () -> return x)
      ~error:(fun x -> db_disconnect configuration dbh >>= fun () -> error x)


end

