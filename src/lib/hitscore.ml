open Hitscore_std


module Make (IO_configuration : Sequme_flow_monad.IO_CONFIGURATION) = struct

  module Flow = Sequme_flow_monad.Make(IO_configuration)

  module Layout = Hitscore_db_access.Make(Flow)

  module Configuration = Hitscore_configuration

  module Access_rights = 
    Hitscore_access_rights.Make (Configuration) (Flow) (Layout)
           
  module Common = 
    Hitscore_common.Make
      (Configuration) (Flow) (Layout) (Access_rights)

  module Assemble_sample_sheet = 
    Hitscore_assemble_sample_sheet.Make (Common)

  module Bcl_to_fastq =
    Hitscore_bcl_to_fastq.Make (Common)

  module Unaligned_delivery =
    Hitscore_unaligned_delivery.Make (Common)

  module Hiseq_raw = Hitscore_hiseq_raw
    
  module B2F_unaligned = Hitscore_b2f_unaligned

  module Delete_intensities =
    Hitscore_delete_intensities.Make (Common)

  module Coerce_b2f_unaligned =
    Hitscore_generic_fastqs.Make_unaligned_coercion (Common)

  module Fastx_quality_stats =
    Hitscore_fastx_quality_stats.Make (Common)

  module Broker = Hitscore_broker.Make(Common)

  let db_connect t =
    let open Configuration in
    match db_host t, db_port t, db_database t, db_username t, db_password t with
    | Some host, Some port, Some database, Some user, Some password ->
      Flow.(bind_on_error
                   (catch_io (Layout.PGOCaml.connect
                                ~host ~port ~database ~user ~password) ())
                   (fun e -> error (`pg_exn e)))
    | _ -> 
      Flow.(bind_on_error
                   (catch_io Layout.PGOCaml.connect ())
                   (fun e -> error (`pg_exn e)))
        
  let db_disconnect t dbh = 
    Flow.(bind_on_error
                 (catch_io Layout.PGOCaml.close dbh )
                 (fun e -> error (`pg_exn e)))

  let with_database ~configuration ~f =
    let open Flow in
    db_connect configuration 
    >>= fun dbh ->
    let m = f ~dbh in
    double_bind m
      ~ok:(fun x -> db_disconnect configuration dbh >>= fun () -> return x)
      ~error:(fun x -> db_disconnect configuration dbh >>= fun () -> error x)


end

