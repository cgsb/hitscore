open Hitscore_std

module Flow = Flow

  (*
module Backend_queries = Hitscore_db_backend.Sql_query
module Backend = Hitscore_db_backend.Backend
  *)
include Hitscore_db_backend

include Hitscore_layout

  (* module Layou_access = Hitscore_db_access.Make(Flow) *)

module Configuration = Hitscore_configuration.Configuration

module Access_rights = Hitscore_access_rights.Access_rights
  
module Common = Hitscore_common.Common

module Assemble_sample_sheet = 
  Hitscore_assemble_sample_sheet. Assemble_sample_sheet

module Bcl_to_fastq = Hitscore_bcl_to_fastq. Bcl_to_fastq

module Unaligned_delivery =
  Hitscore_unaligned_delivery. Unaligned_delivery

module Hiseq_raw = Hitscore_hiseq_raw
  
module B2F_unaligned = Hitscore_b2f_unaligned

module Delete_intensities =
  Hitscore_delete_intensities. Delete_intensities

module Coerce_b2f_unaligned =
  Hitscore_generic_fastqs.Unaligned_coercion

module Fastx_quality_stats =
  Hitscore_fastx_quality_stats. Fastx_quality_stats

include Hitscore_broker

let db_connect ?log t =
  let open Configuration in
  let host, port, database, user, password =
    db_host t, db_port t, db_database t, db_username t, db_password t in
  Backend.connect ?host ?port ?database ?user ?password ?log ()
    
let db_disconnect t dbh = 
  Backend.disconnect ~dbh

let with_database ~configuration ?log f =
  let open Flow in
  db_connect ?log configuration 
  >>= fun dbh ->
  let m = f ~dbh in
  double_bind m
    ~ok:(fun x -> db_disconnect configuration dbh >>= fun () -> return x)
    ~error:(fun x -> db_disconnect configuration dbh >>= fun () -> error x)


