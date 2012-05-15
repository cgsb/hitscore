open Hitscore_std
open Hitscore_layout
open Hitscore_access_rights
open Hitscore_db_backend

open Hitscore_common
open Common


module Unaligned_coercion:
  Hitscore_function_interfaces.COERCE_B2F_UNALIGNED
    
  = struct

  let run ~dbh ~configuration ~input =
    let layout = Classy.make dbh in
    layout#bcl_to_fastq_unaligned#get input
    >>= fun b2fu ->
    layout#add_coerce_b2f_unaligned
      ~input ~recomputable:true ~recompute_penalty:0.1 ()
    >>= fun inserted ->
    inserted#get >>= fun inserted ->
    inserted#set_started >>= fun () ->
    let post_insert_work =
      layout#add_link_volume 
        ~kind:`generic_fastqs_dir ~pointer:b2fu#directory#pointer ()
      >>= fun vol ->
      layout#add_generic_fastqs ~directory:vol#pointer ()
      >>= fun res ->
      inserted#set_succeeded res#pointer
    in
    double_bind post_insert_work ~ok:return
      ~error:(fun e -> inserted#set_failed >>= fun () -> error e)

    
  end
