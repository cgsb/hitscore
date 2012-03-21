


module Make_unaligned_coercion
  (Common: Hitscore_common.COMMON):
  Hitscore_function_interfaces.COERCE_B2F_UNALIGNED
  with module Common = Common

  = struct

  module Common = Common
  open Common

  open Hitscore_std
  open Result_IO

  let run ~dbh ~configuration ~input =
    Layout.Record_bcl_to_fastq_unaligned.(
      get ~dbh input >>= fun {directory} -> return directory)
    >>= fun unaligned_pointer ->
    Layout.Function_coerce_b2f_unaligned.(
      add_evaluation ~dbh ~input ~recomputable:true ~recompute_penalty:0.1
      >>= fun inserted ->
      set_started ~dbh inserted >>= fun started ->
      let post_insert_work =
        Layout.File_system.(
          add_link ~dbh ~kind:`generic_fastqs_dir unaligned_pointer
          >>= fun vol_pointer ->
          Layout.Record_generic_fastqs.add_value ~dbh ~directory:vol_pointer
          >>= fun result ->
          set_succeeded ~dbh ~result started)
      in
      double_bind post_insert_work ~ok:return
        ~error:(fun e ->
          set_failed ~dbh started >>= fun _ ->
          error e))

    
  end
