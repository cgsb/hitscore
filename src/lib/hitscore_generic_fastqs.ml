


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
      get ~dbh input >>= fun {directory} ->
      Common.paths_of_volume ~dbh ~configuration directory
      >>= fun paths ->
      match paths with
      | [unaligned] -> 
        eprintf "Unaligned: %s\n%!" unaligned;
        return unaligned
      | l -> error (`wrong_unaligned_volume l))
    >>= fun unaligned ->
    Layout.Function_coerce_b2f_unaligned.(
      add_evaluation ~dbh ~input ~recomputable:true ~recompute_penalty:0.1
      >>= fun inserted ->
      set_started ~dbh inserted >>= fun started ->
      let system_work =
        Layout.File_system.(
          add_volume ~dbh ~kind:`generic_fastqs_dir
            ~hr_tag:"_b2f" ~files:[Opaque ("Unaligned", `opaque)]
          >>= fun vol_pointer ->
          get_volume_entry ~dbh vol_pointer
          >>= fun vol_entry ->
          begin match Configuration.path_of_volume
              configuration (entry_unix_path vol_entry) with
              | Some p -> return p 
              | None -> error `root_directory_not_configured
          end
          >>= fun new_vol_path ->
          eprintf "new_vol_path: %s\n%!" new_vol_path;
          ksprintf system_command "mkdir -p %s" new_vol_path >>= fun () ->
          ksprintf system_command "cd %s && ln -s %s" new_vol_path unaligned
          >>= fun () ->
          Layout.Record_generic_fastqs.add_value ~dbh ~directory:vol_pointer
          >>= fun result ->
          set_succeeded ~dbh ~result started)
      in
      double_bind system_work ~ok:return
        ~error:(fun e ->
          set_failed ~dbh started >>= fun _ ->
          error e))

    
  end
