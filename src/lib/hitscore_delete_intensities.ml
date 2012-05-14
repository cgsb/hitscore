open Hitscore_std
open Hitscore_layout
open Hitscore_access_rights
open Hitscore_db_backend

open Hitscore_common
open Common

module Delete_intensities:
  Hitscore_function_interfaces.DELETE_INTENSITIES
  = struct


  let register ~dbh ~hiseq_raw =
    check_hiseq_raw_availability ~dbh ~hiseq_raw
    >>= fun (availability, all_deleted) ->
    Layout.Record_hiseq_raw.(
      Access.Hiseq_raw.get ~dbh hiseq_raw
      >>= fun hiseq_raw_info ->
      Layout.Function_delete_intensities.(
        Access.Delete_intensities.add_evaluation ~dbh
          ~recomputable:false ~recompute_penalty:0.
          ~hiseq_raw ~availability
        >>= fun evaluation ->
        Access.Delete_intensities.set_started ~dbh evaluation
        >>= fun () ->
        let {flowcell_name; read_length_1; read_length_2; read_length_index;
             with_intensities; run_date; host; hiseq_dir_name;} =
          hiseq_raw_info.g_value in
        Access.Hiseq_raw.add_value ~dbh
          ~flowcell_name ~read_length_1 ?read_length_2 ?read_length_index
          ~with_intensities:false ~run_date ~host ~hiseq_dir_name
        >>= fun new_hiseq_raw ->
        Access.Inaccessible_hiseq_raw.add_value
          ~dbh ~deleted:(Array.of_list (hiseq_raw :: (List.dedup all_deleted)))
        >>= fun (_: Layout.Record_inaccessible_hiseq_raw.pointer) ->
        Access.Delete_intensities.set_succeeded ~dbh evaluation ~result:new_hiseq_raw
        >>= fun () ->
        Access.Log.add_value ~dbh
          ~log:(sprintf "(delete_intensities success %d)" evaluation.id)
        >>= fun (_: Layout.Record_log.pointer) ->
        return evaluation))
                        
  end
