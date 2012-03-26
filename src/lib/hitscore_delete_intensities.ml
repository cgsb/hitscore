


module Make
  (Common: Hitscore_common.COMMON):
  Hitscore_function_interfaces.DELETE_INTENSITIES
  with module Common = Common

  = struct

  module Common = Common
  open Common

  open Hitscore_std
  open Flow

  let register ~dbh ~hiseq_raw =
    check_hiseq_raw_availability ~dbh ~hiseq_raw
    >>= fun (availability, all_deleted) ->
    Layout.Record_hiseq_raw.(
      get ~dbh hiseq_raw
      >>= fun hiseq_raw_info ->
      Layout.Function_delete_intensities.(
        add_evaluation ~dbh
          ~recomputable:false ~recompute_penalty:0.
          ~hiseq_raw ~availability >>= set_started ~dbh
        >>= fun evaluation ->
        let {flowcell_name; read_length_1; read_length_2; read_length_index;
             with_intensities; run_date; host; hiseq_dir_name;} =
          hiseq_raw_info in
        add_value ~dbh
          ~flowcell_name ~read_length_1 ?read_length_2 ?read_length_index
          ~with_intensities:false ~run_date ~host ~hiseq_dir_name
        >>= fun new_hiseq_raw ->
        Layout.Record_inaccessible_hiseq_raw.add_value
          ~dbh ~deleted:(Array.of_list (hiseq_raw :: (List.dedup all_deleted)))
        >>= fun (_: Layout.Record_inaccessible_hiseq_raw.pointer) ->
        set_succeeded ~dbh evaluation ~result:new_hiseq_raw
        >>= fun successful_evaluation ->
        Layout.Record_log.add_value ~dbh
          ~log:(sprintf "(delete_intensities success %ld)"
                  successful_evaluation.id)
        >>= fun (_: Layout.Record_log.pointer) ->
        return successful_evaluation))
                        
  end
