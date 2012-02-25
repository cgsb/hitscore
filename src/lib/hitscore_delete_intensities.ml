


module Make
  (Common: Hitscore_common.COMMON):
  Hitscore_function_interfaces.DELETE_INTENSITIES
  with module Common = Common

  = struct

  module Common = Common
  open Common

  open Hitscore_std
  open Result_IO

  let register ~dbh ~hiseq_raw =
    Layout.Record_inaccessible_hiseq_raw.(
      get_all ~dbh >>= fun all ->
      of_list_sequential all ~f:(fun p ->
        get ~dbh p >>= fun {g_last_modified; deleted; _} ->
        begin match g_last_modified with
        | Some t -> return (p, t, Array.to_list deleted)
        | None -> error (`layout_inconsistency 
                            (`record_inaccessible_hiseq_raw,
                             `no_last_modified_timestamp p))
        end)
      >>| List.sort ~cmp:(fun a b -> compare (snd3 b) (snd3 a))
      >>=
        (function
        | [] ->
          printf "There were no inaccessible_hiseq_raw => \
                       creating the empty one.\n";
          Layout.Record_inaccessible_hiseq_raw.add_value ~dbh ~deleted:[| |]
          >>= fun pointer ->
          return (pointer, [])
        | (h, ts, l) :: t as whole-> 
          printf "Last inaccessible_hiseq_raw: %ld on %s\n"
            h.Layout.Record_inaccessible_hiseq_raw.id
            (Time.to_string ts);
          return (h, List.map whole trd3 |! List.flatten))
      >>= fun (last_avail, all_deleted) ->
      if List.exists all_deleted ((=) hiseq_raw) then
        error `hiseq_dir_deleted
      else
        return (last_avail, all_deleted)
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
          return successful_evaluation)))
                        
  end
