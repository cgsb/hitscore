
module type COMMON = sig

  (** Local definition of the configuration. *)
  module Configuration : Hitscore_interfaces.CONFIGURATION

  (** Local definition of Result_IO.  *)
  module Result_IO : Hitscore_interfaces.RESULT_IO

  (** Local definition of Layout *)
  module Layout : Hitscore_layout_interface.LAYOUT
    with module Result_IO = Result_IO
    with type 'a PGOCaml.monad = 'a Result_IO.IO.t
    (* This last one is used by Assemble_Sample_Sheet to call a PGSQL(dbh) *)
           
  module Access_rights : Hitscore_access_rights.ACCESS_RIGHTS
     with module Configuration = Configuration
    with module Result_IO = Result_IO
    with module Layout = Layout

  val check_hiseq_raw_availability :
    dbh:Layout.db_handle ->
    hiseq_raw:Layout.Record_hiseq_raw.pointer ->
    (Layout.Record_inaccessible_hiseq_raw.pointer *
       Layout.Record_hiseq_raw.pointer Hitscore_std.List.t,
     [> `hiseq_dir_deleted
     | `layout_inconsistency of
         [> `record_inaccessible_hiseq_raw ] *
           [> `insert_did_not_return_one_id of string * int32 list
           | `no_last_modified_timestamp of
               Layout.Record_inaccessible_hiseq_raw.pointer
           | `select_did_not_return_one_tuple of string * int ]
     | `pg_exn of exn ])
      Result_IO.monad
end


  
module Make
  (Configuration : Hitscore_interfaces.CONFIGURATION)
  (Result_IO : Hitscore_interfaces.RESULT_IO) 
  (Layout: Hitscore_layout_interface.LAYOUT
     with module Result_IO = Result_IO
     with type 'a PGOCaml.monad = 'a Result_IO.IO.t)
  (Access_rights : Hitscore_access_rights.ACCESS_RIGHTS 
     with module Result_IO = Result_IO
     with module Configuration = Configuration
     with module Layout = Layout):
  COMMON
    with module Configuration = Configuration
    with module Result_IO = Result_IO
    with module Layout = Layout
    with module Access_rights = Access_rights
= struct
  module Configuration = Configuration
  module Result_IO = Result_IO
  module Access_rights = Access_rights
  module Layout = Layout

  open Hitscore_std
  open Result_IO

  let check_hiseq_raw_availability ~dbh ~hiseq_raw =
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
        return (last_avail, all_deleted))

end
