

module type BROKER = sig


  (**/**)
  module Common : Hitscore_common.COMMON
  open Common
(**/**)

  type t

  val create:
    dbh:Common.Layout.db_handle ->
    configuration:Configuration.local_configuration ->
    unit ->
    (t,
     [> `layout_inconsistency of
         [> `File_system | `Function of string | `Record of string ] *
           [> `select_did_not_return_one_tuple of string * int ]
     | `pg_exn of exn ])
      Common.Flow.monad

  val reload :
    t ->
    dbh:Common.Layout.db_handle ->
    configuration:Configuration.local_configuration ->
    (unit,
     [> `layout_inconsistency of
         [> `File_system | `Function of string | `Record of string ] *
           [> `select_did_not_return_one_tuple of string * int ]
     | `pg_exn of exn ])
      Common.Flow.monad

  val find_person :
    t ->
    identifier:string ->
    (Common.Layout.Record_person.t option,
     [> `person_not_unique of string ])
      Common.Flow.monad

  val modify_person :
    t ->
    dbh:Common.Layout.db_handle ->
    person:Common.Layout.Record_person.t ->
    (unit,
     [> `layout_inconsistency of
         [> `Record of string ] *
           [> `insert_cache_did_not_return_one_id of
               string * int32 list
           | `insert_did_not_return_one_id of string * int32 list ]
     | `pg_exn of exn ])
      Common.Flow.monad
      
  type lane = {
    lane_t: Layout.Record_lane.t;
    lane_index: int; (* Lane index form 1 to 8 *)
  }
  type filtered_flowcell = {
    ff_t: Layout.Record_flowcell.t;
    ff_id: string;
    ff_lanes: lane list;
    ff_runs: Layout.Record_hiseq_run.t list;
  }
  type person_affairs = {
    pa_flowcells: filtered_flowcell list;
  }

  val person_affairs: t -> person:Layout.Record_person.t -> person_affairs

end

module Make
  (Common: Hitscore_common.COMMON):
  BROKER with module Common = Common = struct

    module Common = Common
    open Common

    open Hitscore_std
    open Flow

    type lane = {
      lane_t: Layout.Record_lane.t;
      lane_index: int; (* Lane index form 1 to 8 *)
    }
    type filtered_flowcell = {
      ff_t: Layout.Record_flowcell.t;
      ff_id: string;
      ff_lanes: lane list;
      ff_runs: Layout.Record_hiseq_run.t list;
    }
    type person_affairs = {
      pa_flowcells: filtered_flowcell list;
    }

    type t = {
      layout: Hitscoregen_layout_dsl.dsl_runtime_description;
      mutable current_dump: Layout.dump;
      mutable last_reload: Time.t;
      mutable person_affairs_cache: (int32 * person_affairs * string) list;
    }

    let create ~dbh ~configuration () =
      Layout.get_dump dbh >>= fun current_dump ->
      let layout = Layout.Meta.layout () in
      return { last_reload = Time.now (); layout;
               current_dump; person_affairs_cache = [] }
        
    let reload t ~dbh ~configuration =
      Layout.get_dump dbh >>= fun current_dump ->
      t.current_dump <- current_dump;
      t.last_reload <- Time.now ();
      return ()

    let find_person t ~identifier =
      let l =
      List.find_all t.current_dump.Layout.record_person
        ~f:(fun {Layout.Record_person.login; email; secondary_emails} ->
          login = Some identifier || email = identifier
          || Array.exists secondary_emails ~f:((=) identifier)) in
      match l with
      | [] -> return None
      | [one] -> return (Some one)
      | more -> error (`person_not_unique identifier)
        
    let modify_person t ~dbh ~person =
      let open Layout.Record_person in
      let pointer = unsafe_cast person.g_id in
      delete_value ~dbh pointer >>= fun () ->
      let new_person = { person with g_last_modified = Some (Time.now ()) } in
      insert_value ~dbh new_person >>= fun new_pointer ->
      ksprintf (Common.add_log ~dbh) "(modification record_person %ld)" person.g_id
      >>= fun () ->
      let record_person =
        List.map t.current_dump.Layout.record_person ~f:(fun p ->
          if p.g_id = person.g_id then new_person else p)
      in
      t.current_dump <- { t.current_dump with Layout.record_person };
      return ()
        

    let compute_person_affairs t ~person =
      let module LP = Layout.Record_person in
      let module LFC = Layout.Record_flowcell in
      let module LL = Layout.Record_lane in
      let flowcells =
        List.filter_map t.current_dump.Layout.record_flowcell
          (fun fc ->
            let {LFC. g_id; serial_name; lanes} = fc in
            let p_id = LP.unsafe_cast person.LP.g_id in
            let lanes_of_flowcell =
              List.filter_map t.current_dump.Layout.record_lane
                (fun t ->
                  let t_pointer = LL.unsafe_cast t.LL.g_id in
                  match Array.findi lanes ~f:((=) t_pointer) with
                  | Some array_index ->
                    if Array.exists t.LL.contacts ~f:((=) p_id)
                    then Some { lane_t = t; lane_index = array_index + 1}
                    else None
                  | None -> None)
            in
            match lanes_of_flowcell with
            | [] -> None
            | l ->
              let ff_runs =
                let open Layout.Record_hiseq_run in
                let flowcell_pointer = LFC.unsafe_cast g_id in
                List.filter t.current_dump.Layout.record_hiseq_run ~f:(fun hs ->
                  hs.flowcell_a = Some flowcell_pointer
                   || hs.flowcell_b = Some flowcell_pointer)
              in
              Some  {ff_t = fc; ff_id = serial_name; ff_lanes = l; ff_runs})
      in
      {pa_flowcells = flowcells}
        
    let person_affairs_hash_depencencies t ~person =
      Digest.string (Marshal.to_string
                        (person,
                         t.current_dump.Layout.record_flowcell,
                         t.current_dump.Layout.record_lane,
                         t.current_dump.Layout.record_hiseq_run) [])

    let person_affairs t ~person =
      let module LRP = Layout.Record_person in
      let is_person = fun (p,_,_) -> p = person.LRP.g_id in
      eprintf "cache: %d items\n" (List.length t.person_affairs_cache);
      begin match List.find t.person_affairs_cache is_person with
      | None ->
        eprintf "cache: first compute new for %ld\n%!" person.LRP.g_id;
        let to_cache = compute_person_affairs t ~person in
        t.person_affairs_cache <-
          (person.LRP.g_id, to_cache, person_affairs_hash_depencencies t ~person)
          :: t.person_affairs_cache;
        to_cache
      | Some (p, from_cache, hash) when
          hash = person_affairs_hash_depencencies t ~person ->
        eprintf "cache: from cache for %ld (hash: %s)\n%!" person.LRP.g_id hash;
        from_cache
      | Some (p, from_cache, hash) ->
        let to_cache = compute_person_affairs t ~person in
        eprintf "cache: compute new for %ld\n%!" person.LRP.g_id;
        t.person_affairs_cache <-
          (person.LRP.g_id, to_cache, person_affairs_hash_depencencies t ~person)
          :: (List.filter t.person_affairs_cache ~f:(fun p -> not (is_person p)));
        to_cache
      end



  end
