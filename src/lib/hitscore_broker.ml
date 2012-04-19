

module type BROKER = sig


  (**/**)
  module Common : Hitscore_common.COMMON
  open Common
(**/**)

  type 'a t

  val create:
    ?mutex:((unit -> (unit, 'a) Common.Flow.monad) * (unit -> unit)) ->
    dbh:Common.Layout.db_handle ->
    configuration:Configuration.local_configuration ->
    unit ->
    ('a t,
     [> `layout_inconsistency of
         [> `File_system | `Function of string | `Record of string ] *
           [> `select_did_not_return_one_tuple of string * int ]
     | `pg_exn of exn ])
      Common.Flow.monad

  val reload :
    ([> `layout_inconsistency of
        [> `File_system | `Function of string | `Record of string ] *
          [> `select_did_not_return_one_tuple of string * int ]
     | `pg_exn of exn ]
        as 'a)
           t ->
    dbh:Common.Layout.db_handle ->
    configuration:Configuration.local_configuration ->
    (unit, 'a) Common.Flow.monad

  val find_person :
    'a t ->
    identifier:string ->
    (Common.Layout.Record_person.t option,
     [> `person_not_unique of string ])
      Common.Flow.monad

  val modify_person :
    ([> `layout_inconsistency of
        [> `Record of string ] *
          [> `insert_cache_did_not_return_one_id of
              string * int32 list
          | `insert_did_not_return_one_id of string * int32 list ]
     | `pg_exn of exn ]
        as 'a)
           t ->
    dbh:Common.Layout.db_handle ->
    person:Common.Layout.Record_person.t ->
    (unit, 'a) Common.Flow.monad
 
    type lane = {
      lane_t: Layout.Record_lane.t;
      lane_index: int; (* Lane index form 1 to 8 *)
      lane_libraries: (int32 * string option * string) list;
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

    type submission_info = {
      si_input: Layout.Record_input_library.t;
      si_flowcell: Layout.Record_flowcell.t;
      si_lane: Layout.Record_lane.t * int;
    }
    type library_info = {
      li_stock: Layout.Record_stock_library.t;
      li_submissions: submission_info list;
    }
    type library_input_spec =
    [ `id of int32 | `qualified_name of string option * string ]


  val person_affairs: 'a t -> person:Layout.Record_person.t -> person_affairs

  val library_info: 'a t -> library_input_spec -> library_info option

end

module Make
  (Common: Hitscore_common.COMMON):
  BROKER with module Common = Common = struct

    module Common = Common
    open Common

    open Hitscore_std
    open Flow

    module Cache = struct
      type ('input, 'output) cache_item = {
        key: int;
        input: 'input;
        output: 'output;
        hash: string;
      } 
      let item ~key ~input ~output ~hash =
        {key; input; output; hash;}
      type ('input, 'output) t = {
        make_key: 'input -> int;
        compute: 'input -> 'output;
        hash_dependencies: 'input -> 'output -> string;
        mutable content: ('input, 'output) cache_item list;
      }
      let empty ~make_key ~compute ~hash_dependencies =
        {make_key; compute; hash_dependencies; content = []}
     
      let get t metakey =
        match List.find t.content (fun i -> i.key = t.make_key metakey) with
        | None ->
          let output = t.compute metakey in
          let hash = t.hash_dependencies metakey output in
          let key = t.make_key metakey in
          t.content <- (item ~key ~input:metakey ~output ~hash) :: t.content;
          eprintf "cache: new content for %d (hash: %s)\n%!" key hash;
          output
        | Some i when i.hash = t.hash_dependencies metakey i.output ->
          eprintf "cache: from cache for %d (hash: %s)\n%!" i.key i.hash;
          i.output
        | Some i ->
          let output = t.compute metakey in
          let hash = t.hash_dependencies metakey output in
          let key = t.make_key metakey in
          eprintf "cache: recompute cache for %d (hash: %s)\n%!" i.key i.hash;
          t.content <- item ~key ~input:metakey ~output ~hash
          :: List.filter t.content ~f:(fun i -> i.key <> t.make_key metakey);
          output
          
    end

    
    type lane = {
      lane_t: Layout.Record_lane.t;
      lane_index: int; (* Lane index form 1 to 8 *)
      lane_libraries: (int32 * string option * string) list;
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

    type submission_info = {
      si_input: Layout.Record_input_library.t;
      si_flowcell: Layout.Record_flowcell.t;
      si_lane: Layout.Record_lane.t * int;
    }
    type library_info = {
      li_stock: Layout.Record_stock_library.t;
      li_submissions: submission_info list;
    }
    type library_input_spec =
    [ `id of int32 | `qualified_name of string option * string ]

    module Person = Layout.Record_person
    type 'a t = {
      layout: Hitscoregen_layout_dsl.dsl_runtime_description;
      mutable current_dump: Layout.dump;
      mutable last_reload: Time.t;
      mutable person_affairs_cache: (Person.t, person_affairs) Cache.t option;
      mutable library_info_cache:
        (library_input_spec, library_info option) Cache.t option;
      mutex: ((unit -> (unit, 'a) monad) * (unit -> unit)) option;
    }

    let lock_mutex t =
      match t.mutex with
      | Some (l, _) -> l ()
      | None -> return ()
    let unlock_mutex t =
      match t.mutex with
      | Some (_, u) -> u ()
      | None -> ()
        
    module SL = Layout.Record_stock_library
    module IL = Layout.Record_input_library
    module L  = Layout.Record_lane
    module FC = Layout.Record_flowcell
    module P = Layout.Record_person
    module HSRun = Layout.Record_hiseq_run

    let compute_person_affairs t person =
      let flowcells =
        List.filter_map t.current_dump.Layout.record_flowcell
          (fun fc ->
            let {FC. g_id; serial_name; lanes} = fc in
            let p_id = P.unsafe_cast person.P.g_id in
            let lanes_of_flowcell =
              List.filter_map t.current_dump.Layout.record_lane
                (fun lane ->
                  let t_pointer = L.unsafe_cast lane.L.g_id in
                  match Array.findi lanes ~f:((=) t_pointer) with
                  | Some array_index ->
                    if Array.exists lane.L.contacts ~f:((=) p_id)
                    then (
                      let lane_libraries =
                        List.filter_map (Array.to_list lane.L.libraries) (fun ilp ->
                          List.find_map t.current_dump.Layout.record_input_library
                            (fun il ->
                              if il.IL.g_id = ilp.IL.id
                              then (List.find_map
                                      t.current_dump.Layout.record_stock_library
                                      (fun sl ->
                                        if sl.SL.g_id = il.IL.library.SL.id
                                        then Some (sl.SL.g_id,
                                                   sl.SL.project,
                                                   sl.SL.name)
                                        else None))
                              else None)) in
                      Some { lane_t = lane;
                             lane_index = array_index + 1; lane_libraries}
                    ) else None
                  | None -> None)
            in
            match lanes_of_flowcell with
            | [] -> None
            | l ->
              let ff_runs =
                let open Layout.Record_hiseq_run in
                let flowcell_pointer = FC.unsafe_cast g_id in
                List.filter t.current_dump.Layout.record_hiseq_run ~f:(fun hs ->
                  hs.flowcell_a = Some flowcell_pointer
                   || hs.flowcell_b = Some flowcell_pointer)
              in
              Some  {ff_t = fc; ff_id = serial_name; ff_lanes = l; ff_runs})
      in
      let cmp fca fcb =
        let latest_date fc =
          List.fold_left fc.ff_runs ~init:0.
            ~f:(fun c {HSRun.date; _} -> max c (Time.to_float date)) in
        compare (latest_date fcb) (latest_date fca)  in
      {pa_flowcells = List.sort ~cmp flowcells}
        
    let person_affairs_hash_depencencies t person affairs =
      Digest.string (Marshal.to_string
                        (person,
                         t.current_dump.Layout.record_input_library,
                         t.current_dump.Layout.record_stock_library,
                         t.current_dump.Layout.record_flowcell,
                         t.current_dump.Layout.record_lane,
                         t.current_dump.Layout.record_hiseq_run) [])

    let person_affairs t ~person =
      Cache.get (Option.value_exn t.person_affairs_cache) person

        
    let reload t ~dbh ~configuration =
      lock_mutex t >>= fun () ->
      Layout.get_dump dbh >>= fun current_dump ->
      t.current_dump <- current_dump;
      t.last_reload <- Time.now ();
      unlock_mutex t;
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
      lock_mutex t >>= fun () ->
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
      unlock_mutex t;
      return ()
        

    let find_stock_library t lib =
      let open Layout.Record_stock_library in
      match lib with
      | `id x ->
        List.find t.current_dump.Layout.record_stock_library
          (fun l -> l.g_id = x)
      | `qualified_name (p, n) ->
        List.find t.current_dump.Layout.record_stock_library
          (fun l -> l.project = p && l.name = n)
      
    let hash_deps_of_library_info t spec info =
      Digest.string (Marshal.to_string
                        (t.current_dump.Layout.record_flowcell,
                         t.current_dump.Layout.record_lane,
                         t.current_dump.Layout.record_input_library,
                         t.current_dump.Layout.record_stock_library) [])
      
    let compute_library_info t library =
      Option.map (find_stock_library t library) (fun lib ->
        let lib_pointer = SL.unsafe_cast lib.SL.g_id in
        let li_submissions =
          List.filter_map t.current_dump.Layout.record_input_library
            (fun il ->
              if il.IL.library = lib_pointer
              then (
                let open Option in
                List.find t.current_dump.Layout.record_lane (fun lan ->
                  Array.exists lan.L.libraries
                    (fun l -> l.IL.id = il.IL.g_id))
                >>= fun lane_t ->
                List.find_map t.current_dump.Layout.record_flowcell (fun fc ->
                  Array.findi fc.FC.lanes ~f:(fun l -> l.L.id = lane_t.L.g_id)
                  >>= fun index ->
                  return (fc, index))
                >>= fun (si_flowcell, array_index) ->
                return (lane_t, array_index + 1)
                >>= fun si_lane ->
                return { si_lane; si_flowcell; si_input = il } 
              ) else None)
        in
        { li_stock = lib; li_submissions })
        
    let library_info t library =
      Cache.get (Option.value_exn t.library_info_cache) library
      
    let create ?mutex ~dbh ~configuration () =
      Layout.get_dump dbh >>= fun current_dump ->
      let layout = Layout.Meta.layout () in
      let t_non_init =
        { last_reload = Time.now (); layout;
          current_dump;
          person_affairs_cache = None;
          library_info_cache = None;
          mutex;
        } in
      t_non_init.person_affairs_cache <-
        Some (Cache.empty
                ~make_key:(fun p -> Hashtbl.hash p.Person.g_id)
                ~compute:(compute_person_affairs t_non_init)
                ~hash_dependencies:(person_affairs_hash_depencencies t_non_init));
      t_non_init.library_info_cache <-
        Some (Cache.empty
                ~make_key:(fun s -> Hashtbl.hash s)
                ~compute:(compute_library_info t_non_init)
                ~hash_dependencies:(person_affairs_hash_depencencies t_non_init));
      return t_non_init

  end
