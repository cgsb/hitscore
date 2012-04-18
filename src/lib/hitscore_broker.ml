

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

  type submission_info = {
    si_input: Layout.Record_input_library.t;
    si_flowcell: Layout.Record_flowcell.t;
    si_lane: lane;
  }
  type library_info = {
    li_stock: Layout.Record_stock_library.t;
    li_submissions: submission_info list;
  }

  val library_info: t ->
    [ `id of int32 | `qualified_name of string option * string] ->
    library_info option

end

module Make
  (Common: Hitscore_common.COMMON):
  BROKER with module Common = Common = struct

    module Common = Common
    open Common

    open Hitscore_std
    open Flow

    module Cache = struct
      type ('input, 'output) t = {
        key: 'input -> int32;
        compute: 'input -> 'output;
        hash_dependencies: 'input -> 'output -> string;
        mutable content: (int32 * 'input * 'output * string) list;
      }
      let empty ~key ~compute ~hash_dependencies =
        {key; compute; hash_dependencies; content = []}
     
      let get t metakey =
        match List.find t.content (fun (k, _, _, _) -> k = t.key metakey) with
        | None ->
          let new_content = t.compute metakey in
          let new_hash = t.hash_dependencies metakey new_content in
          let new_key = t.key metakey in
          t.content <- (new_key, metakey, new_content, new_hash) :: t.content;
          eprintf "cache: new content for %ld (hash: %s)\n%!" new_key new_hash;
          new_content
        | Some (k, i, o, h) when h = t.hash_dependencies i o ->
          eprintf "cache: from cache for %ld (hash: %s)\n%!" k h;
          o
        | Some (k, i, o, h) ->
          let new_content = t.compute metakey in
          let new_hash = t.hash_dependencies metakey new_content in
          let new_key = t.key metakey in
          eprintf "cache: recompute cache for %ld (hash: %s)\n%!" k h;
          t.content <- (new_key, metakey, new_content, new_hash)
          :: List.filter t.content ~f:(fun (k, _, _, _) -> k <> t.key metakey);
          new_content
          
    end

    module Person = Layout.Record_person
    
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
      mutable person_affairs_cache: (Person.t, person_affairs) Cache.t option;
    }

    let compute_person_affairs t person =
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
        
    let person_affairs_hash_depencencies t person affairs =
      Digest.string (Marshal.to_string
                        (person,
                         t.current_dump.Layout.record_flowcell,
                         t.current_dump.Layout.record_lane,
                         t.current_dump.Layout.record_hiseq_run) [])

    let person_affairs t ~person =
      Cache.get (Option.value_exn t.person_affairs_cache) person

    let create ~dbh ~configuration () =
      Layout.get_dump dbh >>= fun current_dump ->
      let layout = Layout.Meta.layout () in
      let t_non_init =
        { last_reload = Time.now (); layout;
          current_dump;
          person_affairs_cache = None } in
      t_non_init.person_affairs_cache <-
        Some (Cache.empty
                ~key:(fun p -> p.Layout.Record_person.g_id)
                ~compute:(compute_person_affairs t_non_init)
                ~hash_dependencies:(person_affairs_hash_depencencies t_non_init));
      return t_non_init
        
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
        

    type submission_info = {
      si_input: Layout.Record_input_library.t;
      si_flowcell: Layout.Record_flowcell.t;
      si_lane: lane;
    }
    type library_info = {
      li_stock: Layout.Record_stock_library.t;
      li_submissions: submission_info list;
    }

    let find_stock_library t lib =
      let open Layout.Record_stock_library in
      match lib with
      | `id x ->
        List.find t.current_dump.Layout.record_stock_library
          (fun l -> l.g_id = x)
      | `qualified_name (p, n) ->
        List.find t.current_dump.Layout.record_stock_library
          (fun l -> l.project = p && l.name = n)
      
    let compute_library_info t library =
      let module SL = Layout.Record_stock_library in
      let module IL = Layout.Record_input_library in
      let module L  = Layout.Record_lane in
      let module FC = Layout.Record_flowcell in
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
                return { lane_t; lane_index = array_index + 1}
                >>= fun si_lane ->
                return { si_lane; si_flowcell; si_input = il } 
              ) else None)
        in
        { li_stock = lib; li_submissions })
        
    let library_info t library = compute_library_info t library
      
  end
