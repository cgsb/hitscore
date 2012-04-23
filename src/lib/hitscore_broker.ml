

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
 
    type delivered_demultiplexing = {
      ddmux_b2f: [`can_nothing] Layout.Function_bcl_to_fastq.t;
      ddmux_b2fu: Layout.Record_bcl_to_fastq_unaligned.t;
      ddmux_b2fu_vol: Layout.File_system.volume; 
      ddmux_b2fu_path: string;
      ddmux_deliveries:
        ([`can_nothing] Layout.Function_prepare_unaligned_delivery.t
         * Layout.Record_client_fastqs_dir.t) list;
    }
    type library_in_lane = {
      lil_stock: Layout.Record_stock_library.t;
      lil_input: Layout.Record_input_library.t;
    }
    type lane = {
      lane_t: Layout.Record_lane.t;
      lane_index: int; (* Lane index form 1 to 8 *)
      lane_libraries: library_in_lane list;
      lane_invoices: Layout.Record_invoicing.t list;
    }
    type hiseq_run = {
      hr_t: Layout.Record_hiseq_run.t;
      hr_raws: Layout.Record_hiseq_raw.t list;
    }
    type filtered_flowcell = {
      ff_t: Layout.Record_flowcell.t;
      ff_id: string;
      ff_lanes: lane list;
      ff_runs: hiseq_run list;
    }
    type person_affairs = {
      pa_person_t : Common.Layout.Record_person.t;
      pa_flowcells : filtered_flowcell list;
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


  val person_affairs: 'a t -> person:int32 ->
    (person_affairs, [> `person_not_found of int32 ]) Common.Flow.monad

  val library_info: 'a t -> library_input_spec -> 
    (library_info, [> `library_not_found of library_input_spec ]) Common.Flow.monad

  val delivered_demultiplexings: 'a t -> hiseq_run ->
    (delivered_demultiplexing list, 'b) Common.Flow.monad
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
        mutable key: int;
        mutable input: 'input;
        mutable output: 'output;
        mutable hash: string;
        mutable last_mutation: Time.t;
      } 
      let item ~key ~input ~output ~hash =
        {key; input; output; hash; last_mutation = Time.now ()}
      let mutate_item ci ~output ~hash =
        ci.output <- output;
        ci.hash <- hash;
        ci.last_mutation <- Time.now ()

      type ('input, 'output) t = {
        make_key: 'input -> int;
        compute: 'input -> 'output;
        hash_dependencies: 'input -> 'output -> string;
        mutable content: ('input, 'output) cache_item list;
      }
      let empty ~make_key ~compute ~hash_dependencies =
        {make_key; compute; hash_dependencies; content = []}
     
      let get t ~last_change metakey =
        match List.find t.content (fun i -> i.key = t.make_key metakey) with
        | None ->
          let output = t.compute metakey in
          let hash = t.hash_dependencies metakey output in
          let key = t.make_key metakey in
          t.content <- (item ~key ~input:metakey ~output ~hash) :: t.content;
          eprintf "cache: new content for %d (hash: %s)\n%!" key hash;
          output
        | Some i when i.last_mutation > last_change ->
          eprintf "cache: from cache for %d (%.3f > %.3f)\n%!" i.key
            (Time.to_float i.last_mutation) (Time.to_float last_change);
          i.output
        | Some i when i.hash = t.hash_dependencies metakey i.output ->
          eprintf "cache: from cache for %d (hash: %s)\n%!" i.key i.hash;
          i.output
        | Some i ->
          let output = t.compute metakey in
          let hash = t.hash_dependencies metakey output in
          mutate_item i ~output ~hash;
          eprintf "cache: recompute cache for %d (hash: %s)\n%!" i.key i.hash;
          output
          
    end

    type delivered_demultiplexing = {
      ddmux_b2f: [`can_nothing] Layout.Function_bcl_to_fastq.t;
      ddmux_b2fu: Layout.Record_bcl_to_fastq_unaligned.t;
      ddmux_b2fu_vol: Layout.File_system.volume; 
      ddmux_b2fu_path: string;
      ddmux_deliveries:
        ([`can_nothing] Layout.Function_prepare_unaligned_delivery.t
         * Layout.Record_client_fastqs_dir.t) list;
    }
    type library_in_lane = {
      lil_stock: Layout.Record_stock_library.t;
      lil_input: Layout.Record_input_library.t;
    }
    type lane = {
      lane_t: Layout.Record_lane.t;
      lane_index: int; (* Lane index form 1 to 8 *)
      lane_libraries: library_in_lane list;
      lane_invoices: Layout.Record_invoicing.t list;
    }
    type hiseq_run = {
      hr_t: Layout.Record_hiseq_run.t;
      hr_raws: Layout.Record_hiseq_raw.t list;
    }
    type filtered_flowcell = {
      ff_t: Layout.Record_flowcell.t;
      ff_id: string;
      ff_lanes: lane list;
      ff_runs: hiseq_run list;
    }
    type person_affairs = {
      pa_person_t: Layout.Record_person.t;
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
      configuration: Configuration.local_configuration;
      mutable current_dump: Layout.dump;
      mutable last_change: Time.t;
      mutable person_affairs_cache: (int32, person_affairs option) Cache.t option;
      mutable library_info_cache:
        (library_input_spec, library_info option) Cache.t option;
      mutable delivered_demultiplexings_cache:
        (hiseq_run, delivered_demultiplexing list) Cache.t option;
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
    module HSRaw = Layout.Record_hiseq_raw
    module B2F = Layout.Function_bcl_to_fastq
    module PUD = Layout.Function_prepare_unaligned_delivery
    module B2FU = Layout.Record_bcl_to_fastq_unaligned
    module CFD = Layout.Record_client_fastqs_dir
    module FS = Layout.File_system

      
    let delivered_demultiplexings_hash_depencencies t hiseq_runs dds =
      Digest.string (Marshal.to_string
                        (t.current_dump.Layout.record_hiseq_run,
                         t.current_dump.Layout.record_hiseq_raw,
                         t.current_dump.Layout.record_inaccessible_hiseq_raw,
                         t.current_dump.Layout.function_bcl_to_fastq,
                         t.current_dump.Layout.record_bcl_to_fastq_unaligned,
                         t.current_dump.Layout.function_prepare_unaligned_delivery,
                         t.current_dump.Layout.record_client_fastqs_dir,
                         t.current_dump.Layout.file_system) [])
    let compute_delivered_demultiplexings t hiseq_runs =
      let open Option in
      List.map hiseq_runs.hr_raws (fun hrw ->
        List.filter_map t.current_dump.Layout.function_bcl_to_fastq
          ~f:(fun b2f ->
            if b2f.B2F.g_status = `Succeeded
            && b2f.B2F.tiles = None
            && b2f.B2F.raw_data.HSRaw.id = hrw.HSRaw.g_id
            then (
              List.find t.current_dump.Layout.record_bcl_to_fastq_unaligned
                (fun b2fu -> Some B2FU.(unsafe_cast b2fu.g_id) = b2f.B2F.g_result)
              >>= fun b2fu ->
              List.find t.current_dump.Layout.file_system
                (fun vol -> vol.FS.volume_pointer = b2fu.B2FU.directory)
              >>= fun ddmux_b2fu_vol ->
              Configuration.path_of_volume_fun t.configuration
              >>= fun path_fun ->
              let { FS.volume_pointer = { FS.id }; volume_kind; volume_content } =
                ddmux_b2fu_vol in
              begin match volume_content with
              | FS.Tree (hr_tag, trees) ->
                let vol = volume_unix_directory ~id ~kind:volume_kind ?hr_tag in 
                return (path_fun vol)
              | _ -> None
              end
              >>= fun ddmux_b2fu_path ->
              return {
                ddmux_b2f = b2f;
                ddmux_b2fu = b2fu;
                ddmux_b2fu_vol;
                ddmux_b2fu_path;
                ddmux_deliveries =
                  List.filter_map
                    t.current_dump.Layout.function_prepare_unaligned_delivery
                    (fun fpud ->
                      if fpud.PUD.g_status = `Succeeded
                      && fpud.PUD.unaligned.B2FU.id = b2fu.B2FU.g_id
                      then (
                        List.find t.current_dump.Layout.record_client_fastqs_dir
                          (fun c ->
                            Some CFD.(unsafe_cast c.g_id) = fpud.PUD.g_result)
                        >>= fun cfd ->
                        return (fpud, cfd))
                      else None)})
            else None))
        |! List.flatten

    let delivered_demultiplexings t hs_run =
      Cache.get ~last_change:t.last_change
        (Option.value_exn t.delivered_demultiplexings_cache) hs_run
      |! return

    let library_in_lane t ~il_pointer ~flowcell =
      List.find_map t.current_dump.Layout.record_input_library
        (fun lil_input ->
          if lil_input.IL.g_id = il_pointer.IL.id
          then (List.find_map
                  t.current_dump.Layout.record_stock_library
                  (fun lil_stock ->
                    if lil_stock.SL.g_id = lil_input.IL.library.SL.id
                    then Some {lil_input; lil_stock}
                    else None))
          else None)
      
    let inaccessible_hiseq_raws t =
      let cur = ref (Some 0., [| |]) in
      List.iter t.current_dump.Layout.record_inaccessible_hiseq_raw
        ~f:(fun {Layout.Record_inaccessible_hiseq_raw. g_last_modified; deleted} ->
          let fl = Option.map g_last_modified Time.to_float in
          if (fst !cur) < fl then cur := (fl, deleted));
      Array.to_list (snd !cur)
        
    let compute_person_affairs t person_id =
      let flowcells () =
        List.filter_map t.current_dump.Layout.record_flowcell
          (fun fc ->
            let {FC. g_id; serial_name; lanes} = fc in
            let person_pointer = P.unsafe_cast person_id in
            let lanes_of_flowcell =
              List.filter_map t.current_dump.Layout.record_lane
                (fun lane ->
                  let t_pointer = L.unsafe_cast lane.L.g_id in
                  match Array.findi lanes ~f:((=) t_pointer) with
                  | Some array_index ->
                    if Array.exists lane.L.contacts ~f:((=) person_pointer)
                    then (
                      let lane_libraries =
                        List.filter_map (Array.to_list lane.L.libraries)
                          (fun il_pointer ->
                            library_in_lane t ~il_pointer ~flowcell:fc) in
                      let lane_invoices =
                        List.filter t.current_dump.Layout.record_invoicing
                          ~f:(fun i ->
                            Array.exists i.Layout.Record_invoicing.lanes
                              ~f:(fun l -> l.L.id = lane.L.g_id)) in
                      Some { lane_t = lane; lane_invoices;
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
                List.filter_map t.current_dump.Layout.record_hiseq_run
                  ~f:(fun hs ->
                    if hs.flowcell_a = Some flowcell_pointer
                    || hs.flowcell_b = Some flowcell_pointer
                    then (
                      let hr_raws =
                        List.filter t.current_dump.Layout.record_hiseq_raw
                          ~f:(fun hsrw ->
                            hsrw.HSRaw.flowcell_name = serial_name
                            && List.for_all (inaccessible_hiseq_raws t)
                              ~f:(fun p -> p.HSRaw.id <> g_id))
                      in
                      Some {hr_t = hs; hr_raws})
                    else None)
              in
              Some  {ff_t = fc; ff_id = serial_name; ff_lanes = l; ff_runs})
      in
      let person_t_opt =
        List.find t.current_dump.Layout.record_person
          (fun p -> p.P.g_id = person_id) in
      Option.map person_t_opt (fun pa_person_t ->
        let cmp fca fcb =
          let latest_date fc =
            List.fold_left fc.ff_runs ~init:0.
            ~f:(fun c hsr -> max c (Time.to_float hsr.hr_t.HSRun.date)) in
          compare (latest_date fcb) (latest_date fca)  in
        {pa_person_t; pa_flowcells = List.sort ~cmp (flowcells ())})
        
    let person_affairs_hash_depencencies t person affairs =
      Digest.string (Marshal.to_string
                        (t.current_dump.Layout.record_person,
                         t.current_dump.Layout.record_input_library,
                         t.current_dump.Layout.record_stock_library,
                         t.current_dump.Layout.record_flowcell,
                         t.current_dump.Layout.record_lane,
                         t.current_dump.Layout.record_hiseq_run,
                         t.current_dump.Layout.record_hiseq_raw,
                         t.current_dump.Layout.record_inaccessible_hiseq_raw) [])

    let person_affairs t ~person =
      Cache.get ~last_change:t.last_change
        (Option.value_exn t.person_affairs_cache) person
      |! (function
        | None -> error (`person_not_found person)
        | Some s -> return s)
        
    let reload t ~dbh ~configuration =
      lock_mutex t >>= fun () ->
      Layout.get_dump dbh >>= fun current_dump ->
      t.current_dump <- current_dump;
      t.last_change <- Time.now ();
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
      t.last_change <- Time.now ();
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
      Cache.get ~last_change:t.last_change
        (Option.value_exn t.library_info_cache) library
      |! (function
        | None -> error (`library_not_found library)
        | Some s -> return s)

    let create ?mutex ~dbh ~configuration () =
      Layout.get_dump dbh >>= fun current_dump ->
      let layout = Layout.Meta.layout () in
      let t_non_init =
        { last_change = Time.now (); layout;
          current_dump;
          configuration;
          person_affairs_cache = None;
          library_info_cache = None;
          delivered_demultiplexings_cache = None;
          mutex;
        } in
      t_non_init.person_affairs_cache <-
        Some (Cache.empty
                ~make_key:(fun p -> Hashtbl.hash p)
                ~compute:(compute_person_affairs t_non_init)
                ~hash_dependencies:(person_affairs_hash_depencencies t_non_init));
      t_non_init.library_info_cache <-
        Some (Cache.empty
                ~make_key:(fun s -> Hashtbl.hash s)
                ~compute:(compute_library_info t_non_init)
                ~hash_dependencies:(person_affairs_hash_depencencies t_non_init));
      t_non_init.delivered_demultiplexings_cache <-
        Some (Cache.empty
                ~make_key:(fun hs -> Hashtbl.hash hs)
                ~compute:(compute_delivered_demultiplexings t_non_init)
                ~hash_dependencies:
                (delivered_demultiplexings_hash_depencencies t_non_init));
      return t_non_init

  end
