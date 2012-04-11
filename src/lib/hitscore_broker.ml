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
      
end

module Make
  (Common: Hitscore_common.COMMON):
  BROKER with module Common = Common = struct

    module Common = Common
    open Common

    open Hitscore_std
    open Flow

    type t = {
      layout: Hitscoregen_layout_dsl.dsl_runtime_description;
      mutable current_dump: Layout.dump;
      mutable last_reload: Time.t;
    }

    let create ~dbh ~configuration () =
      Layout.get_dump dbh >>= fun current_dump ->
      let layout = Layout.Meta.layout () in
      return { last_reload = Time.now (); layout; current_dump }
        
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
        
  end
