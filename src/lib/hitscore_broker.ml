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
        
        
  end
