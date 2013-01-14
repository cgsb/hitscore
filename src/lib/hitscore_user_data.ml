open Hitscore_std
module Layout = Hitscore_layout
  
module V0 = struct

  type t = [`dummy] with sexp

  let to_string t = Sexp.to_string_hum (sexp_of_t t)
    
  let of_string s =
    try Ok (t_of_sexp (Sexp.of_string s))
    with e -> Error (`sexp_parsing_error e)

  let create () = `dummy
end

module Current = struct
  type t = {
    mutable uploads: String.Set.t;
  } with sexp
    
  let to_string t = Sexp.to_string_hum (sexp_of_t t)
    
  let of_string s =
    try Ok (t_of_sexp (Sexp.of_string s))
    with e -> Error (`sexp_parsing_error e)

  let create () = {uploads = String.Set.empty}

  let of_v0 v0 = create ()
    
end

open Current
  
type versioned =
| V0 of string
| V1 of string
with sexp

let to_string t = Sexp.to_string_hum (sexp_of_versioned t)
  
let of_string s =
  try Ok (versioned_of_sexp (Sexp.of_string s))
  with e -> Error (`sexp_parsing_error e)

let latest_of_string s =
  let open Result in
  of_string s
  >>= begin function
  | V0 s ->
    V0.of_string s
    >>= fun v0 ->
    return (Current.of_v0 v0)
  | V1 s -> Current.of_string s
  end
  
let create () = Current.create ()

let serialize c = to_string (V1 (Current.to_string c))
  
let _user_data_mutex = Lwt_mutex.create ()

let protect_edition f =
  Lwt_mutex.with_lock _user_data_mutex f
  
let on_user_data ~dbh ~person_id ~function_name f =
  let layout = Layout.Classy.make dbh in
  begin
    layout#person#get_unsafe person_id >>= fun person ->
    begin match person#user_data with
    | Some ud ->
      of_result (latest_of_string ud)
    | None -> return (create ())
    end
    >>= fun user_data ->
      (* Adding a duplicate is considered OK, we could use String.Set.mem. *)
    begin match f user_data with
    | `save o ->
      person#set_user_data (Some (serialize user_data))
      >>= fun () ->
      return o
    | `ok o -> return o
    | `error e -> error e
    end
  end
  >>< begin function
  | Ok o -> return o
  | Error e -> error (`user_data (function_name, e))
  end

    
let add_upload ~dbh ~person_id ~filename =
  protect_edition begin fun () ->
    on_user_data ~dbh ~person_id ~function_name:"add_upload"
      begin fun user_data ->
        user_data.uploads <- String.Set.add user_data.uploads filename;
        `save ()
      end
  end

let remove_upload ~dbh ~person_id ~filename =
  protect_edition begin fun () ->
    on_user_data ~dbh ~person_id ~function_name:"remove_upload"
      begin fun user_data ->
        user_data.uploads <- String.Set.remove user_data.uploads filename;
        `save ()
      end
  end
 
let find_upload  ~dbh ~person_id ~filename =
  on_user_data ~dbh ~person_id ~function_name:"find_upload"
    begin fun user_data ->
      `ok (String.Set.mem user_data.uploads filename)
    end

let all_uploads ~dbh ~person_id =
  on_user_data ~dbh ~person_id ~function_name:"all_uploads"
    begin fun user_data ->
      `ok (Set.to_list user_data.uploads)
    end
