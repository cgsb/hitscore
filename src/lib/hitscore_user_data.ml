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

module V1 = struct
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

module Current = struct
  type upload = {
    upload_filename: string;
    upload_original_name: string;
    upload_date: Time.t;
  }
  with sexp
  let upload ?time  ~original filename =
    let upload_date =
      match time with None -> Time.now () | Some t -> t in
    {upload_filename = filename; upload_original_name = original; upload_date}

  type t = {
    mutable uploads: upload list;
  } with sexp
    
  let to_string t = Sexp.to_string_hum (sexp_of_t t)
    
  let of_string s =
    try Ok (t_of_sexp (Sexp.of_string s))
    with e -> Error (`sexp_parsing_error e)

  let create () = {uploads = []}

  let of_v1 v1 =
    let upload_date = Time.of_float 0. in
    let uploads =
      List.map (Set.to_list v1.V1.uploads) (fun s ->
        {upload_filename = s; upload_original_name = s; upload_date}) in
    {uploads}
    
end

open Current
  
type versioned =
| V0 of string
| V1 of string
| V2 of string
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
    return (Current.of_v1 (V1.of_v0 v0))
  | V1 s -> V1.of_string s >>= fun v1 -> return (Current.of_v1 v1)
  | V2 s -> Current.of_string s
  end
  
let create () = Current.create ()

let serialize c = to_string (V2 (Current.to_string c))
  
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

    
type upload_info = (string * string * Time.t) with sexp

let add_upload ~dbh ~person_id ~filename ~original =
  let upload = upload filename ~original in
  protect_edition begin fun () ->
    on_user_data ~dbh ~person_id ~function_name:"add_upload"
      begin fun user_data ->
        user_data.uploads <- upload :: user_data.uploads;
        `save ()
      end
  end

let remove_upload ~dbh ~person_id ~filename =
  protect_edition begin fun () ->
    on_user_data ~dbh ~person_id ~function_name:"remove_upload"
      begin fun user_data ->
        user_data.uploads <- List.filter user_data.uploads (fun u ->
          u.upload_filename <> filename);
        `save ()
      end
  end
 
let find_upload  ~dbh ~person_id ~filename =
  on_user_data ~dbh ~person_id ~function_name:"find_upload"
    begin fun user_data ->
      let open Option in
      `ok (List.find user_data.uploads (fun {upload_filename} -> upload_filename = filename)
           >>= fun up ->
           return (up.upload_filename, up.upload_original_name, up.upload_date))
    end

let all_uploads ~dbh ~person_id =
  on_user_data ~dbh ~person_id ~function_name:"all_uploads"
    begin fun user_data ->
      `ok (user_data.uploads |! List.map ~f:(fun up ->
        (up.upload_filename, up.upload_original_name, up.upload_date)))
    end
