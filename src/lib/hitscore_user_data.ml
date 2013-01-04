open Hitscore_std
module Layout = Hitscore_layout
  
type t = {
  mutable uploads: String.Set.t;
} with sexp

let to_string t = Sexp.to_string_hum (sexp_of_t t)

let of_string s =
  try Ok (t_of_sexp (Sexp.of_string s))
  with e -> Error (`sexp_parsing_error e)

let on_user_data ~dbh ~person_id ~function_name f =
  let layout = Layout.Classy.make dbh in
  begin
    layout#person#get_unsafe person_id >>= fun person ->
    begin match person#user_data with
    | Some ud ->
      of_result (of_string ud)
    | None -> return { uploads = String.Set.empty }
    end
    >>= fun user_data ->
  (* Adding a duplicate is considered OK, we could use String.Set.mem. *)
    begin match f user_data with
    | `save o ->
      person#set_user_data (Some (to_string user_data))
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
  on_user_data ~dbh ~person_id ~function_name:"add_upload"
    begin fun user_data ->
      user_data.uploads <- String.Set.add user_data.uploads filename;
      `save ()
    end

let remove_upload ~dbh ~person_id ~filename =
  on_user_data ~dbh ~person_id ~function_name:"remove_upload"
    begin fun user_data ->
      user_data.uploads <- String.Set.remove user_data.uploads filename;
      `save ()
    end
 
let find_upload  ~dbh ~person_id ~filename =
  on_user_data ~dbh ~person_id ~function_name:"find_upload"
    begin fun user_data ->
      `ok (String.Set.mem user_data.uploads filename)
    end
