open Hitscore_std
module Layout = Hitscore_layout
  
type t = {
  mutable uploads: String.Set.t;
} with sexp

let to_string t = Sexp.to_string_hum (sexp_of_t t)

let of_string s =
  try Ok (t_of_sexp (Sexp.of_string s))
  with e -> Error (`sexp_parsing_error e)

    
let add_upload ~dbh ~person_id filename =
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
    user_data.uploads <- String.Set.add user_data.uploads filename;
    person#set_user_data (Some (to_string user_data))
  end
  >>< begin function
  | Ok () -> return ()
  | Error e -> error (`user_data ("add_upload", e))
  end
  
 
