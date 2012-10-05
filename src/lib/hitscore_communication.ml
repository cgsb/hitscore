
open Hitscore_std
open Hitscore_layout
open Hitscore_access_rights
open Hitscore_db_backend
open Hitscore_common
open Hitscore_configuration

module Protocol = struct

  type up = [
    `log of string
  ]
  with bin_io, sexp

  type down = [
  | `user_message of string
  ]
  with bin_io, sexp
    

end


module Authentication = struct

  let hash_password person_id password =
    let open Cryptokit in
    let to_hash = sprintf "gencore:%d:%s" person_id password in
    transform_string (Hexa.encode ()) (hash_string (Hash.sha256 ()) to_hash)



end
