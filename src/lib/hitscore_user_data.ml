open Hitscore_std
  
type t = {
  mutable uploads: String.Set.t;
} with sexp

let to_string t = Sexp.to_string_hum (sexp_of_t t)

let of_string s =
  try Ok (t_of_sexp (Sexp.of_string s))
  with e -> Error (`sexp_parsing_error e)

    
  
 
