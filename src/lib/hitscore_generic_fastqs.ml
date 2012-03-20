


module Make_unaligned_coercion
  (Common: Hitscore_common.COMMON):
  Hitscore_function_interfaces.COERCE_B2F_UNALIGNED
  with module Common = Common

  = struct

  module Common = Common
  open Common

  open Hitscore_std
  open Result_IO

  let run ~dbh ~configuration ~input =
    error (`pg_exn (Not_found))

                        
  end
