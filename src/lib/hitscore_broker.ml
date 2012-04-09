module type BROKER = sig


  (**/**)
  module Common : Hitscore_common.COMMON
  open Common
(**/**)


end

module Make
  (Common: Hitscore_common.COMMON):
  BROKER with module Common = Common = struct

    module Common = Common
    open Common

    open Hitscore_std
    open Flow

  end
