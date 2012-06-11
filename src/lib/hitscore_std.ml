include Core.Std


include Sequme_flow
include Sequme_flow_list
include Sequme_flow_sys
  

module Lwt_config = struct
  include Lwt
  include Lwt_chan
end
module PG = PGOCaml_generic.Make(Lwt_config)
(* let (|>) x f = f x *)

