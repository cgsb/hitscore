
(** Manage Access-Control-Lists (POSIX).  *)
module Make :
  functor (Configuration : Hitscore_interfaces.CONFIGURATION) ->
  functor (Result_IO : Hitscore_interfaces.RESULT_IO) ->
    Hitscore_interfaces.ACL
    with module Configuration = Configuration
    with module Result_IO = Result_IO 
