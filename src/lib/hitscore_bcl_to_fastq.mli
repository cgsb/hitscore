
(** The module to run CASAVA's demultiplexer.  *)
module Make :
  functor (Configuration : Hitscore_interfaces.CONFIGURATION) ->
  functor (Result_IO : Hitscore_interfaces.RESULT_IO) ->
  functor (Layout: Hitscore_layout_interface.LAYOUT
           with module Result_IO = Result_IO
           with type 'a PGOCaml.monad = 'a Result_IO.IO.t) ->
  functor (ACL : Hitscore_acl.ACL
           with module Result_IO = Result_IO
           with module Configuration = Configuration
           with module Layout = Layout) ->
  Hitscore_function_interfaces.BCL_TO_FASTQ
    with module Configuration = Configuration
    with module Result_IO = Result_IO
    with module ACL = ACL
    with module Layout = Layout

