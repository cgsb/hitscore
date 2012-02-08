
(** The module to generate sample sheets.  *)
module Make :
  functor (Configuration : Hitscore_interfaces.CONFIGURATION) ->
  functor (Result_IO : Hitscore_interfaces.RESULT_IO) ->
  functor (ACL : Hitscore_interfaces.ACL
           with module Result_IO = Result_IO
           with module Configuration = Configuration) ->
  functor (Layout: Hitscore_layout_interface.LAYOUT
           with module Result_IO = Result_IO
           with type 'a PGOCaml.monad = 'a Result_IO.IO.t) -> 
  Hitscore_function_interfaces.ASSEMBLE_SAMPLE_SHEET
  with module Configuration = Configuration
  with module Result_IO = Result_IO
  with module ACL = ACL
  with module Layout = Layout


