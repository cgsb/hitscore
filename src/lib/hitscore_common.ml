
module type COMMON = sig

  (** Local definition of the configuration. *)
  module Configuration : Hitscore_interfaces.CONFIGURATION

  (** Local definition of Result_IO.  *)
  module Result_IO : Hitscore_interfaces.RESULT_IO

  (** Local definition of Layout *)
  module Layout : Hitscore_layout_interface.LAYOUT
    with module Result_IO = Result_IO
    with type 'a PGOCaml.monad = 'a Result_IO.IO.t
    (* This last one is used by Assemble_Sample_Sheet to call a PGSQL(dbh) *)
           
  module Access_rights : Hitscore_access_rights.ACCESS_RIGHTS
     with module Configuration = Configuration
    with module Result_IO = Result_IO
    with module Layout = Layout

end


  
module Make
  (Configuration : Hitscore_interfaces.CONFIGURATION)
  (Result_IO : Hitscore_interfaces.RESULT_IO) 
  (Layout: Hitscore_layout_interface.LAYOUT
     with module Result_IO = Result_IO
     with type 'a PGOCaml.monad = 'a Result_IO.IO.t)
  (Access_rights : Hitscore_access_rights.ACCESS_RIGHTS 
     with module Result_IO = Result_IO
     with module Configuration = Configuration
     with module Layout = Layout):
  COMMON
    with module Configuration = Configuration
    with module Result_IO = Result_IO
    with module Layout = Layout
    with module Access_rights = Access_rights
     = struct
    module Configuration = Configuration
    module Result_IO = Result_IO
    module Access_rights = Access_rights
    module Layout = Layout

    open Hitscore_std
    open Result_IO



    end
