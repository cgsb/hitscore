

module Make
  (Common: Hitscore_common.COMMON):
  Hitscore_function_interfaces.FASTX_QUALITY_STATS
  with module Common = Common

  = struct

    module Common = Common
    open Common

    open Hitscore_std
    open Result_IO

    let start ~dbh ~configuration
        ?(option_Q=33)
        ?filter_names
        ?(user="sm4431")
        ?(wall_hours=12) ?(nodes=1) ?(ppn=8)
        ?(queue="cgsb-s")
        ?(hitscore_command="echo hitscore should: ")
        ?(make_command="make -j8")
        ~(input_dir:Layout.Record_generic_fastqs.pointer)
         =
      error (`pg_exn (Failure "NOT IMPLEMENTED"))

  end
