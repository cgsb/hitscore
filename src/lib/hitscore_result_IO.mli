open Hitscore_interfaces

module Make (IOC : IO_CONFIGURATION) : RESULT_IO with type 'a IO.t = 'a IOC.t

