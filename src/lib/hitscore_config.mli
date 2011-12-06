open Hitscore_std


val root : string

include PGOCaml_generic.THREAD

val err: string -> unit t

val map_s : ('a -> 'b t) -> 'a list -> 'b list t



