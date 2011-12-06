

module Make (Config : module type of Hitscore_config) = struct

  module Layout = Hitscore_db_access.Make(Config)

end

