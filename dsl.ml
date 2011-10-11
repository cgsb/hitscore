open Printf
type ('a, 'b) result = 
  | Ok of 'a
  | Bad of 'b

module OCamlList = List
module List = struct
  include OCamlList

  let find_opt f l =
    try Some (List.find f l) with _ -> None
end
module HT = struct
  include Hashtbl
  let find ht k = try Some (Hashtbl.find ht k) with _ -> None 
end

module Path = struct
  type t = string list
  let str = String.concat "/"
end


module DSL = struct

  module Definition = struct

    type dsl_type = 
      | T_int
      | T_string
      | T_fastq
      | T_file 



    type fastq = {
      fastq_comment : string;
      fastq_record : string;
      fastq_quality: string;
    }

    type atom =
      | Int of int
      | String of string
      | Fastq of fastq list
      | File of string

    type expression =
      | Constant of atom
      | Variable of string
      | External of Path.t * dsl_type
      | Load_fastq of expression
      | Bowtie of expression * expression (* fastq x some_other -> ??? *)

    type global = string * expression

    type program = global list

  end

  open Definition


  let string_of_type = function
    | T_int -> "int"
    | T_string -> "string"
    | T_fastq -> "fastq"
    | T_file -> "file"

  let string_of_expression e =
    let fastq = fun { fastq_comment; fastq_record; fastq_quality } ->
      sprintf "(%s %s %s)" fastq_comment fastq_record fastq_quality in
    let basic = function
      | Int i -> sprintf "%d" i
      | String s -> sprintf "%S" s
      | Fastq f -> 
        sprintf "(fastq %s)" (String.concat ", " (List.map fastq f))
      | File f -> sprintf "(file %s)" f in
    let rec expr = function
      | Constant c -> sprintf "(cst %s)" (basic c)
      | Variable v -> sprintf "(var %s)" v
      | External (p, t) ->
        sprintf "(ext %s : %s)" (Path.str p) (string_of_type t)
      | Load_fastq e -> sprintf "(load_fastq %s)" (expr e)
      | Bowtie (e1, e2) -> sprintf "(bowtie %s %s)" (expr e1) (expr e2)
    in
    (expr e)

  let print_program ?(indent=0) =
    let strindent = String.make indent ' ' in
    List.iter (fun (g, e) ->
      printf "%s[%S:\n%s  %s\n%s]\n" 
        strindent g strindent (string_of_expression e) strindent)




  module Construct = struct
      
    let int i = Constant (Int i)
    let str s = Constant (String s)
    let var s = Variable s
    let file f = Constant (File f)
    let load_fastq f = Load_fastq f 
    let bowtie e i = Bowtie (e, int i)
    let ext_int p = External (p, T_int)
    let ext_str p = External (p, T_string)
    let ext_fastq p = External (p, T_fastq)
    let ext_file p = External (p, T_file)
  end

  module Verify = struct

    let variable_type env var =
      if String.length var > 2 && String.sub var 0 2 = "__" then
        Bad (sprintf "Variable %S has a wrong name" var)
      else
        begin try
                (snd (List.find (fun (v, t) -> v = var) env))
          with 
            Not_found -> Bad (sprintf "Variable %S not found" var)
        end

    let atom_type = function
      | Int _ -> Ok T_int
      | String _ -> Ok T_string
      | Fastq _ -> Ok T_fastq
      | File _ -> Ok T_file

    let rec type_check_expression 
        ?(check_externals:(Path.t -> (dsl_type, string) result) option) 
        env expr =
      match expr with
      | Constant a -> atom_type a
      | Variable v -> variable_type env v
      | External (p, t) -> 
        begin match check_externals with
        | Some ce ->
          begin match ce p with 
          | Ok et -> 
            if et = t then
              Ok t 
            else
              Bad
                (sprintf "External %s has conflicting types: %s Vs %s"
                   (Path.str p) (string_of_type et) (string_of_type t))
          | Bad b -> Bad b
          end
        | None -> Ok t
        end
      | Load_fastq e -> 
        begin match type_check_expression env e with
        | Ok T_file -> Ok T_fastq
        | Ok other ->
          Bad (sprintf "Load_fastq expects a file, %S has a wrong type: %S"
                 (string_of_expression e) (string_of_type other))
        | Bad s -> Bad s
        end
      | Bowtie (e1, e2) ->
        begin match type_check_expression env e1, 
          type_check_expression env e2 with
          | Ok T_fastq, Ok T_int -> Ok T_fastq
          | _, _ ->
            Bad "Bowtie expects a `fastq and an `int … error messages \
                  will be better in the future"
        end

    let type_check_program ?check_externals p =
      let env = ref [] in
      List.iter (fun (name, expr) ->
        let tc = type_check_expression ?check_externals !env expr in
        env := (name, tc) :: !env
      ) p;
      List.rev !env

  end



  module Runtime = struct

    (* Representation of the server run-time in the meta-language *)
    module Simulation_backend = struct

      type error = [ 
      | `fatal_error of string (* The kind that should not happen *)
      | `wrong_request of string (* The “client” sent an impossible request *)
      | `runtime_error of string (* Error due to external conditions *)
      ]
      let string_of_error = function
        | `fatal_error   s -> sprintf "FATAL ERROR: \"%s\"\n\
                                       ===>  you should complain to the devs" s
        | `wrong_request s -> sprintf "Wrong Request: %s" s 
        | `runtime_error s -> sprintf "Runtime Error: %s" s 
      exception Error of error

      type meta_value = 
        | RT_int of int
        | RT_string of string
        | RT_file of string
        | RT_checked_file of (string * Digest.t)
        | RT_fastq of fastq
        | RT_expression of expression * dsl_type
      type runtime_value = {
        (* kind of path / unique id / Ocsigen's convention *)
        val_id: Path.t;
        (* again a stack, representing history *)
        mutable val_history: meta_value list;
      }

      let rt_value id v = {val_id = id; val_history = [ v ]}
      let update_value v nv =
        v.val_history <- nv :: v.val_history
      let current_value v =
        try List.hd v.val_history
        with e -> raise (Error (`fatal_error "value has no history!"))

      let string_of_runtime_value v =
        let s = 
          match current_value v with
          | RT_int            v    -> sprintf "int %d" v 
          | RT_string         v    -> sprintf "string %s" v
          | RT_file           v    -> sprintf "file %s" v 
          | RT_checked_file (v, _) -> sprintf "checked_file %s" v 
          | RT_fastq          v    -> sprintf "fastq %s" v.fastq_comment 
          | RT_expression   (v, t) -> sprintf "%s Expr %s"
            (string_of_type t) (string_of_expression v)
        in
        sprintf "VAL:%S = [ %s; ... %d updates ... ]"
          (Path.str v.val_id)  s (List.length v.val_history - 1)

      type runtime = {
        unique_id: string -> string;
        mutable programs: (string * (string * expression * dsl_type) list) list;
        mutable values: (Path.t, runtime_value) HT.t;
      }
      let create () =
        let ids = ref 0 in
        {unique_id = (fun s -> incr ids; sprintf "%s%d" s !ids);
         programs = [];
         values = HT.create 42}
      let add_value rt v =
        HT.add rt.values v.val_id v
      let get_value rt id =
        HT.find rt.values id
      exception Found of (Path.t * runtime_value)
      let find_value ~f rt =
        try
          HT.iter (fun path v ->
            if f path v then
              raise (Found (path, v))
          ) rt.values;
          None
        with
          Found (p, v) -> Some (p, v)
      let type_of_value rt path =
        match get_value rt path with
        | Some s ->
          begin match current_value s with
          | RT_int            v    -> Ok T_int
          | RT_string         v    -> Ok T_string
          | RT_file           v    -> Ok T_file  
          | RT_checked_file (v, _) -> Ok T_file
          | RT_fastq          v    -> Ok T_fastq
          | RT_expression   (v, t) -> Ok t
          end
        | None -> Bad (sprintf "Value %s not found" (Path.str path))
        
      let print_runtime ?(indent=0) rt =
        let strindent = String.make indent ' ' in
        HT.iter (fun id v ->
          printf "%s%s\n" strindent (string_of_runtime_value v)
        ) rt.values

      let load_program rt prog_id program =
        let type_checked = 
          Verify.type_check_program ~check_externals:(type_of_value rt) program
        in
        let validate () =
          if program = [] then failwith "Won't load an empty program";
          List.map2 (fun (name, tres) (name, code) ->
            match tres with
            | Ok t -> 
              let path = [prog_id; name] in
              begin match get_value rt path with
              | Some s ->
                failwith (sprintf "Program/Module %S already exists"
                            (Path.str path))
              | None -> (name, code, t)
              end
            | Bad reason ->
              failwith 
                (sprintf "Can't load programs that do not type-check: %s: %s"
                   name reason)
          ) type_checked program
        in
        try 
          let validated = validate () in
          rt.programs <- (prog_id, validated) :: rt.programs;
          List.iter (fun (name, expr, t) ->
            let path = [prog_id; name] in
            add_value rt (rt_value path (RT_expression (expr, t)))
          ) validated;
          Ok ()
        with
          Failure s -> 
            Bad (`wrong_request s)

      module Transform = struct

        type optimization = [ `to_eleven ]

        let factorize_files rt =
          let new_file =
            let i = ref 0 in
            (fun file_expr ->
              let f path v =
                match current_value v with
                | RT_expression (e, _) -> e = file_expr 
                | _ -> false in
              match find_value ~f rt with
              | Some (p, f) -> External (p, T_file)
              | None ->
                let name = incr i; sprintf "__file_%d" !i in
                let path = [ "__files"; name ] in
                add_value rt 
                  (rt_value path (RT_expression (file_expr, T_file)));
                External (path, T_file)) in
          let rec transform_expression = function
            | Constant (File f) as e -> new_file e
            | Constant _ as e -> e
            | Variable _ as e -> e
            | External _ as e -> e
            | Load_fastq  e -> Load_fastq (transform_expression e)
            | Bowtie (e1, e2) -> 
              Bowtie (transform_expression e1, transform_expression e2) in
          HT.iter (fun path v ->
            match current_value v with
            | RT_expression (e, t) -> 
              let new_expr = transform_expression e in
              if e <> new_expr then
                update_value v (RT_expression (new_expr, t))
            | _ -> ()
          ) rt.values

        let optimize (how: optimization) rt =
          match how with
          | `to_eleven -> factorize_files rt

      end

      let compile_runtime ?(optimize=`to_eleven) rt =
        Transform.optimize optimize rt;
        ()

    end

  end


  (* Client or Server side *)
  let check_program prog =
    (* syntax / type checking / (optional) database check *)
    ()

  (* Client to Server transfer *)
  let load_program prog =
    (* if prog not trusted i.e. from client: re-check  *)
    (* create id, add program to database *)
    "prog unique id"

  (* On Server *)
  let compile_program prog_id other_parameters =
    (* look for internal and external redundancies *)
    (* optimize *)
    (* prepare the PBS script *)
    ()

  (* On Server *)
  let run_program prog_id =
    (* prepare database/filesystem -- check dependencies (bowtie) *)
    (* call pbs *)
    (* monitor execution, store intermediate states *)
    (* get and store results *)
    ()

  (* Client or Server *)
  let access_state prog_id =
    "current state / results"



end


module Runtime = DSL.Runtime.Simulation_backend
let runtime = Runtime.create ()

let test name p =
  printf "=== Program %S ===\n" name;
  DSL.print_program ~indent:2 p;
  printf "  Type Checking:\n";
  List.iter (fun (n, res) ->
    printf "    %s : %s\n" n 
      (match res with
      | Ok t -> DSL.string_of_type t
      | Bad b -> b);
  ) (DSL.Verify.type_check_program p);
  begin match Runtime.load_program runtime name p with
  | Ok () -> printf "  Program loaded\n"
  | Bad s -> printf "  Program can't be loaded:\n    %s\n"
    (Runtime.string_of_error s)
  end;
  ()


let () =
  let open DSL.Construct in 
  test "good" [
    "myfile", (file "/path/to/myfile.fastq");
    "bowtie", (bowtie (load_fastq (var "myfile")) 42);
    "rebowtie", (bowtie (var "bowtie") 51);
  ];
  test "bad" [
    "wrong_name", (var "__two_underscores");
    "wrong_file", load_fastq (int 42);
    "wrong_var", var "nope";
    "bad_bowtie", (bowtie (var "bowtie") 51);
  ];
  test "with_wrong_external" [
    "wrong_external", (ext_int ["good"; "myfile"])
  ];
  test "optimizable" [
    "bowtie", (bowtie (load_fastq (file "/path/to/samefile")) 42);
    "rebowtie", (bowtie (load_fastq (file "/path/to/samefile")) 51);
  ];
  test "good" [ "myfile", (int 42) ];
  test "good" [ "added_value", (int 42) ];
  test "good" [];
  printf "===== Current Runtime =====\n";
  Runtime.print_runtime ~indent:2 runtime;
  printf "=== Compiling\n";
  Runtime.compile_runtime runtime;
  printf "===== Current Runtime =====\n";
  Runtime.print_runtime ~indent:2 runtime;
  ()
    
