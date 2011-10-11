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
      | Load_fastq of expression
      | Bowtie of expression * expression (* fastq x some_other -> ??? *)

    type global = string * expression

    type program = global list

  end

  open Definition


  let string_of_expression e =
    let fastq = fun { fastq_comment; fastq_record; fastq_quality } ->
      sprintf "(%s %s %s)" fastq_comment fastq_record fastq_quality in
    let basic = function
      | Int i -> sprintf "%d" i
      | String s -> sprintf "%S" s
      | Fastq f -> 
        sprintf "(fastq: %s)" (String.concat ", " (List.map fastq f))
      | File f -> sprintf "(file: %s)" f in
    let rec expr = function
      | Constant c -> sprintf "(cst: %s)" (basic c)
      | Variable v -> sprintf "(var: %s)" v
      | Load_fastq e -> sprintf "(load_fastq: %s)" (expr e)
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

  end

  module Verify = struct

    type dsl_type = 
      [ `int
      | `string
      | `fastq
      | `file ]

    let string_of_type = function
      | `int -> "int"
      | `string -> "string"
      | `fastq -> "fastq"
      | `file -> "file"

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
      | Int _ -> Ok `int
      | String _ -> Ok `string
      | Fastq _ -> Ok `fastq
      | File _ -> Ok `file

    let rec type_check_expression env = function
      | Constant a -> atom_type a
      | Variable v -> variable_type env v
      | Load_fastq e -> 
        begin match type_check_expression env e with
        | Ok `file -> Ok `fastq
        | Ok other ->
          Bad (sprintf "Load_fastq expects a file, %S has a wrong type: %S"
                 (string_of_expression e) (string_of_type other))
        | Bad s -> Bad s
        end
      | Bowtie (e1, e2) ->
        begin match type_check_expression env e1, 
          type_check_expression env e2 with
          | Ok `fastq, Ok `int -> Ok `fastq
          | _, _ ->
            Bad "Bowtie expects a `fastq and an `int … error messages \
                  will be better in the future"
        end

    let type_check_program p =
      let env = ref [] in
      List.iter (fun (name, expr) ->
        let tc = type_check_expression !env expr in
        env := (name, tc) :: !env
      ) p;
      List.rev !env

  end

  module Transform = struct

    let factorize_files p =
      let files = ref [] in
      let new_file =
        let i = ref 0 in
        (fun f ->
          match List.find_opt (fun (n, file) -> file = f) !files with
          | Some (n, f) -> Variable n
          | None ->
            let name = incr i; sprintf "__fact_file_%d" !i in
            files := (name, f) :: !files;
            Variable name) in
      let rec transform_expression = function
        | Constant (File f) as e -> new_file e
        | Constant _ as e -> e
        | Variable _ as e -> e
        | Load_fastq  e -> Load_fastq (transform_expression e)
        | Bowtie (e1, e2) -> 
          Bowtie (transform_expression e1, transform_expression e2) in
      let new_prog =
        List.map (fun (n, e) -> (n, transform_expression e)) p in
      (List.rev !files) @ new_prog

    let optimize p = factorize_files p

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

      type meta_value = 
        | RT_int of int
        | RT_string of string
        | RT_file of string
        | RT_checked_file of (string * Digest.t)
        | RT_fastq of fastq
        | RT_expression of expression
      type runtime_value = {
        (* kind of path / unique id / Ocsigen's convention *)
        val_id: Path.t;
        (* again a stack, representing history *)
        mutable val_history: meta_value list;
      }

      let rt_value id v = {val_id = id; val_history = [ v ]}
      let update_value v nv =
        v.val_history <- nv :: v.val_history
      let current_value v = List.hd v.val_history        

      let string_of_runtime_value v =
        let s = 
          match current_value v with
          | RT_int            v    -> sprintf "int %d" v 
          | RT_string         v    -> sprintf "string %s" v
          | RT_file           v    -> sprintf "file %s" v 
          | RT_checked_file (v, _) -> sprintf "checked_file %s" v 
          | RT_fastq          v    -> sprintf "fastq %s" v.fastq_comment 
          | RT_expression     v    -> sprintf "expr %s" (string_of_expression v)
        in
        sprintf "VAL:%S = [ %s; ... %d updates ... ]"
          (Path.str v.val_id)  s (List.length v.val_history - 1)

      type runtime = {
        unique_id: string -> string;
        mutable values: (Path.t, runtime_value) HT.t;
      }
      let create () =
        let ids = ref 0 in
        {unique_id = (fun s -> incr ids; sprintf "%s%d" s !ids);
         values = HT.create 42}
      let add_value rt v =
        HT.add rt.values v.val_id v
      let get_value rt id =
        HT.find rt.values id
      let print_runtime ?(indent=0) rt =
        let strindent = String.make indent ' ' in
        HT.iter (fun id v ->
          printf "%s%s\n" strindent (string_of_runtime_value v)
        ) rt.values

      let load_program rt prog_id program =
        let validate () =        
          let type_checked = Verify.type_check_program program in
          List.iter (fun (name, t) ->
            match t with
            | Ok _ -> 
              let path = [prog_id; name] in
              begin match get_value rt path with
              | Some s ->
                failwith (sprintf "Program/Module %S already exists"
                            (Path.str path))
              | None -> ()
              end
            | Bad reason ->
              failwith 
                (sprintf "Can't load programs that do not type-check: %s: %s"
                   name reason)
          ) type_checked
        in
        try 
          validate ();
          List.iter (fun (name, expr) ->
            let path = [prog_id; name] in
            add_value rt (rt_value path (RT_expression expr))
          ) program;
          Ok ()
        with
          Failure s -> 
            Bad (`wrong_request s)

      let compile_runtime ?(optimize=`to_eleven) rt s =
        HT.iter (fun expr_path rtval ->
          match current_value rtval with
          | RT_expression p -> 
            if optimize = `all then
              () (*update_value rtval (RT_expression (Transform.optimize p))*)
            else
              ()
          | _ -> ()
        ) rt.values

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

let test ?(opt=false) name p =
  printf "=== Program %S ===\n" name;
  DSL.print_program ~indent:2 p;
  printf "  Type Checking:\n";
  List.iter (fun (n, res) ->
    printf "    %s : %s\n" n 
      (match res with
      | Ok t -> DSL.Verify.string_of_type t
      | Bad b -> b);
  ) (DSL.Verify.type_check_program p);
  if opt then (
    printf "  Optimization pass:\n";
    DSL.print_program ~indent:4 (DSL.Transform.optimize p);
  );
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
  test ~opt:true "optimizable" [
    "bowtie", (bowtie (load_fastq (file "/path/to/samefile")) 42);
    "rebowtie", (bowtie (load_fastq (file "/path/to/samefile")) 51);
  ];
  test "good" [ "myfile", (int 42) ];
  printf "===== Current Runtime =====\n";
  Runtime.print_runtime ~indent:4 runtime;
  ()
    
