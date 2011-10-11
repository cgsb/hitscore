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
      try
        (snd (List.find (fun (v, t) -> v = var) env))
      with 
        Not_found -> Bad (sprintf "Variable %S not found" var)

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

  module Runtime = struct

    (* Representation of the server run-time in the meta-language *)
    module Simulation_backend = struct

      type not_yet_defined


      (* A "loaded" program, must be type-checked *)
      type compilation_result = not_yet_defined
      
      type runtime_program = {
        
        (* An Id which must be unique *)
        program_id: string;
        original_program: program;
        
        (* A Stack of results of compilation passes *)
        mutable compilations: compilation_result list; 
      }

      type runtime_int = {
        (* kind of pointer / unique id *)
        int_id: string;
        (* again a stack, representing history *)
        mutable int_history: int list;
      }

      (* ... *)

      type runtime_value = 
        | Int of runtime_int

      type meta = {
        mutable programs: (string, runtime_program) Hashtbl.t;
        mutable values: (string, runtime_value) Hashtbl.t;
      }
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
  let compile_program prog_id =
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


let test name p =
  printf "=== Program %S ===\n" name;
  DSL.print_program ~indent:2 p;
  printf "  Type Checking:\n";
  List.iter (fun (n, res) ->
    printf "    %s : %s\n" n 
      (match res with
      | Ok t -> DSL.Verify.string_of_type t
      | Bad b -> b);
  ) (DSL.Verify.type_check_program p)


let () =
  let open DSL.Construct in 
  test "good" [
    "myfile", (file "/path/to/myfile.fastq");
    "bowtie", (bowtie (load_fastq (var "myfile")) 42);
    "rebowtie", (bowtie (var "bowtie") 51)
  ];
  test "bad" [
    "wrong_file", load_fastq (int 42);
    "wrong_var", var "nope";
    "bad_bowtie", (bowtie (var "bowtie") 51)
  ];

  ()
    
