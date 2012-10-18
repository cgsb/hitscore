
type bp_fastx_quality_stats = {
  bfxqs_column: float;
  bfxqs_count: float;
  bfxqs_min: float;
  bfxqs_max: float;
  bfxqs_sum: float;
  bfxqs_mean: float;
  bfxqs_Q1: float;
  bfxqs_med: float;
  bfxqs_Q3: float;
  bfxqs_IQR: float;
  bfxqs_lW: float;
  bfxqs_rW: float;
  bfxqs_A_Count: float;
  bfxqs_C_Count: float;
  bfxqs_G_Count: float;
  bfxqs_T_Count: float;
  bfxqs_N_Count: float;
  bfxqs_Max_count: float;
}
type fastx_quality_stats = bp_fastx_quality_stats list

type 'error agarose_gel = <
  agarose_gel : 'error Hitscore_layout.Classy.agarose_gel_element;
  paths : string list;
>
type 'error bioanalyzer = <
  bioanalyzer : 'error Hitscore_layout.Classy.bioanalyzer_element;
  paths : string list;
>

type 'a delivery = <
  client_fastqs_dir : 'a Hitscore_layout.Classy.client_fastqs_dir_element option;
  oo : 'a Hitscore_layout.Classy.prepare_unaligned_delivery_element;
>

type 'a unaligned = <
  b2fu : 'a Hitscore_layout.Classy.bcl_to_fastq_unaligned_element;
  dmux_summary : Hitscore_interfaces.B2F_unaligned_information.demux_summary option;
  fastx_paths : string list;
  fastx_qss : 'a Hitscore_layout.Classy.fastx_quality_stats_element list;
  fastx_results : 'a Hitscore_layout.Classy.fastx_quality_stats_result_element list;
  generic_vols : 'a Hitscore_layout.Classy.volume_element list;
  generics : 'a Hitscore_layout.Classy.generic_fastqs_element list;
  hiseq_raw : 'a Hitscore_layout.Classy.hiseq_raw_element;
  path : string;
  vol : 'a Hitscore_layout.Classy.volume_element;
>
type 'e demultiplexing = <
  assembly: 'e Hitscore_layout.Classy.assemble_sample_sheet_element option;
  b2f : 'e Hitscore_layout.Classy.bcl_to_fastq_element;
  deliveries : 'e delivery list;
  sample_sheet : 'e Hitscore_layout.Classy.sample_sheet_element option;
  unaligned: 'e unaligned option;
>
  
type 'error hiseq_raw =
  < demultiplexings: 'error demultiplexing list;
    oo : 'error Hitscore_layout.Classy.hiseq_raw_element >

type 'a lane = <
  contacts : 'a Hitscore_layout.Classy.person_element list;
  inputs : 'a Hitscore_layout.Classy.input_library_element list;
  oo : 'a Hitscore_layout.Classy.lane_element >

type 'error flowcell = <
  hiseq_raws: 'error hiseq_raw list;
  lanes : 'error lane list;
  oo: 'error Hitscore_layout.Classy.flowcell_element;
>
type 'error submission = <
  flowcell: 'error flowcell;
  invoices : 'error Hitscore_layout.Classy.invoicing_element list;
  lane :  'error lane;
  lane_index : int;
>

type 'error classy_library = <
  agarose_gels: 'error agarose_gel list;
  bioanalyzers: 'error bioanalyzer list;
  barcoding: 'error Hitscore_layout.Classy.barcode_element list list;
  preparator : 'error Hitscore_layout.Classy.person_element option;
  protocol : 'error Hitscore_layout.Classy.protocol_element option;
  protocol_paths : string list option;
  sample : < organism : 'error Hitscore_layout.Classy.organism_element option;
             sample : 'error Hitscore_layout.Classy.sample_element > option;
  stock : 'error Hitscore_layout.Classy.stock_library_element;
  submissions: 'error submission list;
>
  
type 'a classy_libraries_information = <
  configuration : Hitscore_configuration.Configuration.local_configuration;
  created_on : Hitscore_std.Time.t;
  creation_started_on : Hitscore_std.Time.t;
  libraries : 'a classy_library list;
>

type 'a filtered_classy_libraries_information = <
  static_info : 'a classy_libraries_information;
  filtered_on : Hitscore_std.Time.t;
  configuration : Hitscore_configuration.Configuration.local_configuration;
  libraries : 'a classy_library list;
  qualified_names : string list;
>
