CREATE TABLE timelog
(
  id         serial         PRIMARY KEY    ,
  table_name varchar(64)    NOT NULL       ,
  object_id  integer        NOT NULL       ,
  inserted   timestamptz                   ,
  requested  timestamptz                   ,
  started    timestamptz                   ,
  completed  timestamptz                   ,
  CONSTRAINT table_obj_unique UNIQUE(table_name,object_id)
);

CREATE TABLE note
(
  id         serial         PRIMARY KEY    ,
  table_name varchar(64)    NOT NULL       ,
  object_id  integer        NOT NULL       ,
  note       text           NOT NULL
);

CREATE TABLE person
(
  id                    serial       PRIMARY KEY,
  first_name            varchar(64)     NOT NULL,
  middle_initial        char                    ,
  last_name             varchar(64)     NOT NULL,
  email                 varchar(128)    NOT NULL
);

CREATE TABLE organism
(
  id   serial         PRIMARY KEY    ,
  name varchar(256)   UNIQUE NOT NULL
);

CREATE TABLE sample
(
  id           serial          PRIMARY KEY                     ,
  name1        varchar(128)    UNIQUE NOT NULL                 ,
  name2        varchar(128)                                    ,
  investigator integer         NOT NULL REFERENCES person(id)  ,
  contact1     integer         REFERENCES person(id)           ,
  contact2     integer         REFERENCES person(id)           ,
  organism     integer         NOT NULL REFERENCES organism(id)
);

CREATE TABLE library
(
  id             serial          PRIMARY KEY               ,
  sample_id      integer         REFERENCES sample(id)     ,
  vial_id        integer         UNIQUE                    , -- should also be NOT NULL
  name           varchar(128)    UNIQUE NOT NULL           ,
  application    varchar(64)                               ,
  stranded       bool                                      ,
  control_type   varchar(128)                              ,
  truseq_control bool                                      ,
  rnaseq_control varchar(128)                              ,
  barcode_type   varchar(32)                               ,
  barcodes       varchar(16)                               ,
  read_type      varchar(8)                                ,
  read_length1   smallint        NOT NULL                  ,
  read_length2   smallint
);

CREATE TABLE qpcr
(
  id             serial          PRIMARY KEY                    ,
  library_id     integer         NOT NULL REFERENCES library(id),
  concentration  real            NOT NULL                       ,
  volume         real            NOT NULL
);

CREATE TABLE bioanalyzer
(
  id                  serial          PRIMARY KEY                    ,
  library_id          integer         NOT NULL REFERENCES library(id),
  pdf_filename        varchar(512)                                   , -- all fields should be NOT NULL
  well_number         smallint                                       ,
  mean_fragment_size  real
);

CREATE TABLE agarose_gel
(
  id                  serial          PRIMARY KEY                    ,
  library_id          integer         NOT NULL REFERENCES library(id),
  pdf_filename        varchar(512)                                   , -- all fields should be NOT NULL
  well_number         smallint                                       ,
  mean_fragment_size  real
);
