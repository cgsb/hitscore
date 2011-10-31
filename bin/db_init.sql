CREATE TABLE person
(
  person_id             serial                  ,
  first_name            varchar(64)     NOT NULL,
  middle_initial        char                    ,
  last_name             varchar(64)     NOT NULL,
  email                 varchar(128)    NOT NULL,
  CONSTRAINT            person_pk PRIMARY KEY(person_id)
);
