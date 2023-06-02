CREATE USER spec_admin WITH PASSWORD 'SPEC_Admin123456';
ALTER USER spec_admin WITH SUPERUSER;

psql -U spec_admin -d postgres


CREATE USER spec_dwd_admin WITH PASSWORD 'SPEC_DWD_Admin123456';
ALTER USER spec_dwd_admin WITH CREATEDB;
create database spec_dwd;


CREATE USER spec_tdm_admin WITH PASSWORD 'SPEC_TDM_Admin123456';
ALTER USER spec_tdm_admin WITH CREATEDB;
create database spec_tdm;


CREATE USER spec_dataset_admin WITH PASSWORD 'SPEC_DATA_Admin123456';
ALTER USER spec_dataset_admin WITH CREATEDB;
create database spec_dataset;


