CREATE USER spec_admin WITH PASSWORD 'SPEC_Admin123456';
ALTER USER spec_admin WITH SUPERUSER;

CREATE USER spec_dwd_admin WITH PASSWORD 'SPEC_DWD_Admin123456';
ALTER USER spec_dwd_admin WITH CREATEDB;
create database spec_dwd;

