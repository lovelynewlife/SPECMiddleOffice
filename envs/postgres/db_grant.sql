psql -U spec_dataset_admin -d spec_dataset

SPEC_DATA_Admin123456

CREATE USER spec_dataset_analyze_user1 WITH PASSWORD '123456';

\c spec_dataset

GRANT SELECT
ON dataset_cpu2017_summary
TO spec_dataset_analyze_user1;

GRANT SELECT
ON dataset_cpu2017_base_details
TO spec_dataset_analyze_user1;


psql -U spec_dataset_analyze_user1 -d spec_dataset
123456

