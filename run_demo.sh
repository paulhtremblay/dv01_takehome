set -e

#python ingest_csv.py --runner DirectRunner -tl paul-henry-tremblay-general \
#    -p gs://paul-henry-tremblay-general/dv01 \
#	--table-name paul-henry-tremblay.data_engineering.dv01

mkdir -p data_out

python ingest_csv.py --demo \
	--runner DirectRunner \
	-tl f \
    -p data_test \
	--table-name some_project.data_set.table \
	-o data_out


