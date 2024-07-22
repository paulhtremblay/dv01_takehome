set -e
#python ingest_csv.py --runner DirectRunner -tl f -p data_test
#python ingest_csv.py --runner DirectRunner -tl paul-henry-tremblay-general \
#   	-p gs://paul-henry-tremblay-general/dv01 \
#	--table-name paul-henry-tremblay.data_engineering.dv01

python ingest_csv.py --runner DirectRunner -tl paul-henry-tremblay-general \
    -p gs://paul-henry-tremblay-general/dv01 \
	--table-name paul-henry-tremblay.data_engineering.dv01


