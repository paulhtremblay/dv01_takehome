# dv01_takehome
For demo
========

1. Please unzip files; dataflow cannot read zipped files (but it can read gzip).
2. pip install -r requirements.txt
3. bash run_demo.sh

This will create files to a bucket

4. python dependencies/demo_print.py

Note that my code actually pushes the data to a BQ table, but since you don't have perms
to this table, that would not work.


Notes
======

Using Apache beam, becaus it will parallel process many files. I have uploaded these files 
to a bucket, and access them through <bucket/folder/*> So that thousands of files could
be ingested if needed

Spent 30 minutes trying to figure out which fields to use. Made my best estimate. Could not find fully_paid field
