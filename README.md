# dv01_takehome
For demo
========

1. Please unzip files; dataflow cannot read zipped files (but it can read gzip).
2. pip install -r requirements.txt


Notes
======

Using Apache beam, becaus it will parallel process many files. I have uploaded these files 
to a bucket, and access them through <bucket/folder/*> So that thousands of files could
be ingested if needed
