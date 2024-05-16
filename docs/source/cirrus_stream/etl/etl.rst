CirrusStream ETL
================
ETL in the CirrusStream.
-------------------------
Once incoming data has been preserved locally from clients,
it is now time to perform data engineering tasks such as cleaning and formatting.
The various class members below interact together to produce, from
an unstructured .json file, two levels of log files.


A 'bronze' log file is a copy of an original log file which has been formatted
into a file able to be read into common json parsing algorithms.

A 'silver' log file is a row-column relational structure, generally in .csv format.
Silver log files are over an entire day, for an individual edge client.
