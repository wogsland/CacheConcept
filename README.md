CacheConcept
============

Hack Nashville 5 project - well, my part of it.

This is basically just a quick (MySQL) proof-of-concept for querying a cache table instead of the database for repeated queries. 

Setup
-----

1) Have a MySQL database with some data and the MySQL Connector/J package installed along with your JDK.

2) Open up src/CacheConcept/MyQuery.java, add the cache table to your database, and input the database credentials.

3) Go into the main method of src/CacheConcept/CacheConcept.java and enter select, table, where clause and group by information appropriate to the query you want to run on your database.

4) Compile, run and watch the magic.

References
----------

A paper outlining the HOLAP flavor we're trying to implement:
https://skillupjapan.co.jp/news/rd_product/pdf/data_analytics_2012_3_10_60011.pdf

My teammate's code:
https://github.com/dagilmore/locke
