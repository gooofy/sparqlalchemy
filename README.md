# sparqlalchemy

A simple (buy hopefully reasonably efficient) implementation of an RDF triple store on top of SQLAlchemy. 

Fundamental Idea here is to translate SPARQL queries into SQLAlchemy's SQL Expression Language in order to
run them on (hopefully) any SQL database SQLAlchemy supports with reasonable efficiency.

Triples are hosted in a single, flat table.

Currently, re-uses much of RDFLib's infrastructure for parsing tasks.

*Note*: This is work in progress. While I do aim for RDF/SPARQL standards compliance where possible, there
is no (and most probably never will be) any guarantee that this triple store is complete and/or compliant
with regards to any semantic web standards. Efficiency and portability are my main goals here.

Requirements
============

*Note*: very incomplete.

* Python 2.7 
* RDFLib
* SQLAlchemy
* pu-nltools

License
=======

My own code is LGPLv3 licensed unless otherwise noted in the scritp's copyright
headers.

Some scripts and files are based on works of others, in those cases it is my
intention to keep the original license intact. Please make sure to check the
copyright headers inside for more information.

Author
======

Guenter Bartsch <guenter@zamia.org>
Heiko Schäfer <heiko@schaefer.name>

