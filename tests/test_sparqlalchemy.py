#!/usr/bin/env python
# -*- coding: utf-8 -*- 

#
# Copyright 2017 Guenter Bartsch, Heiko Schaefer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
import logging
import codecs
import rdflib

from nltools import misc
from sparqlalchemy.sparqlalchemy import SPARQLAlchemyStore

NUM_SAMPLE_ROWS = 153

class TestSPARQLAlchemy (unittest.TestCase):

    def setUp(self):

        config = misc.load_config('.airc')

        #
        # db, store
        #

        db_url = config.get('db', 'url')
        # db_url = 'sqlite:///tmp/foo.db'

        self.sas = SPARQLAlchemyStore(db_url, 'unittests', echo=True)
        self.context = u'http://example.com'
        
        #
        # import triples to test on
        #

        self.sas.clear_all_graphs()

        samplefn = 'tests/triples.n3'

        with codecs.open(samplefn, 'r', 'utf8') as samplef:

            data = samplef.read()

            self.sas.parse(data=data, context=self.context, format='n3')

    # @unittest.skip("temporarily disabled")
    def test_import(self):
        self.assertEqual (len(self.sas), NUM_SAMPLE_ROWS)

    # @unittest.skip("temporarily disabled")
    def test_clear_graph(self):
        self.assertEqual (len(self.sas), NUM_SAMPLE_ROWS)

        # add a triple belonging to a different context
        foo_context = u'http://foo.com'
        self.sas.addN([(u'foo', u'bar', u'baz', rdflib.Graph(identifier=foo_context))])
        self.assertEqual (len(self.sas), NUM_SAMPLE_ROWS + 1)

        # clear context that does not exist
        self.sas.clear_graph(u'http://bar.com')
        self.assertEqual (len(self.sas), NUM_SAMPLE_ROWS + 1)

        # clear context that does exist, make sure other triples survive
        self.sas.clear_graph(self.context)
        self.assertEqual (len(self.sas), 1)

        # add a triple belonging to yet another context
        foo_context = u'http://baz.com'
        self.sas.addN([(u'foo', u'bar', u'baz', rdflib.Graph(identifier=foo_context))])
        self.assertEqual (len(self.sas), 2)

        # test clear_all_graphs

        self.sas.clear_all_graphs()
        self.assertEqual (len(self.sas), 0)

    # @unittest.skip("temporarily disabled")
    def test_query_optional(self):

        sparql = """
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX schema: <http://schema.org/>
                 PREFIX dbr: <http://dbpedia.org/resource/>
                 PREFIX dbo: <http://dbpedia.org/ontology/>
                 SELECT ?leader ?label ?leaderobj 
                 WHERE {
                     ?leader rdfs:label ?label. 
                     ?leader rdf:type schema:Person.
                     OPTIONAL {?leaderobj dbo:leader ?leader}
                 }
                 """

        res = self.sas.query(sparql)

        self.assertEqual(len(res), 24)

        for row in res:
            s = ''
            for v in res.vars:
                s += ' %s=%s' % (v, row[v])
            logging.debug('sparql result row: %s' % s)

    # @unittest.skip("temporarily disabled")
    def test_query_limit(self):

        sparql = """
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX schema: <http://schema.org/>
                 PREFIX dbr: <http://dbpedia.org/resource/>
                 PREFIX dbo: <http://dbpedia.org/ontology/>
                 SELECT ?leader ?label ?leaderobj 
                 WHERE {
                     ?leader rdfs:label ?label. 
                     ?leader rdf:type schema:Person.
                     OPTIONAL {?leaderobj dbo:leader ?leader}
                 }
                 LIMIT 1
                 """

        res = self.sas.query(sparql)

        self.assertEqual(len(res), 1)

    # @unittest.skip("temporarily disabled")
    def test_query_filter(self):

        sparql = """
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX schema: <http://schema.org/>
                 PREFIX dbr: <http://dbpedia.org/resource/>
                 PREFIX dbo: <http://dbpedia.org/ontology/>
                 SELECT ?leader ?label ?leaderobj 
                 WHERE {
                     ?leader rdfs:label ?label. 
                     ?leader rdf:type schema:Person.
                     OPTIONAL {?leaderobj dbo:leader ?leader}
                     FILTER (lang(?label) = 'de')
                 }
                 """

        res = self.sas.query(sparql)

        self.assertEqual(len(res), 2)

        for row in res:
            s = ''
            for v in res.vars:
                s += ' %s=%s' % (v, row[v])
            logging.debug('sparql result row: %s' % s)

        sparql = """
                 PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
                 PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX schema: <http://schema.org/>
                 PREFIX dbr:    <http://dbpedia.org/resource/>
                 PREFIX dbo:    <http://dbpedia.org/ontology/>
                 PREFIX owl:    <http://www.w3.org/2002/07/owl#> 
                 PREFIX wdt:    <http://www.wikidata.org/prop/direct/> 
                 SELECT ?label ?birthPlace ?wdgenderlabel
                 WHERE {
                     ?chancellor rdfs:label ?label.
                     ?chancellor dbo:birthPlace ?birthPlace.
                     ?chancellor rdf:type schema:Person.
                     ?birthPlace rdf:type dbo:Settlement.
                     ?chancellor owl:sameAs ?wdchancellor.
                     ?wdchancellor wdt:P21 ?wdgender.
                     ?wdgender rdfs:label ?wdgenderlabel.
                     FILTER (lang(?label) = 'de')
                     FILTER (lang(?wdgenderlabel) = 'de')
                 }"""

        res = self.sas.query(sparql)

        self.assertEqual(len(res), 2)

        for row in res:
            s = ''
            for v in res.vars:
                s += ' %s=%s' % (v, row[v])
            logging.debug('sparql result row: %s' % s)

        sparql = """
                 PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
                 PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX schema: <http://schema.org/>
                 PREFIX dbr:    <http://dbpedia.org/resource/>
                 PREFIX dbo:    <http://dbpedia.org/ontology/>
                 PREFIX dbp:    <http://dbpedia.org/property/>
                 PREFIX owl:    <http://www.w3.org/2002/07/owl#> 
                 PREFIX wdt:    <http://www.wikidata.org/prop/direct/> 
                 SELECT ?label ?leaderof
                 WHERE {
                     ?chancellor rdfs:label ?label.
                     ?chancellor rdf:type schema:Person.
                     ?chancellor dbp:office dbr:Chancellor_of_Germany.
                     OPTIONAL { ?leaderof dbo:leader ?chancellor }.
                     FILTER (lang(?label) = 'de')
                 }"""

        res = self.sas.query(sparql)

        self.assertEqual(len(res), 1)

        for row in res:
            s = ''
            for v in res.vars:
                s += ' %s=%s' % (v, row[v])
            logging.debug('sparql result row: %s' % s)

    # @unittest.skip("temporarily disabled")
    def test_distinct(self):

        sparql = """
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX schema: <http://schema.org/>
                 PREFIX dbr: <http://dbpedia.org/resource/>
                 PREFIX dbo: <http://dbpedia.org/ontology/>
                 SELECT DISTINCT ?leader  
                 WHERE {
                     ?leader rdfs:label ?label. 
                     ?leader rdf:type schema:Person.
                 }
                 """

        res = self.sas.query(sparql)

        self.assertEqual(len(res), 2)

        for row in res:
            s = ''
            for v in res.vars:
                s += ' %s=%s' % (v, row[v])
            logging.debug('sparql result row: %s' % s)

    # @unittest.skip("temporarily disabled")
    def test_dates(self):

        sparql = """
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX schema: <http://schema.org/>
                 PREFIX dbr: <http://dbpedia.org/resource/>
                 PREFIX dbo: <http://dbpedia.org/ontology/>
                 PREFIX hal: <http://hal.zamia.org/kb/> 
                 SELECT ?temp_min ?temp_max ?precipitation ?clouds ?icon
                 WHERE {
                     ?wev hal:dt_end ?dt_end. 
                     ?wev hal:dt_start ?dt_start.
                     ?wev hal:location dbr:Stuttgart.
                     ?wev hal:temp_min ?temp_min   .
                     ?wev hal:temp_max ?temp_max   .
                     ?wev hal:precipitation ?precipitation .
                     ?wev hal:clouds ?clouds .
                     ?wev hal:icon ?icon .
                     FILTER (?dt_start >= \"2016-12-04T10:20:13+05:30\"^^xsd:dateTime &&
                             ?dt_end   <= \"2016-12-23T10:20:13+05:30\"^^xsd:dateTime)
                 }
                 """

        res = self.sas.query(sparql)

        self.assertEqual(len(res), 2)

        for row in res:
            s = ''
            for v in res.vars:
                s += ' %s=%s' % (v, row[v])
            logging.debug('sparql result row: %s' % s)

    # @unittest.skip("temporarily disabled")
    def test_filter_quads(self):

        quads = self.sas.filter_quads(None, None, None, self.context)
        self.assertEqual(len(quads), NUM_SAMPLE_ROWS)

        quads = self.sas.filter_quads(u'http://dbpedia.org/resource/Helmut_Kohl', None, None, self.context)
        self.assertEqual(len(quads), 73)

        quads = self.sas.filter_quads(u'http://dbpedia.org/resource/Helmut_Kohl', u'http://dbpedia.org/ontology/birthPlace', None, self.context)
        self.assertEqual(len(quads), 2)



if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    
    unittest.main()

