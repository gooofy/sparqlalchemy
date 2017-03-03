#!/usr/bin/env python
# -*- coding: utf-8 -*- 

#
# Copyright 2017 Guenter Bartsch, Heiko Schaefer
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

        config = misc.load_config('.nlprc')

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

