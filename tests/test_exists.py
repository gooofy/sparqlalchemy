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
from rdflib.plugins.sparql.parserutils import CompValue

from nltools import misc
from sparqlalchemy.sparqlalchemy import SPARQLAlchemyStore

class TestExists (unittest.TestCase):

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
    def test_query_no_vars(self):

        triples = [(rdflib.URIRef('http://dbpedia.org/resource/Helmut_Kohl'), 
                    rdflib.URIRef('http://dbpedia.org/property/deputy'),  
                    rdflib.URIRef('http://dbpedia.org/resource/Klaus_Kinkel'))]

        algebra = CompValue ('SelectQuery', p = CompValue('BGP', triples=triples, _vars=set()),
                                            datasetClause = None, PV = [], _vars = set())

        res = self.sas.query_algebra(algebra)

        self.assertEqual(len(res), 1)

        for row in res:
            s = ''
            for v in res.vars:
                s += ' %s=%s' % (v, row[v])
            logging.debug('algebra result row: %s' % s)

    # # @unittest.skip("temporarily disabled")
    # def test_query_none(self):

    #     vx = rdflib.Variable('X')

    #     triples = [(vx, 
    #                 rdflib.URIRef('http://dbpedia.org/property/office'),  
    #                 rdflib.URIRef('http://dbpedia.org/resource/Chancellor_of_Germany')),
    #                (vx, 
    #                 rdflib.URIRef('http://dbpedia.org/property/termEnd'),  
    #                 None) ]

    #     algebra = CompValue ('SelectQuery', p = CompValue('BGP', triples=triples, _vars=set([vx])),
    #                                         datasetClause = None, PV = [vx], _vars = set([vx]))

    #     res = self.sas.query_algebra(algebra)

    #     self.assertEqual(len(res), 1)

    #     for row in res:
    #         s = ''
    #         for v in res.vars:
    #             s += ' %s=%s' % (v, row[v])
    #         logging.debug('algebra result row: %s' % s)

if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    
    unittest.main()

