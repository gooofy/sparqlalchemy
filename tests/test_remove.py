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
import datetime
import rdflib
from rdflib.plugins.sparql.parserutils import CompValue

from nltools import misc
from sparqlalchemy.sparqlalchemy import SPARQLAlchemyStore

class TestRemove (unittest.TestCase):

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

        samplefn = 'tests/dt.n3'

        with codecs.open(samplefn, 'r', 'utf8') as samplef:

            data = samplef.read()

            self.sas.parse(data=data, context=self.context, format='n3')

    # @unittest.skip("temporarily disabled")
    def test_remove(self):

        q = """
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <http://schema.org/>
            PREFIX dbr: <http://dbpedia.org/resource/>
            PREFIX dbo: <http://dbpedia.org/ontology/>
            PREFIX hal: <http://hal.zamia.org/kb/>
            SELECT ?d ?p
            WHERE {
                hal:sun_Washington__D_C__20161209 ?p ?d.
            }
            """

        res = self.sas.query(q)
        self.assertEqual(len(res), 7)

        self.sas.remove((u'http://hal.zamia.org/kb/sun_Washington__D_C__20161209', u'http://hal.zamia.org/kb/dawn', None, self.context))

        res = self.sas.query(q)
        self.assertEqual(len(res), 6)

        self.sas.remove((u'http://hal.zamia.org/kb/sun_Washington__D_C__20161209', None, None, self.context))

        res = self.sas.query(q)
        self.assertEqual(len(res), 0)

if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    
    unittest.main()

