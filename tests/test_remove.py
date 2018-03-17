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

