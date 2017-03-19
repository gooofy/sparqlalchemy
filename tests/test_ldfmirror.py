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
from sparqlalchemy.ldfmirror     import LDFMirror

COMMON_PREFIXES = {
            'rdf':     'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'rdfs':    'http://www.w3.org/2000/01/rdf-schema#',
            'hal':     'http://hal.zamia.org/kb/',
            'dbo':     'http://dbpedia.org/ontology/',
            'dbr':     'http://dbpedia.org/resource/',
            'dbp':     'http://dbpedia.org/property/',
            'xml':     'http://www.w3.org/XML/1998/namespace',
            'xsd':     'http://www.w3.org/2001/XMLSchema#',
            'geo':     'http://www.opengis.net/ont/geosparql#',
            'geo1':    'http://www.w3.org/2003/01/geo/wgs84_pos#',
            'geof':    'http://www.opengis.net/def/function/geosparql/',
            'owl':     'http://www.w3.org/2002/07/owl#',
            'schema':  'http://schema.org/',
            'wde':     'http://www.wikidata.org/entity/',
            'wdes':    'http://www.wikidata.org/entity/statement',
            'wdpd':    'http://www.wikidata.org/prop/direct/',
            'wdps':    'http://www.wikidata.org/prop/statement/',
            'wdpq':    'http://www.wikidata.org/prop/qualifier/',
            'wdp':     'http://www.wikidata.org/prop/',
    }

ENDPOINTS = {
                'www.wikidata.org': 'https://query.wikidata.org/bigdata/ldf',
                'sws.geonames.org': 'http://data.linkeddatafragments.org/geonames',
            }

RESOURCE_ALIASES = {
                      u'wde:Human'                      : u'http://www.wikidata.org/entity/Q5',
                      u'wde:AngelaMerkel'               : u'http://www.wikidata.org/entity/Q567',
                      u'wde:HelmutKohl'                 : u'http://www.wikidata.org/entity/Q2518',
                      u'wde:GerhardSchröder'            : u'http://www.wikidata.org/entity/Q2530',
                      u'wde:PresidentOfGermany'         : u'http://www.wikidata.org/entity/Q25223',
                      u'wde:ComputerScientist'          : u'http://www.wikidata.org/entity/Q82594',
                      u'wde:FederalChancellorOfGermany' : u'http://www.wikidata.org/entity/Q4970706',
                      u'wde:Female'                     : u'http://www.wikidata.org/entity/Q6581072',
                      u'wde:Male'                       : u'http://www.wikidata.org/entity/Q6581097',
                      u'wde:Freudental'                 : u'http://www.wikidata.org/entity/Q61656',
                   }

# wikidata properties

for prefix, iri in [('wdpd',    'http://www.wikidata.org/prop/direct/'),
                    ('wdps',    'http://www.wikidata.org/prop/statement/'),
                    ('wdpq',    'http://www.wikidata.org/prop/qualifier/'),
                    ('wdp',     'http://www.wikidata.org/prop/')]:

    for proplabel, propid in [(u'PlaceOfBirth'               , u'P19'),
                              (u'SexOrGender'                , u'P21'),
                              (u'InstanceOf'                 , u'P31'),
                              (u'PositionHeld'               , u'P39'),
                              (u'Occupation'                 , u'P106'),
                              (u'StartTime'                  , u'P580'),
                              (u'EndTime'                    , u'P582'), 
                              (u'GeoNamesID'                 , u'P1566'), 
                             ]:

        RESOURCE_ALIASES[prefix + ':' + proplabel] = iri + propid


class TestLDFMirror (unittest.TestCase):

    def setUp(self):

        config = misc.load_config('.airc')

        #
        # db, store
        #

        db_url = config.get('db', 'url')
        # db_url = 'sqlite:///tmp/foo.db'

        self.sas = SPARQLAlchemyStore(db_url, 'unittests', echo=True, prefixes=COMMON_PREFIXES, aliases=RESOURCE_ALIASES)
        self.context = u'http://example.com'
        self.sas.clear_all_graphs()
       
        #
        # LDF Mirror
        #

        self.ldfmirror = LDFMirror (self.sas, ENDPOINTS)

    # @unittest.skip("temporarily disabled")
    def test_wikidata_mirror(self):

        # triples = [(rdflib.URIRef('http://dbpedia.org/resource/Helmut_Kohl'), 
        #             rdflib.URIRef('http://dbpedia.org/property/deputy'),  
        #             rdflib.URIRef('http://dbpedia.org/resource/Klaus_Kinkel'))]

        # algebra = CompValue ('SelectQuery', p = CompValue('BGP', triples=triples, _vars=set()),
        #                                     datasetClause = None, PV = [], _vars = set())

        # res = self.sas.query_algebra(algebra)

        # self.assertEqual(len(res), 1)

        # for row in res:
        #     s = ''
        #     for v in res.vars:
        #         s += ' %s=%s' % (v, row[v])
        #     logging.debug('algebra result row: %s' % s)

        # ldf_mirror = LDFMirror(kb, rdflib.Graph(identifier=gn))
        
        RES_PATHS = [
                      ( [ u'wde:AngelaMerkel'],
                        [
                          ['wdpd:PlaceOfBirth'], 
                          ['wdp:PositionHeld','*']
                        ]
                      )
                    ]
        self.ldfmirror.mirror (RES_PATHS, self.context)

        quads = self.sas.filter_quads(u'wde:AngelaMerkel', 'wdpd:PlaceOfBirth', 'wde:Q1138')
        self.assertEqual(len(quads), 1)

        RES_PATHS = [
                      (
                        [ 
                          ('wdpd:PositionHeld', 'wde:FederalChancellorOfGermany'),
                        ],
                        [
                          ['wdpd:PlaceOfBirth'], 
                        ]
                      )
                    ]
        
        # RES_PATHS = [
        #               (
        #                 [ #u'wde:AngelaMerkel', 
        #                   #u'wde:GerhardSchröder',
        #                   ('wdpd:PositionHeld', 'wde:FederalChancellorOfGermany'),
        #                   ('wdpd:PositionHeld', 'wde:PresidentOfGermany'),
        #                   # ('wdpd:Occupation',   'wde:ComputerScientist'),
        #                 ],
        #                 [
        #                   ['wdpd:PlaceOfBirth'], 
        #                   ['wdp:PositionHeld','*']
        #                 ]
        #               )
        #             ]
        
        quads = self.sas.filter_quads(u'wde:HelmutKohl', 'wdpd:PlaceOfBirth', 'wde:Q2910')
        self.assertEqual(len(quads), 0)

        self.ldfmirror.mirror (RES_PATHS, self.context)

        quads = self.sas.filter_quads(u'wde:HelmutKohl', 'wdpd:PlaceOfBirth', 'wde:Q2910')
        self.assertEqual(len(quads), 1)

    # @unittest.skip("temporarily disabled")
    def test_path_lambda(self):

        # path segments can contain tuples with lambda functions that transform the entry

        RES_PATHS = [
                      ( [ u'wde:Freudental'],
                        [
                          [('wdpd:GeoNamesID', lambda l: (rdflib.URIRef('hal:GeoNames'), rdflib.URIRef('http://sws.geonames.org/%s/' % l)))], 
                        ]
                      )
                    ]
        self.ldfmirror.mirror (RES_PATHS, self.context)

        quads = self.sas.filter_quads(u'wde:Freudental', 'hal:GeoNames', 'http://sws.geonames.org/2924888/')
        self.assertEqual(len(quads), 1)

        quads = self.sas.filter_quads('http://sws.geonames.org/2924888/', u'http://www.geonames.org/ontology#countryCode', 'DE')
        self.assertEqual(len(quads), 1)

if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    
    unittest.main()

