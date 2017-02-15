#!/usr/bin/env python
# -*- coding: utf-8 -*- 

#
# Copyright 2017 Guenter Bartsch
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

#
# quick'n'dirty dbpedia mirror to files
#

import os
import sys
import traceback
import codecs
import logging
import requests

from time import time
from optparse import OptionParser

from nltools import misc

import pprint
from rdflib.plugins.sparql import parser, algebra
from rdflib import term, Graph

#
# init, cmdline
#

misc.init_app('dbpmirror')

option_parser = OptionParser("usage: %prog [options] foo.aiml")

option_parser.add_option ("-v", "--verbose", action="store_true", dest="verbose",
                   help="verbose output")

(options, args) = option_parser.parse_args()

if options.verbose:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

#
# mirror part of dbpedia
#

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
            'schema':  'http://schema.org/',
    }

query_prefixes = ''.join(map(lambda k: "PREFIX %s: <%s>\n" % (k, COMMON_PREFIXES[k]), COMMON_PREFIXES))

def remote_sparql(endpoint, query, user=None, passwd=None, response_format='application/sparql-results+json'):

    if user:
        auth   = HTTPDigestAuth(user, passwd)
    else:
        auth   = None

    query  = query_prefixes + query
    # print query

    response = requests.post(
      endpoint,
      # data    = '',
      params  = {'query': query},
      headers = {"accept": response_format},
      auth    = auth
    )
    return response

KB_SOURCES=[
              ('http://dbpedia.org/sparql',
               ['dbr:Freudental',
                'dbr:Ludwigsburg',
                'dbr:Stuttgart',
                'dbr:Tallinn',
                'dbr:San_Francisco',
                'dbr:Los_Angeles',
                'dbr:New_York_City', 
                'dbr:London',
                'dbr:Paris',
                'dbr:Reykjavík',
                'dbr:Oberwiesenthal',
                'dbr:Arnstorf',
                'dbr:Hamburg',
                'dbr:Brackenheim',
                'dbr:Heilbronn',
                'dbr:Ludwigshafen',
                '<http://dbpedia.org/resource/Blomberg,_North_Rhine-Westphalia>',
                'dbr:Biberach_an_der_Riss',
                '<http://dbpedia.org/resource/Washington,_D.C.>',
                '<http://dbpedia.org/resource/Fairbanks,_Alaska>', 
                'dbr:Angela_Merkel',
                'dbr:Gerhard_Schröder',
                'dbr:Helmut_Kohl',
               ]
              ) ]

def gen_fn(node):

    return node.replace('<', '_').replace('>', '_').replace('/', '_')


for endpoint, nodes in KB_SOURCES:

    for node in nodes:

        with codecs.open('mirror/%s_1.turtle' % gen_fn(node), 'w', 'utf8') as outf:

            logging.info('importing %s from %s...' % (node, endpoint))

            query = u"""
                     CONSTRUCT {
                        %s ?r ?n .
                     }
                     WHERE {
                        %s ?r ?n .
                     }
                     """ % (node, node)

            logging.debug('query: %s' % (query))

            res = remote_sparql(endpoint, query, response_format='text/turtle')

            if res.status_code != 200:
                raise Exception ('%d: SPARQL request failed: %s query was: %s' % (res.status_code, res.text, query))

            logging.info("importing %s ?r ?n from %s: %d bytes." % (node, endpoint, len(res.text)))

            outf.write(res.text)

        with codecs.open('mirror/%s_2.turtle' % gen_fn(node), 'w', 'utf8') as outf:

            query = u"""
                     CONSTRUCT {
                        ?n ?r %s .
                     }
                     WHERE {
                        ?n ?r %s .
                     }
                     """ % (node, node)

            logging.info('query: %s' % (query))

            res = remote_sparql(endpoint, query, response_format='text/turtle')

            if res.status_code != 200:
                raise Exception ('%d: SPARQL request failed: %s query was: %s' % (res.status_code, res.text, query))

            logging.info("importing %s ?r ?n from %s: %d bytes." % (node, endpoint, len(res.text)))

            outf.write(res.text)

