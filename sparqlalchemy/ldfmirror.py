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
# linked data mirror using LDF endpoints for sourcing triples
#

import os
import sys
import traceback
import codecs
import logging
import time
import urllib
import requests
import rdflib

from urlparse import urlparse

from nltools import misc

class LDFMirror(object):
    """
    Helper class for mirroring triples from LDF endpoints
    """

    def __init__ (self, store, endpoints):
        """
        Create new endpoint mirror helper

        store     -- target sparqlalchemy store
                    
        endpoints -- dict mapping host names to LDF endpoints, e.g. 
                     {
                         'www.wikidata.org': 'https://query.wikidata.org/bigdata/ldf',
                     }
        """

        self.store     = store
        self.endpoints = endpoints

    def _find_endpoint (self, resource):
        parsed_uri = urlparse(resource)
        if parsed_uri.netloc in self.endpoints:
            return self.endpoints[parsed_uri.netloc]
        return None

    def _fetch_ldf (self, s=None, p=None, o=None):

        quads = []

        params = {}
        endpoint = None
        if s:
            params['subject']   = s
            endpoint = self._find_endpoint(s)
        if p:
            params['predicate'] = p
            if not endpoint:
                endpoint = self._find_endpoint(p)
        if o:
            params['object']    = o
            if not endpoint:
                endpoint = self._find_endpoint(o)

        if not endpoint:
            return quads

        logging.info ("LDF: *** fetching from endpoint %s" % endpoint)

        url = endpoint + '?' + urllib.urlencode(params)
        while True:

            response = requests.get(
              url,
              headers = {"accept": 'text/turtle'},
            )

            # logging.debug ('%s response: %d' % (url, response.status_code))

            # for h in response.headers:
            #     logging.debug ('   header %s: %s' % (h, response.headers[h]))

            if response.status_code != 200:
                break

            # extract quads

            # logging.debug('parsing to memory...')

            # logging.debug (response.text)

            memg = rdflib.Graph()
            memg.parse(data=response.text, format='turtle')
          
            for s2,p2,o2 in memg.triples((rdflib.URIRef(s) if s else None, 
                                          rdflib.URIRef(p) if p else None, 
                                          rdflib.URIRef(o) if o else None )):

                # print english labels as we come across them to make mirroring log less boring
                if isinstance(o2, rdflib.term.Literal) \
                    and unicode(p2) == u'http://www.w3.org/2000/01/rdf-schema#label' \
                    and o2.language == u'en':
                    logging.info (u'LDF:   fetched LABEL(en)=%s' % o2)

                # logging.debug ('quad: %s %s %s' % (s2,p2,o2))
                quads.append((s2,p2,o2,self.context))

            # paged resource?
            url = None
            for s2,p2,o2 in memg.triples((None, rdflib.URIRef('http://www.w3.org/ns/hydra/core#nextPage'), None )):
                url = str(o2)
                # logging.debug ('got next page url: %s, %d quads so far' % (url, len(quads)))
            for s2,p2,o2 in memg.triples((None, rdflib.URIRef('http://www.w3.org/ns/hydra/core#next'), None )):
                # logging.debug ('got next page ref: %s' % repr(o))
                url = str(o2)
                # logging.debug ('got next page url: %s, %d quads so far' % (url, len(quads)))

            if not url:
                break

        return quads

    def mirror (self, res_paths, context):
        """ 
        mirror triples from endpoints according to resource paths specified in res_paths.

        Each resource path is a tuple consisting of a list of start resources and a list
        of patterns describing which edges to follow.

        A very simple example of a resource path may consist of just a start resource, e.g.

        ( [ u'wde:AngelaMerkel' ], [] )

        this would mirror the resource u'wde:AngelaMerkel' with all its direct properties but
        not recurse into the graph at all. Imagine we'd also be interested in her birth place
        and statements about positions held we could specify paths:

        ( [ u'wde:AngelaMerkel' ], [ ['wdpd:PlaceOfBirth'], ['wdp:PositionHeld','*'] ] )

        this would mirror all properties of her birth place as well as recurse into all
        wdp:PositionHeld statements and further down to any resource linked from there.

        Lastly, we can also specify basic graph patterns for start points. Using this feature,
        we could mirror all federal chancellors of Germany using this resource path specification:

        ( [ ('wdpd:PositionHeld', 'wde:FederalChancellorOfGermany') ], [ ['wdpd:PlaceOfBirth'], ['wdp:PositionHeld','*'] ] )

        context -- graph context to put mirrored triples into, e.g. 'http://example.com'
        """


        self.context = rdflib.Graph(identifier=context)

        todo = []

        for res_path in res_paths:

            resolved_paths = map( 
                               lambda p: 
                                 map( 
                                   lambda p: (self.store.resolve_shortcuts(p[0]), p[1]) if type(p) is tuple else
                                   self.store.resolve_shortcuts(p), p), res_path[1])

            for resource in res_path[0]:

                if isinstance(resource, basestring):
                    rs = [ self.store.resolve_shortcuts (resource) ]
                else:
                    rs = []
                    for quad in self._fetch_ldf(p=self.store.resolve_shortcuts(resource[0]), 
                                                o=self.store.resolve_shortcuts(resource[1])):
                        rs.append(quad[0])
          
                for r in rs:
                    for resolved_path in resolved_paths:
                        logging.debug ('adding task: %s %s' % (r, repr(resolved_path)))
                        todo.append((r, resolved_path))
               
        start_time = time.time()

        while len(todo)>0:

            resource, path = todo.pop()

            logging.info ('LDF: %8.1fs %5d %s %s' % (time.time() - start_time, len(todo), resource, repr(path)))

            todo_new = set()

            # try to fetch from our triple store
            quads = self.store.filter_quads(s=resource, context=self.context.identifier)

            do_add = False
            if len(quads) == 0:

                quads = self._fetch_ldf (s=resource)
                do_add = True

            # transformations

            if len(path)>0:
                res_filter = path[0]

                if type(res_filter) is tuple:
                    pred, f = res_filter

                    for s,p,o,c in quads:
                        if unicode(p) != pred:
                            continue

                        np, no = f(o)

                        np = self.store.resolve_shortcuts(np)

                        if do_add:
                            quads.append ((s, np, no, c))

                        res_filter = unicode(np)

            if do_add:
                self.store.addN(quads)

            if len(path)>0:

                new_path   = path[1:]

                for s,p,o,c in quads:

                    if not isinstance(o, rdflib.URIRef):
                        continue

                    # logging.debug ('LDF   checking %s %s' % (p, o))

                    if res_filter == '*' or res_filter == unicode(p):

                        # import pdb; pdb.set_trace()

                        task = (o, new_path)

                        # logging.debug ('LDF   adding new task: %s' % repr(task))
                        todo.append(task)

