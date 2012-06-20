#!/usr/bin/env python

'''ASTERIX Parser

Related work:
https://github.com/dehmer/asterix
'''

__copyright__ = '''\
Copyright (C) 2012  Volker Grabsch <vog@notjusthosting.com>

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
'''

import logging
import lxml.etree
import os.path
import struct
import sys

def parse(stream):
    with open(os.path.join(os.path.dirname(__file__), 'asterix-schema.xml')) as schema_file:
        schema = lxml.etree.XML(schema_file.read())
    return parse_any(stream, schema)

def parse_any(stream, schema):
    parsers = {
        '{http://www.profv.de/asterix}schema': parse_asterix,
        '{http://www.profv.de/asterix}fspec': parse_fspec,
        '{http://www.profv.de/asterix}fx': parse_fx,
        '{http://www.profv.de/asterix}multi': parse_multi,
        '{http://www.profv.de/asterix}list': parse_unknown, # TODO
        '{http://www.profv.de/asterix}number': parse_number,
        '{http://www.profv.de/asterix}bool': parse_unknown, # TODO
        '{http://www.profv.de/asterix}enum': parse_unknown, # TODO
        '{http://www.profv.de/asterix}unknown': parse_unknown,
    }
    return parsers[schema.tag](stream, schema)

def parse_asterix(stream, schema):
    logging.debug('BEGIN parse_asterix')
    result = []
    while True:
        logging.debug('Data block %r', len(result) + 1)
        counting_stream = CountingIO(stream)
        cat_s = counting_stream.read(1)
        if len(cat_s) == 0:
            logging.debug('END parse_asterix')
            return result
        (cat,) = struct.unpack('B', cat_s)
        subschema = schema.find('*[@id="cat%03d"]' % (cat,))
        if subschema is None:
            raise Exception('Unable to parse data block with unknown category %r' % (cat,))
        (length,) = struct.unpack('>H', counting_stream.read(2))
        records = []
        while counting_stream.total < length:
            records.append(parse_any(counting_stream, subschema))
        if counting_stream.total != length:
            raise Exception('Unexpected length of data block: parsed %r, expected %r' % (counting_stream.total, length))
        result.append({
            'cat': cat,
            'records': records,
        })
    return result

def parse_fspec(stream, schema):
    logging.debug('BEGIN parse_fspec')
    fspec = []
    while True:
        (fspec_octet,) = struct.unpack('B', stream.read(1))
        fspec_bits = [(fspec_octet >> i) & 1 for i in xrange(0, 8)]
        for i in xrange(7, 0, -1):
            fspec.append(fspec_bits[i])
        if fspec_bits[0] == 0:
            break
    result = {}
    schema_children = list(schema.iterchildren(tag='*'))
    for i, subschema in enumerate(schema_children, 0):
        logging.debug('FRN %r', i + 1)
        if i < len(fspec) and fspec[i] == 1:
            result.update(parse_any(stream, subschema))
    for i in xrange(len(schema_children), len(fspec)):
        if fspec[i] == 1:
            raise Exception('Unknown FRN %r', i + 1)
    logging.debug('END parse_fspec')
    return result

def parse_fx(stream, schema):
    logging.debug('BEGIN parse_fx')
    fx = []
    while True:
        (fx_octet,) = struct.unpack('B', stream.read(1))
        fx_bits = [(fx_octet >> i) & 1 for i in xrange(0, 8)]
        for i in xrange(7, 0, -1):
            fx.append(fx_bits[i])
        if fx_bits[0] == 0:
            break
    # TODO: Replace "bitdump" with a real implementation
    bitdump = ''.join('%d' % (bit,) for bit in fx)
    for subschema in enumerate(schema.iterchildren(tag='*'), 0):
        pass
    logging.debug('parse_fx bitdump=%r', bitdump)
    logging.debug('END parse_fx')
    return {
        schema.attrib['id']: 'UNKNOWN FX ' + bitdump,
    }

def parse_multi(stream, schema):
    logging.debug('BEGIN parse_multi')
    result = {}
    for subschema in schema.iterchildren(tag='*'):
        result.update(parse_any(stream, subschema))
    logging.debug('END parse_multi')
    return result

def parse_number(stream, schema):
    logging.debug('BEGIN parse_number')
    octets = int(schema.attrib['octets'])
    # TODO: Handle signed (twos complement) values
    number = sum(struct.unpack('B', c)[0] << (i * 8) for i, c in enumerate(reversed(stream.read(octets))))
    if schema.attrib.has_key('rshift'):
        rshift = int(schema.attrib['rshift'])
        number *= 1.0 / (1 << rshift)
    if schema.attrib.has_key('factor'):
        factor = float(schema.attrib['factor'])
        number *= factor
    logging.debug('END parse_number')
    return {
        schema.attrib['id']: number,
    }

def parse_unknown(stream, schema):
    logging.debug('BEGIN parse_unknown')
    if schema.attrib.has_key('failure_info'):
        raise Exception('Unknown item of unknown size: %r' % (schema.attrib['failure_info'],))
    octets = int(schema.attrib['octets'])
    logging.warn('Item %r is unknown', schema.attrib['id'])
    hexdump = ' '.join('%02x' % (ord(octet),) for octet in stream.read(octets))
    logging.debug('END parse_unknown')
    return {
        schema.attrib['id']: 'UNKNOWN ' + hexdump,
    }

class CountingIO:
    def __init__(self, stream):
        self.stream = stream
        self.total = 0
    def read(self, size):
        data = self.stream.read(size)
        self.total += len(data)
        return data

def main():
    '''Run the ASTERIX parser as command line tool'''
    # TODO: Add command line switch to enable debug output
    #logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(message)s')
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
    # TODO: Provide XML output, e.g.:
    #       <asterix xmlns="http://www.profv.de/asterix">
    #           <cat062 sac="..." sic="..." .../>
    #           <cat062 sac="..." sic="..." .../>
    #           <cat062 sac="..." sic="..." ...>
    #               <ctn sui="..." stn="..."/>
    #               <ctn sui="..." stn="..."/>
    #               ...
    #           </cat062>
    #           ...
    #       </asterix>
    data_blocks = parse(sys.stdin)
    for data_block in data_blocks:
        print '### data block ###'
        records = data_block['records']
        def key(record):
            return (record['time'], record['sac'], record['sic'], record['track'])
        records.sort(key=key)
        for record in records:
            print '--- record ---'
            for key in sorted(record.keys()):
                print '%r = %r' % (key, record[key])
