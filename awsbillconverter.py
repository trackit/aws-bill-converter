#!/usr/bin/env python3

import collections
import csv
import sys
import dateutil.parser
import regex


def identity(x):
    return x


Mapping = collections.namedtuple('Mapping', [
    'old_key', 'new_key', 'old_mapper', 'new_mapper'
])


def old_date_mapper(new_date):
    try:
        date = dateutil.parser.parse(new_date)
    except ValueError:
        print('failed parsing "{}"'.format(new_date), file=sys.stderr)
        raise
    return date.strftime('%d-%m-%y %I:%M %p')


def new_date_mapper(old_date):
    try:
        date = dateutil.parser.parse(old_date)
    except ValueError:
        print('failed parsing "{}"'.format(old_date), file=sys.stderr)
        raise
    return date.isoformat() + 'Z'


mappings = (
    Mapping('InvoiceID'        , 'bill/InvoiceId'            , identity        , identity)        ,
    Mapping('PayerAccountId'   , 'bill/PayerAccountId'       , identity        , identity)        ,
    Mapping('LinkedAccountId'  , 'lineItem/UsageAccountId'   , identity        , identity)        ,
    Mapping('ProductName'      , 'product/ProductName'       , identity        , identity)        ,
    Mapping('Operation'        , 'lineItem/Operation'        , identity        , identity)        ,
    Mapping('UsageStartDate'   , 'lineItem/UsageStartDate'   , old_date_mapper , new_date_mapper) ,
    Mapping('UsageEndDate'     , 'lineItem/UsageEndDate'     , old_date_mapper , new_date_mapper) ,
    Mapping('UsageQuantity'    , 'lineItem/UsageAmount'      , identity        , identity)        ,
    Mapping('Rate'             , 'lineItem/UnblendedRate'    , identity        , identity)        ,
    Mapping('Cost'             , 'lineItem/UnblendedCost'    , identity        , identity)        ,
    Mapping('UnBlendedCost'    , 'lineItem/UnblendedCost'    , identity        , identity)        ,
    Mapping('UsageType'        , 'lineItem/UsageType'        , identity        , identity)        ,
    Mapping('AvailabilityZone' , 'lineItem/AvailabilityZone' , identity        , identity)        ,
)


new_constants = {
    'lineItem/LineItemType': 'Usage',
}


old_constants = {
    'RecordType': 'LineItem',
}


mappings_by_old_key = {
    m.old_key: m
    for m in mappings
}


mappings_by_new_key = {
    m.new_key: m
    for m in mappings
}


box_size_re = regex.compile(r'[a-z][0-9]\.[0-9]*[a-z]+')


def old_to_new(old):
    items = list(
        (v, mappings_by_old_key[k])
        for k, v in old.items()
        if k in mappings_by_old_key
    )
    mapped = {
        m.new_key: m.new_mapper(v)
        for v, m in items
    }
    mapped.update(new_constants)
    m = (
        box_size_re.search(mapped['lineItem/UsageType']) or 
        box_size_re.search(old['ItemDescription'])
    )
    instance_type = m.group(0) if m else ''
    mapped['product/instanceType'] = instance_type
    return mapped


def new_to_old(new):
    items = (
        (v, mappings_by_new_key[k])
        for k, v in new.items()
        if k in mappings_by_new_key
    )
    mapped = {
        m.old_key: m.old_mapper(v)
        for v, m in items
    }
    mapped.update(old_constants)
    return mapped


def new_to_old_stream(reader, writer):
    for record in reader:
        record = new_to_old(record)
        writer.writerow(record)


_statement_total_invoice_date_re = regex.compile(r'\d{4}[-/]\d\d[-/]\d\d \d\d:\d\d:\d\d')
def old_to_new_stream(reader, writer):
    punctual = []
    invoice_date = None
    for record in reader:
        if record['RecordType'] == 'LineItem':
            if record['UsageStartDate'] and record['UsageEndDate']:
                record = old_to_new(record)
                writer.writerow(record)
            else: # punctual payment
                punctual.append(record)
        elif record['RecordType'] == 'StatementTotal':
            invoice_date = _statement_total_invoice_date_re.search(record['ItemDescription'])
    for p in punctual:
        p['UsageStartDate'] = p['UsageEndDate'] = invoice_date.group(0)
        p = old_to_new(p)
        writer.writerow(p)
                

if __name__ == '__main__':
    reader = csv.DictReader(sys.stdin)
    writer = csv.DictWriter(sys.stdout, fieldnames=(
        [m.new_key for m in mappings] + list(new_constants.keys()) + ['product/instanceType']
    ))
    writer.writeheader()
    old_to_new_stream(reader, writer)
