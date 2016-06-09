#!/bin/env python2.7
import json
import gzip

# Regenerate the country code lookup with
# python wikiticker-lookup-generatory.py > wikiticker-country-code-lookup.json
if __name__ == "__main__":
    d = dict()
    with gzip.open('wikiticker-2015-09-12-sampled.json.gz', 'rb') as gzf:
        for line in gzf.readlines():
            j = json.loads(line)
            if(j["countryIsoCode"] and j["countryName"]):
                ccode = j["countryIsoCode"]
                cname = j["countryName"]
                d[ccode] = cname
            pass
        pass
    print json.dumps(d)
