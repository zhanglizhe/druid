#!/bin/env python2.7

#
# Licensed to Metamarkets Group Inc. (Metamarkets) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Metamarkets licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

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
