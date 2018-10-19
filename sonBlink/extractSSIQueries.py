# looking for a profile tht causes the filter 'totalNumOfAdsReturnedFromSearch' to be 0

import subprocess
import sys
import re
import urllib2
import urllib
import requests
import threading
import time
from pexpect import pxssh
import base64
import re
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl import load_workbook
from multiprocessing import Pool, Process
from subprocess import PIPE, Popen
from threading import Thread
from Queue import Queue, Empty

bucketsHosts = [("sonBlink", "visitor1.nads.bf1.yahoo.com"), ("copyProdMain", "visitor2.nads.bf1.yahoo.com")]
# profiles_file = "son_profiles"
profiles_file = "/Users/ofek/son_profiles"

# totalNumOfAdsReturnedFromSearchPattern = re.compile(r'.*totalNumOfAdsReturnedFromSearch=(\d*).*')
# totalNumOfHitBeforeRenderingAfterAdsRequestedCappingAndAdvertiserDedupPattern = re.compile(r'.*totalNumOfHitBeforeRenderingAfterAdsRequestedCappingAndAdvertiserDedup=(\d*).*')
# sources = ["blinkSon", "consolidatedSearchOnNative"]
#rr = r'<hit relevancy=\"([\d+\.E-]*)\" source=\"blinkSon\">[.\s](\s*<field name.*)*\s*<field name=\"creative_id\">(\d)*<\/field>\s*(\s*<field name.*)*\s*(\s*<struct-field name.*)*\s*<struct-field name=\"rankingExpression\(FINAL_BID_BIAS\)\">(\d+\.\d+)'
#creativesPatternRegular = re.compile(r'<hit relevancy=\"([\d+\.E-]*)\" source=\"consolidatedSearchOnNative\">[.\s](\s*<field name.*)*\s*<field name=\"creative_id\">(\d*)<\/field>')
# creativesPatternRegular = re.compile(r'<hit relevancy=\"([\d+\.E-]*)\" source=\"consolidatedSearchOnNative\">[.\s](\s*<field name.*)*\s*<field name=\"creative_id\">(\d*)<\/field>\s*(\s*<field name.*)*\s*(\s*<struct-field name.*)*\s*<struct-field name=\"rankingExpression\(FINAL_BID_BIAS\)\">(\d+\.\d+)')
# creativesPatternBlink = re.compile(r'<hit relevancy=\"([\d+\.E-]*)\" source=\"blinkSon\">[.\s](\s*<field name.*)*\s*<field name=\"creative_id\">(\d*)<\/field>\s*(\s*<field name.*)*\s*(\s*<struct-field name.*)*\s*<struct-field name=\"rankingExpression\(FINAL_BID_BIAS\)\">(\d+\.\d+)')
ssiRequestPattern = re.compile(r'<field name=\"uri\">(.*)<\/field>')


def getKey(item):
    return item[0]


# visitor1.nads.bf1.yahoo.com:4080/search/?debug=false&queryProfile=curveball&bucketId=sonBlink&useDPA=false&enableRtb=false&useDPASearchProspecting=false&presentation.format=xml&sonFederationTimeoutMs=120&timeout=130
def make_call(host, bucket, profile, q):
    query_args = {}
    query_args['debug'] = 'true'
    query_args['queryProfile'] = 'curveball'
    query_args['bucketId'] = bucket
    query_args['useDPA'] = 'false'
    query_args['enableRtb'] = 'false'
    query_args['useDPASearchProspecting'] = 'false'
    query_args['presentation.format'] = 'xml'
    query_args['sonFederationTimeoutMs'] = '5000'
    query_args['timeout'] = '1530'
    query_args["searchRequestTimeoutMs"] = '1000'
    #query_args['useBidBias'] = 'true'
    url_values = urllib.urlencode(query_args)

    # cont_len = len(profile)
    # req = urllib2.Request("http://" + host + ":4080/search/?" + url_values, profile, {'Content-Length': cont_len})
    # f = urllib2.urlopen(req)
    # response = f.read()
    # f.close()

    response = requests.post("http://" + host + ":4080/search/?" + url_values, data=profile)

    q.put((bucket, response));


with open(profiles_file, "r") as profiles_file:
    # [{"profilePrefix": <string>, "bucketsAnalysis": {...}}]
    analysis = []
    for count, profile in enumerate(profiles_file):
        # print("profile: [" + profilePrefix + "]")
        if profile.strip() == "":
            continue

        q = Queue()
        threads = []

        for i in range(len(bucketsHosts)):
            t = Thread(target=make_call, args=(bucketsHosts[i][1], bucketsHosts[i][0], profile, q))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        # analyze the responses

        ssiRequests = []

        for job in iter(q.get, None):
            bucket = job[0]
            response = job[1].content

            matches = ssiRequestPattern.findall(response)
            if any(matches):
                ssiRequest = matches[0]
                ssiRequest = ssiRequest.replace("&amp;", "&")
                ssiRequest = ssiRequest.replace("&nr=0", "")
                ssiRequests.append(ssiRequest)
                # print(ssiRequest)

            if q.empty():
                break

        if len(ssiRequests) == 1:
            print ("Only one" + ssiRequests[0])
        elif len(ssiRequests) == 2 and ssiRequests[0] != ssiRequests[1]:
            print ("Difference " + ssiRequests[0] + ", " + ssiRequests[1])
        elif len(ssiRequests) == 2:
            print("OK - " + ssiRequests[0])

        if count > 10:
            break

print "==="

print(ssiRequests)
