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

# bucketsHosts = [("copyProdMain", "qrs1004.nads.bf1.yahoo.com"), ("sonBlink", "qrs1000.nads.bf1.yahoo.com")]
bucketsHosts = [("sonBlink", "visitor2.nads.bf1.yahoo.com")]
#bucketsHosts = [("copyProdMain", "qrs1004.nads.bf1.yahoo.com"), ("sonBlink", "vespa9.dev.nads.ir2.yahoo.com")]
# profiles_file = "son_profiles_2"
profiles_file = "/Users/ofek/son_profiles"  # "/Users/ofek/son_profiles_sorted"

totalNumOfAdsReturnedFromSearchPattern = re.compile(r'.*totalNumOfAdsReturnedFromSearch=(\d*).*')
totalNumOfAdsWithMissingAssetsPattern = re.compile(r'.*missingRequiredAssets=(\d*).*')
totalNumOfHitBeforeRenderingAfterAdsRequestedCappingAndAdvertiserDedupPattern = re.compile(r'.*totalNumOfHitBeforeRenderingAfterAdsRequestedCappingAndAdvertiserDedup=(\d*).*')
# [FINAL_BID_BIAS, BIASED_BID, native_pCTR]
creativesPatternRegular = re.compile(r'<hit relevancy=\"([\d+\.E-]*)\" source=\"consolidatedSearchOnNative\">[.\s](\s*<field name.*)*\s*<field name=\"creative_id\">(\d*)<\/field>\s*(\s*<field name.*)*\s*(\s*<struct-field name.*)*\s*<struct-field name=\"rankingExpression\(native_pCTR\)\">(\d+\.\d+)')
# creativesPatternBlink = re.compile(r'<hit relevancy=\"([\d+\.E-]*)\" source=\"blinkSon\">[.\s](\s*<field name.*)*\s*<field name=\"creative_id\">(\d*)<\/field>\s*(\s*<field name.*)*\s*(\s*<struct-field name.*)*\s*<struct-field name=\"rankingExpression\(FINAL_BID_BIAS\)\">(\d+\.\d+)')
creativesPatternBlink = re.compile(r'<hit relevancy=\"([\d+\.E-]*)\" source=\"blinkSon\">[.\s](\s*<field name.*)*\s*<field name=\"creative_id\">(\d*)<\/field>\s*(\s*<field name.*>)*\s*(<field name="summaryfeatures">).*rankingExpression\(native_pCTR\)=([\d+\.E-]*)')

def getKey(item):
    return item[0]


# visitor1.nads.bf1.yahoo.com:4080/search/?debug=false&queryProfile=curveball&bucketId=sonBlink&useDPA=false&enableRtb=false&useDPASearchProspecting=false&presentation.format=xml&sonFederationTimeoutMs=120&timeout=130
def make_call(host, bucket, profile, q):
    query_args = {}
    query_args['debug'] = 'false'
    query_args['queryProfile'] = 'curveball'
    query_args['bucketId'] = bucket
    #query_args['useBlinkSON'] = 'false'
    # query_args['customBlinkSonHost'] = 'son1.nads.bf1.yahoo.com'
    query_args['customBlinkSonHost'] = 'son4.nads.bf1.yahoo.com'
    query_args['useDPA'] = 'false'
    query_args['enableRtb'] = 'false'
    query_args['useDPASearchProspecting'] = 'false'
    query_args['presentation.format'] = 'CBProtobufRendererToString'
    # query_args['sonFederationTimeoutMs'] = '130'
    # query_args['timeout'] = '0.13'
    # query_args["searchRequestTimeoutMs"] = '100'
    query_args['sonFederationTimeoutMs'] = '5030'
    query_args['timeout'] = '1300'
    query_args["searchRequestTimeoutMs"] = '10000'
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
    noAdsInBlink = 0
    noAdsInProd = 0
    totalNumOfProfilesChecks = 0
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

        totalNumOfProfilesChecks += 1

        # analyze the responses
        for job in iter(q.get, None):

            bucket = job[0]
            response = job[1].content

            # creatives ids
            if bucket == "sonBlink":
                noAdsInBlink += int(response.count("\"SEARCH\"") == 0)
            else:
                noAdsInProd += int(response.count("\"SEARCH\"") == 0)

            if q.empty():
                break

        print(str(count) + "/100")

        if count % 20 == 0:
            print "+++"
            print "noAdsInBlink: " + str(noAdsInBlink) + "[" + str((noAdsInBlink/float(totalNumOfProfilesChecks)) * 100) + "%]"
            print "noAdsInProd: " + str(noAdsInProd) + "[" + str((noAdsInProd/float(totalNumOfProfilesChecks)) * 100) + "%]"
            print "==="
            print "totalNumOfProfilesChecks: " + str(totalNumOfProfilesChecks)

        if count > 100:
            break

print "+++ FINAL +++"
print "noAdsInBlink: " + str(noAdsInBlink) + "[" + str((noAdsInBlink/float(totalNumOfProfilesChecks)) * 100) + "%]"
print "noAdsInProd: " + str(noAdsInProd) + "[" + str((noAdsInProd/float(totalNumOfProfilesChecks)) * 100) + "%]"
print "==="
print "totalNumOfProfilesChecks: " + str(totalNumOfProfilesChecks)
