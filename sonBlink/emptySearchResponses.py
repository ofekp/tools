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
#bucketsHosts = [("copyProdMain", "visitor1.nads.bf1.yahoo.com")]
# profiles_file = "son_profiles_2"
profiles_file = "/Users/ofek/son_profiles"

totalNumOfAdsReturnedFromSearchPattern = re.compile(r'.*totalNumOfAdsReturnedFromSearch=(\d*).*')
totalNumOfAdsWithMissingAssetsPattern = re.compile(r'.*missingRequiredAssets=(\d*).*')
totalNumOfHitBeforeRenderingAfterAdsRequestedCappingAndAdvertiserDedupPattern = re.compile(r'.*totalNumOfHitBeforeRenderingAfterAdsRequestedCappingAndAdvertiserDedup=(\d*).*')
sources = ["blinkSon", "consolidatedSearchOnNative"]
#rr = r'<hit relevancy=\"([\d+\.E-]*)\" source=\"blinkSon\">[.\s](\s*<field name.*)*\s*<field name=\"creative_id\">(\d)*<\/field>\s*(\s*<field name.*)*\s*(\s*<struct-field name.*)*\s*<struct-field name=\"rankingExpression\(FINAL_BID_BIAS\)\">(\d+\.\d+)'
#creativesPatternRegular = re.compile(r'<hit relevancy=\"([\d+\.E-]*)\" source=\"consolidatedSearchOnNative\">[.\s](\s*<field name.*)*\s*<field name=\"creative_id\">(\d*)<\/field>')
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
    query_args['useDPA'] = 'false'
    query_args['enableRtb'] = 'false'
    query_args['useDPASearchProspecting'] = 'false'
    query_args['presentation.format'] = 'xml'
    query_args['sonFederationTimeoutMs'] = '14140'
    query_args['timeout'] = '5013'
    query_args["searchRequestTimeoutMs"] = '15135'
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

        # {bucketName: {"totalNumOfAdsReturnedFromSearch": <num>, "creatives": [(creativeIdLong, relevancyDouble, final_bid_bias), (34534534, 0.223)]}, bucketName: {...}}
        bucketsAnalysis = {}

        for job in iter(q.get, None):
            bucket = job[0]
            response = job[1].content

            # totalNumOfAdsReturnedFromSearch
            matches = totalNumOfAdsReturnedFromSearchPattern.findall(response)
            totalNumOfAdsReturnedFromSearch = -1
            if any(matches):
                totalNumOfAdsReturnedFromSearch = matches[0]

            matches = totalNumOfAdsWithMissingAssetsPattern.findall(response)
            totalNumOfAdsWithMissingAssets = -1
            if any(matches):
                totalNumOfAdsWithMissingAssets = matches[0]

            # creatives ids
            if bucket == "sonBlink":
                matches = creativesPatternBlink.findall(response)
            else:
                matches = creativesPatternRegular.findall(response)
            creatives = []
            if any(matches):
                # item[2] - creativeId
                # item[0] - relevancy
                creatives = [(item[2], item[0], item[5]) for item in matches];
                sorted(creatives, key=getKey)

            isInconsistent = False
            if bucket == "sonBlink":
                matches = totalNumOfHitBeforeRenderingAfterAdsRequestedCappingAndAdvertiserDedupPattern.findall(response)
                if any(matches):
                    if len(creatives) != int(matches[0]):
                        isInconsistent = True

            bucketsAnalysis[bucket] = {"creatives": creatives, "isInconsistent": isInconsistent, "totalNumOfAdsReturnedFromSearch": totalNumOfAdsReturnedFromSearch, "totalNumOfAdsWithMissingAssets": totalNumOfAdsWithMissingAssets}

            if q.empty():
                break

        analysis.append({"profilePrefix": profile.strip()[0:40], "bucketsAnalysis": bucketsAnalysis})

        print(str(count) + "/10")

        if count > 600:
            break

# print all lines
for dic in analysis:
    print dic["profilePrefix"]
    for bucketName in [item[0] for item in bucketsHosts]:
        print bucketName.ljust(20) + ":  " + str(dic["bucketsAnalysis"][bucketName])

print "==="

# sonBlink, copyProdMain specific stats
equalNumOfReturnedAds = 0
moreAdsInSon = 0
moreAdsInProd = 0
timeoutInBoth = 0
timeoutOnlyInSon = 0
timeoutOnlyInProd = 0
totalNumOfProfilesChecks = 0
prodScoreHigher = 0
sonScoreHigher = 0
equalScores = 0
noAdsInSon = 0
noAdsInProd = 0
for dic in analysis:
    totalNumOfProfilesChecks += 1
    sonVal = dic["bucketsAnalysis"]["sonBlink"]["totalNumOfAdsReturnedFromSearch"]
    prodVal = dic["bucketsAnalysis"]["copyProdMain"]["totalNumOfAdsReturnedFromSearch"]
    equalNumOfReturnedAds += int(sonVal != -1 and sonVal == prodVal)
    moreAdsInSon += int(sonVal != -1 and prodVal != -1 and sonVal > prodVal)
    moreAdsInProd += int(sonVal != -1 and prodVal != -1 and prodVal > sonVal)
    timeoutInBoth += int(sonVal == -1 and prodVal == -1)
    timeoutOnlyInSon += int(sonVal == -1 and prodVal != -1)
    timeoutOnlyInProd += int(prodVal == -1 and sonVal != -1)
    sonCreativesScoreMap = {}
    sonCreativesList = dic["bucketsAnalysis"]["sonBlink"]["creatives"]
    noAdsInSon += int(len(sonCreativesList) == 0)
    for sonCreative in sonCreativesList:
        sonCreativesScoreMap[sonCreative[0]] = sonCreative[1]
    prodCreativesScoreMap = {}
    prodCreativesList = dic["bucketsAnalysis"]["copyProdMain"]["creatives"]
    noAdsInProd += int(len(prodCreativesList) == 0)
    for prodCreative in prodCreativesList:
        prodCreativesScoreMap[prodCreative[0]] = prodCreative[1]

    for sonCreative in sonCreativesScoreMap:
        sonScore = float(sonCreativesScoreMap[sonCreative])
        if sonCreative in prodCreativesScoreMap:
            prodScore = float(prodCreativesScoreMap[sonCreative])  # yeah, looking for the same creative id
            prodScoreHigher += int(prodScore > sonScore)
            sonScoreHigher += int(prodScore < sonScore)
            equalScores += int(prodScore == sonScore)


print ""
print "equalNumOfReturnedAds: " + str(equalNumOfReturnedAds)
print "moreAdsInSon: " + str(moreAdsInSon)
print "moreAdsInProd: " + str(moreAdsInProd)
print "+++"
print "timeoutInBoth: " + str(timeoutInBoth)
print "timeoutOnlyInSon: " + str(timeoutOnlyInSon)
print "timeoutOnlyInProd: " + str(timeoutOnlyInProd)
print "+++"
print "prodScoreHigher: " + str(prodScoreHigher)
print "sonScoreHigher: " + str(sonScoreHigher)
print "equalScores: " + str(equalScores)
print "+++"
print "noAdsInSon: " + str(noAdsInSon) + "[" + str(noAdsInSon/float(totalNumOfProfilesChecks) * 100) + "%]"
print "noAdsInProd: " + str(noAdsInProd) + "[" + str(noAdsInProd/float(totalNumOfProfilesChecks) * 100) + "%]"
print "==="
print "totalNumOfProfilesChecks: " + str(totalNumOfProfilesChecks)
