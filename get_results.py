#!/usr/bin/env python
import time
date_time = '12.12.2016 12:20:00'
pattern = '%m.%d.%Y %H:%M:%S'
epoch_start = int(time.mktime(time.strptime(date_time, pattern)))

date_time = '12.14.2016 12:20:00'
pattern = '%m.%d.%Y %H:%M:%S'
epoch_end = int(time.mktime(time.strptime(date_time, pattern)))

print epoch_start
print epoch_end

import csv

csv_filename_feeds = 'feeds_' + `epoch_start` + '_' + `epoch_end` +  '.csv'
csv_filename_clf = 'clf_' + `epoch_start` + '_' + `epoch_end` +  '.csv' 

with open(csv_filename_feeds, 'wb') as csvfile_feeds:

    writer = csv.DictWriter(csvfile_feeds, lineterminator='\n',fieldnames=[ "FeedName","GoldAvgRun", "GoldRuns","GoldEventTimeAvg","GoldEventTimeRuns","GoldMapReduceAvg","GoldMapReduceRuns","PublishAvg", "PublishRuns", "PublishMapReduceAvg", "PublishMapReduceRuns", "CompactionAvg", "CompactionRuns", "CompactionMapReduceAvg", "CompactionMapReduceRuns","RetentionAvg", "RetentionRuns"])


    writer.writerow({'FeedName': 'FeedName', 'GoldAvgRun': 'GoldAvgRun', 'GoldRuns': 'GoldRuns', 'GoldEventTimeAvg': 'GoldEventTimeAvg','GoldEventTimeRuns': 'GoldEventTimeRuns', 'GoldMapReduceAvg': 'GoldMapReduceAvg', 'GoldMapReduceRuns': 'GoldMapReduceRuns', 'PublishAvg': 'PublishAvg', 'PublishRuns': 'PublishRuns', 'PublishMapReduceAvg': 'PublishMapReduceAvg', 'PublishMapReduceRuns': 'PublishMapReduceRuns','CompactionAvg': 'CompactionAvg', 'CompactionRuns': 'CompactionRuns', 'CompactionMapReduceAvg': 'CompactionMapReduceAvg', 'CompactionMapReduceRuns': 'CompactionMapReduceRuns', 'RetentionAvg': 'RetentionAvg', 'RetentionRuns': 'RetentionRuns'})

csvfile_feeds.close()

with open(csv_filename_clf, 'wb') as csvfile_clf:

    writer = csv.DictWriter(csvfile_clf, lineterminator='\n',fieldnames=[ "FeedName","GoldAvgRun", "GoldRuns","BridgeJoin","BridgeJoinRuns","ConsolidateClf","ConsolidateClfRuns","CssngSitesJoin","CssngSitesJoinRuns","EventTime","EventTimeRuns","PublishAvg", "PublishRuns", "PublishMapReduceAvg", "PublishMapReduceRuns", "CompactionAvg", "CompactionRuns", "CompactionMapReduceAvg", "CompactionMapReduceRuns","RetentionAvg", "RetentionRuns"])


    writer.writerow({'FeedName': 'FeedName', 'GoldAvgRun': 'GoldAvgRun', 'GoldRuns': 'GoldRuns', 'BridgeJoin': 'BridgeJoinMapReduceAvg','BridgeJoinRuns': 'BridgeJoinMapReduceRuns', 'ConsolidateClf': 'ConsolidateClfMapReduceAvg', 'ConsolidateClfRuns': 'ConsolidateClfMapReduceRuns', 'CssngSitesJoin': 'CssngSitesJoinMapReduceAvg', 'CssngSitesJoinRuns': 'CssngSitesJoinMapReduceRuns','EventTime': 'EventTimeMapReduceAvg', 'EventTimeRuns': 'EventTimeMapReduceRuns','PublishAvg': 'PublishAvg', 'PublishRuns': 'PublishRuns', 'PublishMapReduceAvg': 'PublishMapReduceAvg', 'PublishMapReduceRuns': 'PublishMapReduceRuns','CompactionAvg': 'CompactionAvg', 'CompactionRuns': 'CompactionRuns', 'CompactionMapReduceAvg': 'CompactionMapReduceAvg', 'CompactionMapReduceRuns': 'CompactionMapReduceRuns', 'RetentionAvg': 'RetentionAvg', 'RetentionRuns': 'RetentionRuns'})

csvfile_clf.close()

#get bearer token
with open('/home/slptl/.etl_private/auth_token.txt', 'r') as myfile:
    token=myfile.read().replace('\n', '')
print token
clfFeeds = ['AWSV-to-CLF-Workflow','AWSD-to-CLF-Workflow','SMSD-to-CLF-Workflow','SMARTWIFI-to-CLF-Workflow','ATT_WIFI_SESSION-to-CLF-Workflow','NELOS-to-CLF-Workflow']

from get_clf_results import get_clf_results

for clfFeed in clfFeeds: 
 print '============ ' + clfFeed + ' CLF RESULTS====='
 get_clf_results(epoch_start, epoch_end, csv_filename_clf, clfFeed)

Feeds = ['AWSVFeed', 'AWSDFeed', 'SMSDFeed', 'ATT_WIFI_PHONEBOOKFeed' , 'ATT_WIFI_SESSIONFeed', 'SMARTWIFIFeed', 'NELOSFeed', 'BridgeFeed']
from get_feedx_results import get_feedx_results

for feedName in Feeds:
  print '============' + feedName + ' RESULTS ===='
  get_feedx_results(epoch_start, epoch_end, feedName, csv_filename_feeds)


with open(csv_filename_feeds, 'rb') as f:
    reader = csv.reader(f)
    rows = list(reader)

csv_filename_feeds_transposed = csv_filename_feeds + '_transposed.csv'
 
with open(csv_filename_feeds_transposed, 'wb') as csvfile:
  writer = csv.writer(csvfile,  lineterminator='\n')
  for col in xrange(0, len(rows[0])):
    writer.writerow([row[col] for row in rows])

with open(csv_filename_clf, 'rb') as f:
    reader = csv.reader(f)
    rows = list(reader)

csv_filename_clf_transposed = csv_filename_clf + '_transposed.csv'

with open(csv_filename_clf_transposed, 'wb') as csvfile:
  writer = csv.writer(csvfile,  lineterminator='\n')
  for col in xrange(0, len(rows[0])):
    writer.writerow([row[col] for row in rows])

import os
os.system("mail -s \"CSV and CLF Feeds\" -a" +  csv_filename_feeds_transposed + " -a  " + csv_filename_clf_transposed + " eg9661@att.com < output")

