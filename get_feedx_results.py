#!/usr/bin/env python
def get_feedx_results(epoch_start, epoch_end, feedName, csv_filename):
 import time
 import requests
 import re
 import json
 import csv
 from pprint import pprint
  
 nameSpace = 'https://clpd357.sldc.sbc.com:10443/v3/namespaces/PTQQ'
 
 #get bearer token
 with open('/home/slptl/.etl_private/auth_token.txt', 'r') as myfile:
    token=myfile.read().replace('\n', '')

 #===================== Feed Gold =========================
 
 r = requests.get(nameSpace + '/apps/' + feedName + '/workflows/GoldCreationWorkflow/statistics?start=' + `epoch_start` + '&end=' + `epoch_end`, headers={"Authorization":"Bearer " + token})
 print `r.text`
 newdata = json.loads(r.text)
 pprint(newdata)
 
 #gold average run time
 goldAvgRunTime = newdata["avgRunTime"]
 print feedName + ' Gold Avg Run Time ' + `goldAvgRunTime`
 
 #gold runs
 goldRuns = newdata["runs"]
 print feedName + ' Gold runs ' + `goldRuns`
 
 if 'EventTimePartitionMapReduce' in newdata["nodes"]:
   #gold EventTimePartitionMapReduce
   goldEventTimePartMapreduce = str(newdata["nodes"]["EventTimePartitionMapReduce"]["avgRunTime"])
   print feedName + ' goldEventTimePartMapreduce avg run time ' + goldEventTimePartMapreduce
 
   goldEventTimePartMapreduceRuns =  str(newdata["nodes"]["EventTimePartitionMapReduce"]["runs"])
   print  feedName + ' goldEventTimePartMapreduce runs ' + goldEventTimePartMapreduceRuns
 
 else:
     print 'No EventTimePartition Map Reduce Runs'
     goldEventTimePartMapreduce  = 0
     goldEventTimePartMapreduceRuns = 0

 if 'GoldCreationMapReduce' in newdata["nodes"]: 
   #gold GoldCreationMapReduce
   GoldCreationMapReduce = str(newdata["nodes"]["GoldCreationMapReduce"]["avgRunTime"])
   print  feedName + ' GoldCreationMapReduce avg run time ' + GoldCreationMapReduce
 
   GoldCreationMapReduceRuns = str(newdata["nodes"]["GoldCreationMapReduce"]["runs"])
   print  feedName + ' GoldCreationMapReduce runs ' + GoldCreationMapReduceRuns
 else:
     print 'No GoldCreation Map Reduce Runs'
     GoldCreationMapReduce = 0
     GoldCreationMapReduceRuns = 0 

 #=================Feed Gold Jobs ===================================
 
 r = requests.get(nameSpace + '/apps/' + feedName + '/workflows/GoldCreationWorkflow/runs?start=' + `epoch_start` + '&end=' + `epoch_end` + '&status=COMPLETED&limit=1000', headers={"Authorization":"Bearer " + token})
 #print r.text
 
 
 
 newdata = json.loads(r.text)
 #print newdata
 #pprint(newdata)
 #pprint(newdata[0]["start"])
 
 mylen = len(newdata)
 for index in range(len(newdata)):
    #print index
    #print  newdata[mylen-index-1]["end"]
    #print  newdata[mylen-index-1]["start"]
    diff = newdata[mylen-index-1]["end"] - newdata[mylen-index-1]["start"]
    print  feedName + ' Gold Creation Job started at ||' + `newdata[mylen-index-1]["start"]` + '||' + `diff`
 print 'Total jobs ' + `mylen`
 
 # ============== feed gold failures

 r = requests.get(nameSpace + '/apps/' + feedName+ '/workflows/GoldCreationWorkflow/runs?start=' + `epoch_start` + '&end=' + `epoch_end` + '&status=FAILED&limit=1000', headers={"Authorization":"Bearer " + token})
 #print r.text

 newdata = json.loads(r.text)
 #print newdata
 #pprint(newdata)
 #pprint(newdata[0]["start"])

 print 'Total number of ' + feedName + ' gold job failures is ' + `len(newdata)`

 
 #==================== Feed Publish =========================
 
 r = requests.get(nameSpace + '/apps/' + feedName + '/workflows/PublishPreparationWorkflow/statistics?start=' + `epoch_start` + '&end=' + `epoch_end`, headers={"Authorization":"Bearer " + token})
 
 newdata = json.loads(r.text)
 
 #pprint(newdata)
 
 #publish average run time
 publishAvgRunTime = newdata["avgRunTime"]
 print  feedName + ' Publish Avg Run Time ' + `publishAvgRunTime`
 
 #publish runs
 publishRuns = newdata["runs"]
 print  feedName + ' Publish runs ' + `publishRuns`
 
 if 'PublishMapReduce' in newdata["nodes"]:
     #publish MapReduce
     publishMapReduce = str(newdata["nodes"]["PublishMapReduce"]["avgRunTime"])
     print  feedName + ' PublishMapReduce avg run time ' + publishMapReduce
 
     publishMapReduceRuns = str(newdata["nodes"]["PublishMapReduce"]["runs"])
     print  feedName + ' PublishMapReduce runs ' + publishMapReduceRuns
 else:
     print 'No Publish Map Reduce Runs'
     publishMapReduce = 0
     publishMapReduceRuns = 0
 
 #=================Feed Publish Jobs ===================================
 
 r = requests.get(nameSpace + '/apps/' + feedName + '/workflows/PublishPreparationWorkflow/runs?start=' + `epoch_start` + '&end=' + `epoch_end` + '&status=COMPLETED&limit=1000', headers={"Authorization":"Bearer " + token})
 #print r.text
 
 
 
 newdata = json.loads(r.text)
 #print newdata
 #pprint(newdata)
 #pprint(newdata[0]["start"])
 
 mylen = len(newdata)
 for index in range(len(newdata)):
    #print index
    #print  newdata[mylen-index-1]["end"]
    #print  newdata[mylen-index-1]["start"]
    diff = newdata[mylen-index-1]["end"] - newdata[mylen-index-1]["start"]
    print  feedName + ' Publish Preparation Job started at ||' + `newdata[mylen-index-1]["start"]` + '||' + `diff`
 print 'Total jobs ' + `mylen`

 # ============== feed publish preparation failures

 r = requests.get(nameSpace + '/apps/' + feedName+ '/workflows/PublishPreparationWorkflow/runs?start=' + `epoch_start` + '&end=' + `epoch_end` + '&status=FAILED&limit=1000', headers={"Authorization":"Bearer " + token})
 #print r.text

 newdata = json.loads(r.text)
 #print newdata
 #pprint(newdata)
 #pprint(newdata[0]["start"])

 print 'Total number of ' + feedName + ' publish preparation failures is ' + `len(newdata)`
 
 #==================Feed Compaction ===========================
 
 r = requests.get(nameSpace + '/apps/' + feedName + '/workflows/CompactionWorkflow/statistics?start=' + `epoch_start` + '&end=' + `epoch_end`, headers={"Authorization":"Bearer " + token})

 if not 'There are no statistics' in r.text:
  newdata = json.loads(r.text) 
 
 
  #pprint(newdata)
 
  #compaction average run time
  compactionAvgRunTime = newdata["avgRunTime"]
  print  feedName + ' Compaction Avg Run Time ' + `compactionAvgRunTime`
 
  #compaction runs
  compactionRuns = newdata["runs"]
  print  feedName + ' Compaction runs ' + `compactionRuns`
 
  if 'CompactionMapReduce' in newdata["nodes"]:
     #compaction MapReduce
     compactionMapReduce = str(newdata["nodes"]["CompactionMapReduce"]["avgRunTime"])
     print  feedName + ' CompactionMapReduce avg run time ' + compactionMapReduce
 
     compactionMapReduceRuns = str(newdata["nodes"]["CompactionMapReduce"]["runs"])
     print  feedName + ' CompactionMapReduce runs ' + compactionMapReduceRuns
 
  else:
     print 'No Compaction Map Reduce Runs'
     compactionMapReduce = 0
     compactionMapReduceRuns = 0
 else:
     print 'There are no stats for Compaction'
     compactionAvgRunTime = 0
     compactionRuns = 0
     compactionMapReduce = 0
     compactionMapReduceRuns = 0
 
 
 #=================Feed Compaction Jobs ===================================
 
 r = requests.get(nameSpace + '/apps/' + feedName + '/workflows/CompactionWorkflow/runs?start=' + `epoch_start` + '&end=' + `epoch_end` + '&status=COMPLETED&limit=1000', headers={"Authorization":"Bearer " + token})
 #print r.text
 
 
 
 newdata = json.loads(r.text)
 #print newdata
 #pprint(newdata)
 #pprint(newdata[0]["start"])
 
 mylen = len(newdata)
 for index in range(len(newdata)):
    #print index
    #print  newdata[mylen-index-1]["end"]
    #print  newdata[mylen-index-1]["start"]
    diff = newdata[mylen-index-1]["end"] - newdata[mylen-index-1]["start"]
    print  feedName + ' Compaction Job started at ||' + `newdata[mylen-index-1]["start"]` + '||' + `diff`
 print 'Total Jobs' + `mylen`
 
 # ============== feed compaction failures

 r = requests.get(nameSpace + '/apps/' + feedName+ '/workflows/CompactionWorkflow/runs?start=' + `epoch_start` + '&end=' + `epoch_end` + '&status=FAILED&limit=1000', headers={"Authorization":"Bearer " + token})
 #print r.text

 newdata = json.loads(r.text)
 #print newdata
 #pprint(newdata)
 #pprint(newdata[0]["start"])

 print 'Total number of ' + feedName + ' compaction job failures is ' + `len(newdata)`


 #==================Feeed Retention ===========================
 
 r = requests.get(nameSpace + '/apps/' + feedName + '/workflows/RetentionWorkflow/statistics?start=' + `epoch_start` + '&end=' + `epoch_end`, headers={"Authorization":"Bearer " + token})
 
 #print r.text
 
 if not 'There are no statistics' in r.text:
  newdata = json.loads(r.text)
 
  #pprint(newdata)
 
  #retenion average run time
  retentionAvgRunTime = newdata["avgRunTime"]
  print  feedName + ' Retenion Avg Run Time ' + `retentionAvgRunTime`
 
  #retenion runs
  retentionRuns = newdata["runs"]
  print  feedName + ' Retenion runs ' + `retentionRuns`
 
  if 'RetentionMapReduce' in newdata["nodes"]:
    #retention MapReduce
    retentionMapReduce = str(newdata["nodes"]["RetentionMapReduce"]["avgRunTime"])
    print  feedName + ' RetentionMapReduce avg run time ' + retentionMapReduce
 
    retentionMapReduceRuns = str(newdata["nodes"]["RetentionMapReduce"]["runs"])
    print  feedName + ' RetentionMapReduce runs ' + retentionMapReduceRuns
 
  else:
     print 'No Retention Map Reduce Runs'
     retentionMapReduce = 0
     retentionMapReduceRuns = 0
 else:
     print 'There are no stats for Retention'
     retentionAvgRunTime = 0
     retentionRuns = 0
     retentionMapReduce = 0
     retentionMapReduceRuns = 0

 
 #=================Feed Retenion Jobs ===================================
 
 r = requests.get(nameSpace + '/apps/' + feedName + '/workflows/RetentionWorkflow/runs?start=' + `epoch_start` + '&end=' + `epoch_end` + '&status=COMPLETED&limit=1000', headers={"Authorization":"Bearer " + token})
# print r.text
 

 newdata = json.loads(r.text)
 #print newdata
 #pprint(newdata)
 #pprint(newdata[0]["start"])
 
 mylen = len(newdata)
 for index in range(len(newdata)):
    #print index
    #print  newdata[mylen-index-1]["end"]
    #print  newdata[mylen-index-1]["start"]
    diff = newdata[mylen-index-1]["end"] - newdata[mylen-index-1]["start"]
    print  feedName + ' Retention Job started at ||' + `newdata[mylen-index-1]["start"]` + '||' + `diff`
 print 'Total jobs ' + `mylen`
 
 # ============== feed retention failures

 r = requests.get(nameSpace + '/apps/' + feedName+ '/workflows/RetentionWorkflow/runs?start=' + `epoch_start` + '&end=' + `epoch_end` + '&status=FAILED&limit=1000', headers={"Authorization":"Bearer " + token})
 #print r.text

 newdata = json.loads(r.text)
 #print newdata
 #pprint(newdata)
 #pprint(newdata[0]["start"])

 print 'Total number of ' + feedName + ' retention job failures is ' + `len(newdata)`


  #append to file
 with open(csv_filename, 'a') as csvfile:
  writer = csv.DictWriter(csvfile, lineterminator='\n',fieldnames=[ "FeedName","GoldAvgRun", "GoldRuns","GoldEventTimeAvg","GoldEventTimeRuns","GoldMapReduceAvg","GoldMapReduceRuns","PublishAvg", "PublishRuns", "PublishMapReduceAvg", "PublishMapReduceRuns", "CompactionAvg", "CompactionRuns", "CompactionMapReduceAvg", "CompactionMapReduceRuns","RetentionAvg", "RetentionRuns"])

  writer.writerow({'FeedName': feedName, 'GoldAvgRun': goldAvgRunTime, 'GoldRuns': goldRuns, 'GoldEventTimeAvg': goldEventTimePartMapreduce,'GoldEventTimeRuns': goldEventTimePartMapreduceRuns, 'GoldMapReduceAvg': GoldCreationMapReduce, 'GoldMapReduceRuns': GoldCreationMapReduceRuns, 'PublishAvg': publishAvgRunTime, 'PublishRuns': publishRuns, 'PublishMapReduceAvg': publishMapReduce, 'PublishMapReduceRuns': publishMapReduceRuns,'CompactionAvg': compactionAvgRunTime, 'CompactionRuns': compactionRuns, 'CompactionMapReduceAvg': compactionMapReduce, 'CompactionMapReduceRuns': compactionMapReduceRuns, 'RetentionAvg': retentionAvgRunTime, 'RetentionRuns': retentionRuns})
 csvfile.close()
