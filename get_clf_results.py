#!/usr/bin/env python
def get_clf_results(epoch_start, epoch_end, csv_filename, workFlowName):
 import time
 import requests
 import re
 import json
 from pprint import pprint
  
 nameSpace = 'https://clpd357.sldc.sbc.com:10443/v3/namespaces/PT'
 with open('/home/slptl/.etl_private/auth_token.txt', 'r') as myfile:
    token=myfile.read().replace('\n', '')

 #===================== Clf Gold =========================
 #print 'I am here' 
 r = requests.get(nameSpace + '/apps/ClfApp/workflows/' + workFlowName +'/statistics?start=' + `epoch_start` + '&end=' + `epoch_end`, headers={"Authorization":"Bearer " + token})
 
 #print ' This sucks'
 print `r.text`
 #print `r.request.data` 
 newdata = json.loads(r.text)
 #pprint(newdata)
 
 #gold average run time
 goldAvgRunTime = newdata["avgRunTime"]
 print 'Clf Gold Avg Run Time ' + `goldAvgRunTime`
 
 #gold runs
 goldRuns = newdata["runs"]
 print 'Clf Gold runs ' + `goldRuns`

 bridgeMRruns = 0
 bridgeMRavg = 0
 cssngsitesMRavg = 0
 cssngsitesMRruns = 0
 consolidateMRavg = 0
 consolidateMRruns = 0
 eventimeMRruns = 0
 eventimeMRavg = 0

 for data in newdata["nodes"]:
   jsonkey = str(data)
   pprint(jsonkey)
   #print str(newdata["nodes"][jsonkey]["avgRunTime"])
   #print str(newdata["nodes"][jsonkey]["runs"])
   if 'BridgeJoinMapReduce' in jsonkey:
       bridgeMRavg = str(newdata["nodes"][jsonkey]["avgRunTime"])  
       bridgeMRruns = str(newdata["nodes"][jsonkey]["runs"])

   if 'CssngSitesJoinMap' in jsonkey:
       cssngsitesMRavg = str(newdata["nodes"][jsonkey]["avgRunTime"])
       cssngsitesMRruns = str(newdata["nodes"][jsonkey]["runs"])

   if 'ConsolidateClfMap' in jsonkey:
       consolidateMRavg = str(newdata["nodes"][jsonkey]["avgRunTime"])
       consolidateMRruns = str(newdata["nodes"][jsonkey]["runs"])

   if 'EventTimePartitionMap' in jsonkey:
       eventimeMRavg = str(newdata["nodes"][jsonkey]["avgRunTime"])
       eventimeMRruns = str(newdata["nodes"][jsonkey]["runs"])
 
 #=================clf Gold Jobs ===================================
 
 r = requests.get(nameSpace + '/apps/ClfApp/workflows/' + workFlowName + '/runs?start=' + `epoch_start` + '&end=' + `epoch_end` + '&status=COMPLETED&limit=1000', headers={"Authorization":"Bearer " + token})
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
    print 'CLF Gold Creation Job started at ||' + `newdata[mylen-index-1]["start"]` + '||' + `diff`
 print 'Total jobs ' + `mylen`
 
  #=========clf gold failures
 r = requests.get(nameSpace + '/apps/ClfApp/workflows/' + workFlowName + '/runs?start=' + `epoch_start` + '&end=' + `epoch_end` + '&status=FAILED&limit=1000', headers={"Authorization":"Bearer " + token})
 #print r.text

 newdata = json.loads(r.text)
 #print newdata
 #pprint(newdata)
 #pprint(newdata[0]["start"])

 print 'Total number of CLF gold creation job failures is ' + `len(newdata)`


 #==================== clf Publish =========================
 if 'a' in 'a':  
  r = requests.get(nameSpace + '/apps/ClfApp/workflows/PublishPreparationWorkflow/statistics?start=' + `epoch_start` + '&end=' + `epoch_end`, headers={"Authorization":"Bearer " + token})
 
  if not 'There are no statistics' in r.text:
    newdata = json.loads(r.text)
 
    #pprint(newdata)
 
    #publish average run time
    publishAvgRunTime = newdata["avgRunTime"]
    print 'Clf Publish Avg Run Time ' + `publishAvgRunTime`
 
    #publish runs
    publishRuns = newdata["runs"]
    print 'Clf Publish runs ' + `publishRuns`
 
    if 'PublishMapReduce' in newdata["nodes"]:
      #publish MapReduce
      publishMapReduce = str(newdata["nodes"]["PublishMapReduce"]["avgRunTime"])
      print 'Clf PublishMapReduce avg run time ' + publishMapReduce
 
      publishMapReduceRuns = str(newdata["nodes"]["PublishMapReduce"]["runs"])
      print 'Clf PublishMapReduce runs ' + publishMapReduceRuns
    else:
      print 'No Publish Map Reduce Runs'
      publishMapReduce = 0
      publishMapReduceRuns = 0
  else:
    publishAvgRunTime = 0
    publishRuns = 0
    publishMapReduce = 0
    publishMapReduceRuns = 0

  #=================clf Publish Jobs ===================================
 
  r = requests.get(nameSpace + '/apps/ClfApp/workflows/PublishPreparationWorkflow/runs?start=' + `epoch_start` + '&end=' + `epoch_end` + '&status=COMPLETED&limit=1000', headers={"Authorization":"Bearer " + token})
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
    print 'Clf Publish Preparation Job started at ||' + `newdata[mylen-index-1]["start"]` + '||' + `diff`
  print 'Total jobs ' + `mylen`
 
  #=========clf publish failures
  r = requests.get(nameSpace + '/apps/ClfApp/workflows/PublishPreparationWorkflow/runs?start=' + `epoch_start` + '&end=' + `epoch_end` + '&status=FAILED&limit=1000', headers={"Authorization":"Bearer " + token})
  #print r.text

  newdata = json.loads(r.text)
  #print newdata
  #pprint(newdata)
  #pprint(newdata[0]["start"])

  print 'Total number of CLF publish preparation job failures is ' + `len(newdata)`

  #==================clf Compaction ===========================
 
  r = requests.get(nameSpace + '/apps/ClfApp/workflows/CompactionWorkflow/statistics?start=' + `epoch_start` + '&end=' + `epoch_end`, headers={"Authorization":"Bearer " + token})
 
  if not 'There are no statistics' in r.text:
    newdata = json.loads(r.text)
 
    #pprint(newdata)
 
    #compaction average run time
    compactionAvgRunTime = newdata["avgRunTime"]
    print 'Clf Compaction Avg Run Time ' + `compactionAvgRunTime`
 
    #compaction runs
    compactionRuns = newdata["runs"]
    print 'Clf Compaction runs ' + `compactionRuns`
 
    if 'CompactionMapReduce' in newdata["nodes"]:
      #compaction MapReduce
      compactionMapReduce = str(newdata["nodes"]["CompactionMapReduce"]["avgRunTime"])
      print 'Clf CompactionMapReduce avg run time ' + compactionMapReduce
 
      compactionMapReduceRuns = str(newdata["nodes"]["CompactionMapReduce"]["runs"])
      print 'Clf CompactionMapReduce runs ' + compactionMapReduceRuns
 
    else:
       print 'No Compaction Map Reduce Runs'
       compactionMapReduce = 0
       compactionMapReduceRuns = 0
  else: 
    compactionAvgRunTime = 0
    compactionRuns = 0
    compactionMapReduce = 0
    compactionMapReduceRuns = 0 
 
  #=================clf Compaction Jobs ===================================
 
  r = requests.get(nameSpace + '/apps/ClfApp/workflows/CompactionWorkflow/runs?start=' + `epoch_start` + '&end=' + `epoch_end` + '&status=COMPLETED&limit=1000', headers={"Authorization":"Bearer " + token})
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
    print 'ClfApp Compaction Job started at ||' + `newdata[mylen-index-1]["start"]` + '||' + `diff`
  print 'Total Jobs' + `mylen`

  #=========clf compaction failures 
  r = requests.get(nameSpace + '/apps/ClfApp/workflows/CompactionWorkflow/runs?start=' + `epoch_start` + '&end=' + `epoch_end` + '&status=FAILED&limit=1000', headers={"Authorization":"Bearer " + token})
  #print r.text

  newdata = json.loads(r.text)
  #print newdata
  #pprint(newdata)
  #pprint(newdata[0]["start"])

  print 'Total number of CLF compaction job failures is ' + `len(newdata)`

  #==================clf Retention ===========================
 
  r = requests.get(nameSpace + '/apps/ClfApp/workflows/RetentionWorkflow/statistics?start=' + `epoch_start` + '&end=' + `epoch_end`, headers={"Authorization":"Bearer " + token})
 
  #print r.text
 
  if not 'There are no statistics' in r.text:
   newdata = json.loads(r.text)
 
   #pprint(newdata)
 
   #retenion average run time
   retentionAvgRunTime = newdata["avgRunTime"]
   print 'Clf Retenion Avg Run Time ' + `retentionAvgRunTime`
 
   #retenion runs
   retentionRuns = newdata["runs"]
   print 'Clf Retenion runs ' + `retentionRuns`
 
   if 'RetentionMapReduce' in newdata["nodes"]:
     #retention MapReduce
     retentionMapReduce = str(newdata["nodes"]["RetentionMapReduce"]["avgRunTime"])
     print 'Clf RetentionMapReduce avg run time ' + retentionMapReduce
 
     retentionMapReduceRuns = str(newdata["nodes"]["RetentionMapReduce"]["runs"])
     print 'Clf RetentionMapReduce runs ' + retentionMapReduceRuns
 
   else:
     print 'No Retention Map Reduce Runs'
     retentionMapReduce = 0
     retentionMapReduceRuns = 0
  else:
      print 'There are no stats for Retention'
      retentionAvgRunTime = 0
      retentionRuns = 0
 
  #=================clf Retenion Jobs ===================================
 
  r = requests.get(nameSpace + '/apps/ClfApp/workflows/RetentionWorkflow/runs?start=' + `epoch_start` + '&end=' + `epoch_end` + '&status=COMPLETED&limit=1000', headers={"Authorization":"Bearer " + token})
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
     print 'Clf Retention Job started at ||' + `newdata[mylen-index-1]["start"]` + '||' + `diff`
  print 'Total jobs ' + `mylen`

  # ============== clf retention failures

  r = requests.get(nameSpace + '/apps/ClfApp/workflows/RetentionWorkflow/runs?start=' + `epoch_start` + '&end=' + `epoch_end` + '&status=FAILED&limit=1000', headers={"Authorization":"Bearer " + token})
  #print r.text

  newdata = json.loads(r.text)
  #print newdata
  #pprint(newdata)
  #pprint(newdata[0]["start"])

  print 'Total number of CLF retention job failures is ' + `len(newdata)`
 
  import csv 
  #append to file
  with open(csv_filename, 'a') as csvfile:
   
   writer = csv.DictWriter(csvfile, lineterminator='\n',fieldnames=[ "FeedName","GoldAvgRun", "GoldRuns","BridgeJoin","BridgeJoinRuns","ConsolidateClf","ConsolidateClfRuns","CssngSitesJoin","CssngSitesJoinRuns","EventTime","EventTimeRuns","PublishAvg", "PublishRuns", "PublishMapReduceAvg", "PublishMapReduceRuns", "CompactionAvg", "CompactionRuns", "CompactionMapReduceAvg", "CompactionMapReduceRuns","RetentionAvg", "RetentionRuns"])



   writer.writerow({'FeedName': 'CLF ' + workFlowName, 'GoldAvgRun': goldAvgRunTime, 'GoldRuns': goldRuns, 'BridgeJoin': bridgeMRavg, 'BridgeJoinRuns': bridgeMRruns, 'ConsolidateClf': consolidateMRavg, 'ConsolidateClfRuns': consolidateMRruns, 'CssngSitesJoin': cssngsitesMRavg, 'CssngSitesJoinRuns': cssngsitesMRruns, 'EventTime': eventimeMRavg, 'EventTimeRuns': eventimeMRruns ,'PublishAvg': publishAvgRunTime, 'PublishRuns': publishRuns, 'PublishMapReduceAvg': publishMapReduce, 'PublishMapReduceRuns': publishMapReduceRuns,'CompactionAvg': compactionAvgRunTime, 'CompactionRuns': compactionRuns, 'CompactionMapReduceAvg': compactionMapReduce, 'CompactionMapReduceRuns': compactionMapReduceRuns, 'RetentionAvg': retentionAvgRunTime, 'RetentionRuns': retentionRuns})
  csvfile.close()

