#!/usr/bin/env python
import json
import requests
from HTMLParser import HTMLParser
from re import sub
from sys import stderr
from traceback import print_exc
import time
import csv
import sys, getopt

date_time = '12.20.2016 00:20:00'
pattern = '%m.%d.%Y %H:%M:%S'
epoch_start_from = int(time.mktime(time.strptime(date_time, pattern)))

date_time = '12.20.2016 02:20:00'
pattern = '%m.%d.%Y %H:%M:%S'
epoch_end_to = int(time.mktime(time.strptime(date_time, pattern)))


class _DeHTMLParser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.__text = []

    def handle_data(self, data):
        text = data.strip()
        if len(text) > 0:
            text = sub('[ \t\r\n]+', ' ', text)
            self.__text.append(text + ' ')

    def handle_starttag(self, tag, attrs):
        if tag == 'p':
            self.__text.append('\n\n')
        elif tag == 'br':
            self.__text.append('\n')

    def handle_startendtag(self, tag, attrs):
        if tag == 'br':
            self.__text.append('\n\n')

    def text(self):
        return ''.join(self.__text).strip()


def dehtml(text):
    try:
        parser = _DeHTMLParser()
        parser.feed(text)
        parser.close()
        return parser.text()
    except:
        print_exc(file=stderr)
        return text

def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""

def main(argv):
    server = ''
    appID = ''
 #   try:
 #     opts, args = getopt.getopt(argv,"hs:a:",["server=","appID="])
 #   except getopt.GetoptError:
 #     print 'test.py -s <server> -a <appID> f'
 #     sys.exit(2)
 #   for opt, arg in opts:
 #     if opt == '-h':
 #        print 'test.py -s <server> -a <appID>'
 #        sys.exit()
 #     elif opt in ("-s", "--server"):
 #        server = arg
 #     elif opt in ("-a", "--appID"):
 #        appID = arg
    date_time = '12.20.2016 00:30:00'
    pattern = '%m.%d.%Y %H:%M:%S'
    epoch_start_from = int(time.mktime(time.strptime(date_time, pattern)))

    date_time = '12.20.2016 01:20:02'
    pattern = '%m.%d.%Y %H:%M:%S'
    epoch_end_to = int(time.mktime(time.strptime(date_time, pattern)))

    server = 'clpd355.sldc.sbc.com:19888'
    r = requests.get('http://' + server + '/jobhistory/')
    #print(dehtml(r.text))
    mydhtml=dehtml(r.text)
    mystr=find_between(dehtml(r.text), "jobsTableData=[ [", "] ]")
    #print(mystr)
    my_list = mystr.split("], [")
    #print my_list
    csv_filename = 'Job_Results' + '.csv'
    with open(csv_filename, 'wb') as csvfile:
     writer = csv.DictWriter(csvfile, lineterminator='\n',fieldnames=[ "Start_Time", "Duration","JobID", "Maps_Total","Maps_Completed","Reduces_Total","Reduces_Completed", "CPU_Map", "CPU_Reduce", "CPU_Total", "VCore_Map","VCore_Reduce","MB_Sec", "VCore_Sec", "Name","Attempts","Containers"])
     writer.writerow({'Start_Time':'Start_Time','Duration': 'Duration', 'JobID': 'JobID' ,'Maps_Total': 'Maps_Total', 'Maps_Completed': 'Maps_Completed', 'Reduces_Total': 'Reduces_Total','Reduces_Completed': 'Reduces_Completed', 'CPU_Map':'CPU_Map', 'CPU_Reduce':'CPU_Reduce','CPU_Total':'CPU_Total','VCore_Map':'VCore_Map','VCore_Reduce':'VCore_Reduce','MB_Sec':'MB_Sec','VCore_Sec':'VCore_Sec','Name':'Name','Attempts':'Attempts','Containers':'Containers'})

    csvfile.close()

    for x in my_list:
       my_inner_list = x.split(",")
       #for y in my_inner_list:
         #print 'Y is ' + y
       date_time_start = find_between(my_inner_list[1],"\"", " CST\"")
       #print 'Value is ' + date_time_start
       pattern = '%Y.%m.%d %H:%M:%S'
       epoch_start = int(time.mktime(time.strptime(date_time_start, pattern)))

       date_time_end = find_between(my_inner_list[2],"\"", " CST\"")
       pattern = '%Y.%m.%d %H:%M:%S'
       epoch_end = int(time.mktime(time.strptime(date_time_end, pattern)))

       duration=epoch_end - epoch_start
       #print 'Duration is ' + `duration`
       jobid= find_between(my_inner_list[3],"\" ", " \"")
       #print 'Job id ' + jobid
       maps_total = find_between(my_inner_list[8],"\"", "\"")
       maps_completed = find_between(my_inner_list[9],"\"", "\"")
       reducers_total = find_between(my_inner_list[10],"\"", "\"")
       reducers_completed = find_between(my_inner_list[11],"\"", "\"")
       user = find_between(my_inner_list[5],"\"", "\"")
       namespace = find_between(my_inner_list[4],"\"", "\"")

       #print namespace
       #print ' Maps total ' + maps_total
       #print ' Maps completed ' + maps_completed
       #print ' Reducers total ' + reducers_total
       #print ' Reducers completed ' + reducers_completed
       #print 'X is ' + x
       #appID = 1475270229635
       if ('cdap' in user and '.PTQ' in namespace and epoch_start < epoch_end_to and epoch_start > epoch_start_from):
           with open(csv_filename, 'a') as csvfile:
            #http://clpd355.sldc.sbc.com:19888/jobhistory/jobcounters/job_1481291418014_2825
            print "here"
            r = requests.get('http://clpd355.sldc.sbc.com:19888/jobhistory/jobcounters/' + jobid)
            my_counters = dehtml(r.text)
            cpu = find_between(my_counters, "CPU time spent (ms)", "Physical memory (bytes)")
            if (len(cpu) > 0): 
             cpu_list = cpu.split(" ")
             #print cpu_list
             cpu_map = str(cpu_list[1]).replace("\"","")
             #print cpu_map
             cpu_reduce = str(cpu_list[2].replace("\"",""))
             #print cpu_reduce
             cpu_total = str(cpu_list[3].replace("\"",""))
             #print cpu_total
            else:
             cpu_list = 0
             cpu_reduce = 0
             cpu_total = 0
            #Total vcore-seconds taken by all reduce tasks 0 0 
            totalVCore_reduce = str(find_between(my_counters, "Total vcore-seconds taken by all reduce tasks 0 0 ", " "))
            totalVCore_maps = str(find_between(my_counters, "Total vcore-seconds taken by all map tasks 0 0 ", " "))
             
            #http://clpd254.sldc.sbc.com:8088/cluster/app/application_1481291418014_0833
            r = requests.get('http://clpd254.sldc.sbc.com:8088/cluster/app/application' + jobid.replace("job",""))
            my_yarn_stats = dehtml(r.text)
            #print my_yarn_stats
            mb_sec = 0
            mb_sec = str(find_between(my_yarn_stats, "Aggregate Resource Allocation: ", " MB-seconds"))
            vcore_sec = 0
            vcore_sec = str(find_between(my_yarn_stats, "MB-seconds, "," vcore-seconds"))
            wfName = str(find_between(my_yarn_stats, "Name: ", " "))
            wfName = ''.join(wfName.split('.')[1:])
            attempt_table = str(find_between(my_yarn_stats, "attemptsTableData=[ [", "] ]"))
            attempts = attempt_table.count("cluster")
            attempt_url = str(find_between(attempt_table, "href='", "'"))
            r = requests.get('http://clpd254.sldc.sbc.com:8088' + attempt_url)
            my_attempt = dehtml(r.text)
            #print my_attempt
            my_containers = str(find_between(my_attempt, "Total Allocated Containers: ", " "))
            writer = csv.DictWriter(csvfile, lineterminator='\n',fieldnames=[ "Start_Time", "Duration","JobID", "Maps_Total","Maps_Completed","Reduces_Total","Reduces_Completed", "CPU_Map", "CPU_Reduce", "CPU_Total", "VCore_Map","VCore_Reduce","MB_Sec","VCore_Sec","Name", "Attempts", "Containers"])
            writer.writerow({'Start_Time': epoch_start,'Duration': duration, 'JobID': jobid ,'Maps_Total': maps_total, 'Maps_Completed': maps_completed, 'Reduces_Total': reducers_total,'Reduces_Completed': reducers_completed, 'CPU_Map': cpu_map, 'CPU_Reduce': cpu_reduce, 'CPU_Total': cpu_total,'VCore_Map': totalVCore_maps, 'VCore_Reduce': totalVCore_reduce,'MB_Sec':mb_sec,'VCore_Sec':vcore_sec,'Name':wfName, 'Attempts': attempts, 'Containers': my_containers})
            csvfile.close()
    print len(my_list)
    import os
    os.system("mail -s \"Job Results\" -a Job_Results.csv eg9661@att.com < Job_Results.csv")

if __name__ == '__main__':
    main(sys.argv[1:])
