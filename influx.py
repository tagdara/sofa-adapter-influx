#!/usr/bin/python3

import sys, os
# Add relative paths for the directory where the adapter is located as well as the parent
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__),'../../base'))

from sofabase import sofabase, adapterbase, configbase
import devices
from sofacollector import SofaCollector

import json
import asyncio
import concurrent.futures
import datetime
import uuid
#import influxdb import InfluxDBClient
import requests
from struct import pack, unpack

from aioinflux import InfluxDBClient, iterpoints

class influxServer(sofabase):

    class adapter_config(configbase):
    
        def adapter_fields(self):
            self.exclude=self.set_or_default('exclude', default=[])
            self.exclude_adapters=self.set_or_default('exclude_adapters', default=[])
            self.db_server=self.set_or_default('db_server', mandatory=True)

    class adapterProcess(SofaCollector.collectorAdapter):

        @property
        def collector_categories(self):
            return ['ALL']    
    
        def __init__(self, log=None, loop=None, dataset=None, notify=None, request=None, config=None, **kwargs):
            self.config=config
            self.dataset=dataset
            #self.definitions=definitions.Definitions
            self.log=log
            self.notify=notify
            self.dbConnected=False
            self.dbRetries=0

            if not loop:
                self.loop = asyncio.new_event_loop()
            else:
                self.loop=loop
                

        def jsonDateHandler(self, obj):

            if hasattr(obj, 'isoformat'):
                return obj.isoformat()
            else:
                self.log.error('Found unknown object for json dump: (%s) %s' % (type(obj),obj))
            return None
            
            
        async def start(self):
            self.polltime=1
            self.dblistcache=[]
            self.log.info('.. Starting Influx Manager')
            try:
                self.connectDatabase('beta')
            except:
                self.log.error('!! Problem starting influx client', exc_info=True)
                
        async def handleChangeReport(self, message):
            try:
                endpointId=message['event']['endpoint']['endpointId']
                for change in message['event']['payload']['change']['properties']:
                    
                    if endpointId in self.config.exclude:
                        return False
                    
                    for ea in self.config.exclude_adapters:
                        if endpointId.startswith(ea):
                            return False

                    if type(change['value'])==dict:
                        if 'value' in change['value']:
                            change['value']=change['value']['value']
                        else:
                            change['value']=json.dumps(change['value'])
                                
                    if type(change['value'])==list:
                        change['value']=str(change['value'])

                    if change['value']:
                        line=[{  "measurement":"controller_property", 
                            "tags": {"endpoint":endpointId, "namespace":change['namespace'].split('.')[0], "controller": change['namespace'].split('.')[1] },
                            "time": change["timeOfSample"],
                            "fields": { change["name"] : change["value"]}
                        }]
                        asyncio.create_task(self.database_write_data(line))

            except requests.exceptions.ConnectionError:
                self.log.error("!! InfluxDB server connection error - did not save change report for %s" % message) 
            except:
                self.log.warn('!! Problem with value data: %s of type %s' % (change['value'], type(change['value'])))
                self.log.error("!! Error handling change report for %s" % message,exc_info=True)            

        async def database_write_data(self, data, db="beta"):
            
            try:
                if self.config.log_changes:
                    self.log.info('<< Influx: %s' % data)
                for point in data:
                    async with InfluxDBClient(db=db, host=self.config.db_server) as client:
                        #await client.create_database(db='testdb')
                        await client.create_database(db=db)
                        await client.write(point)

                #self.influxclient.write_points(data, database='beta')
            except:
                self.log.error('!! Error writing to database: %s' % data, exc_info=True)

        async def database_query(self, query, db="beta"):
            
            try:
                if self.config.log_changes:
                    self.log.info('<< Influx: %s' % query)
                async with InfluxDBClient(db=db, host=self.config.db_server) as client:
                    #await client.create_database(db='testdb')
                    await client.create_database(db=db)
                    resp = await client.query(query, epoch='ms')
                    return resp

                #self.influxclient.write_points(data, database='beta')
            except:
                self.log.error('!! Error writing to database: %s' % data, exc_info=True)

                    

        def retryDatabase(self, dbname):
            
            try:
                self.log.info('Retrying database connection')
                time.sleep(5*dbRetries)
                self.connectDatabase(dbname)
            except:
                self.log.error('Error Retrying Database Connection', exc_info=True)
                self.dbConnected=False

        def connectDatabase(self, dbname):
            
            try:
                self.dbConnected=True
                self.dbRetries=0
            except:
                self.log.error('Error starting Influx', exc_info=True)
                self.dbConnected=False
                  
        def createDatabase(self, dbname):
        
            try:
                self.influxclient.create_database(dbname)
                self.dblistcache.append(dbname)
            except:
                self.log.info("Could not create Database "+dbname,exc_info=True)


        def databaseExists(self, dbname):
        
            if dbname in self.dblistcache:
                return True
        
            try:
                dblist=self.influxclient.get_list_database()
                for db in dblist:
                    if db['name']==dbname:
                        self.dblistcache.append(dbname)
                        return True
                return False
            except:
                self.log.info("Could not look for Database "+dbname,exc_info=True)

        async def convert_points_to_list(self, result):
            try:
                self.log.info('Starting data: %s' % result)
                response=[]
                if 'results' in result and 'series' in result['results'][0]:
                    series_data=result['results'][0]['series']
                    pointlist=[]
                    for item in series_data:
                        for point in item['values']:
                            pointdata={}
                            for x,field in enumerate(point):
                                pointdata[item['columns'][x]]=field
                            pointlist.append(pointdata)
                            #response[pointdata['endpoint']]=pointdata
            except:
                self.log.error('!! error converting points', exc_info=True)
            return pointlist
        
            
        async def virtualList(self, itempath, query={}):

            try:
                self.log.info('list request: %s %s' % (itempath, query))
                itempath=itempath.split('/')
                if itempath[0]=="powerState":
                    qry='select endpoint,powerState from controller_property'
                    if len(itempath)>1:
                        qry=qry+" where endpoint='%s'" % itempath[1]
                    self.log.info('.. query: %s' % qry)
                    result=await self.database_query(qry)
                    #result=self.influxclient.query(qry,database='beta')
                    return result.raw

                if itempath[0]=="last":
                    self.log.info('.. getting last info for %s - query: %s' % (itempath, query))
                    if query:
                        elist=json.loads(query)
                        rgx="~ /%s/" % "|".join(elist)
                        qry="select endpoint,last(%s) from controller_property where endpoint=%s group by endpoint" % (itempath[1], rgx)
                    else:
                        if len(itempath)>2:
                            qry="select endpoint,last(%s) from controller_property where endpoint='%s'" % (itempath[2], itempath[1])
                            if len(itempath)>3:
                                qry=qry+" AND %s='%s'" % (itempath[2], itempath[3])
                        else:
                            qry="select endpoint,last(%s) from controller_property" % itempath[1]

                    self.log.debug('.. running query: %s' % qry)
                    #result=self.influxclient.query(qry,database='beta')
                    result=await self.database_query(qry)

                    if query:
                        response={}
                        if 'results' in result and 'series' in result['results'][0]:
                            series_data=result['results'][0]['series']
                            pointlist=[]
                            for item in series_data:
                                for point in item['values']:
                                    pointdata={}
                                    for x,field in enumerate(point):
                                        pointdata[item['columns'][x]]=field
                                    pointlist.append(pointdata)
                                    response[pointdata['endpoint']]=pointdata
                    else:
                        response=result
                        if 'results' in result and 'series' in result['results'][0]:
                            last_data=result['results'][0]['series'][0]
                            pointlist=[]
                            for point in last_data['values']:
                                pointdata={}
                                for x,field in enumerate(point):
                                    pointdata[last_data['columns'][x]]=field
                                pointlist.append(pointdata)
                            response=pointlist[0]
                                
                        self.log.info('response: %s ' % response)
                    return response

                if itempath[0]=="history":
                    if len(itempath)>3:
                        offset=int(itempath[3])*50
                    else:
                        offset=0

                        qry="select endpoint,%s from controller_property where endpoint='%s' ORDER BY time DESC LIMIT 50 OFFSET %s" % (itempath[2],itempath[1],offset)
                    self.log.info('Running history query: %s' % qry)
                    #result=self.influxclient.query(qry,database='beta')
                    result=await self.database_query(qry)
                    response=await self.convert_points_to_list(result)
                    self.log.info('response: %s' % response)
                    #response=list(result.get_points())
                    #return result.raw
                    return response


                if itempath[0]=="query":
                    self.log.info('influx query: %s' % query)
                    qry=query
                    #result=self.influxclient.query(qry,database='beta')
                    result=await self.database_query(qry)
                    response=list(result.get_points())
                    #return result.raw
                    return response
                    #return result.raw

                if itempath[0]=="querylist":
                    self.log.info('influx query: %s' % query)
                    
                    qry=json.loads(query)['query']
                    #result=await self.influxclient.query(qry,epoch='s',database='beta')
                    result=await self.database_query(qry)
                    response=list(result.get_points())
                    #return result.raw
                    return response
                    
                return {}

            except:
                self.log.error('Error getting virtual controller types for %s' % itempath, exc_info=True)



if __name__ == '__main__':
    adapter=influxServer(name='influx')
    adapter.start()