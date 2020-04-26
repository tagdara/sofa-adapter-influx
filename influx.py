#!/usr/bin/python3

import sys, os
# Add relative paths for the directory where the adapter is located as well as the parent
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__),'../../base'))

from sofabase import sofabase
from sofabase import adapterbase
import devices

import json
import asyncio
import concurrent.futures
import datetime
import uuid
import influxdb

class influxServer(sofabase):

    class adapterProcess():
    
        def __init__(self, log=None, loop=None, dataset=None, notify=None, request=None, **kwargs):
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
            self.connectDatabase('beta')
                
        async def handleChangeReport(self, message):
            try:
                endpointId=message['event']['endpoint']['endpointId']
                for change in message['event']['payload']['change']['properties']:
                    
                    if 'exclude' in self.dataset.config:
                        if endpointId in self.dataset.config['exclude']:
                            return False
                    
                    if 'exclude_adapters' in self.dataset.config:
                        for ea in self.dataset.config['exclude_adapters']:
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
                        self.influxclient.write_points(line,database='beta')
                        if 'log_changes' in self.dataset.config and self.dataset.config['log_changes']==True:
                            self.log.info('<< Influx: %s' % line)
                
            except:
                self.log.warn('Problem with value data: %s of type %s' % (change['value'], type(change['value'])))
                self.log.error("Error handling change report for %s" % message,exc_info=True)            


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
                self.influxclient=influxdb.InfluxDBClient(self.dataset.config['dbserver'])
                dbs=self.influxclient.get_list_database()    
                if not self.databaseExists(dbname):
                    self.createDatabase(dbname)
                self.log.info('Databases: %s' % dbs)
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

        def writeInflux(self, adapter, item, data):
        
            try:
                if type(data)==list:
                    self.log.info('List data is not currently supported in the influx history database: %s' % (item))
                    return None
            
                if not self.databaseExists(adapter):
                    self.createDatabase(adapter)
                
                ifjson=[{'measurement':item,'fields': {'value':data}}]
                
                try:
                    while not self.dbConnected:
                        self.retryDatabaseConnection('beta')
                    self.influxclient.write_points(ifjson,database=adapter)
                except ConnectionRefusedError:
                    self.dbConnected=False
                    self.retryDatabaseConnection('beta')
                
                self.log.info('Sent %s to influx' % ifjson)

            except:
                self.log.error('Error inserting influx data with the following value: %s = %s (%s)' % (item, data, ifjson),exc_info=True)


        def virtualControllers(self, itempath):

            try:
                return {}

            except:
                self.log.error('Error getting virtual controller types for %s' % itempath, exc_info=True)

        async def virtualList(self, itempath, query={}):

            try:
                itempath=itempath.split('/')
                if itempath[0]=="powerState":
                    qry='select endpoint,powerState from controller_property'
                    if len(itempath)>1:
                        qry=qry+" where endpoint='%s'" % itempath[1]
                    self.log.info('Running query: %s' % qry)
                    result=self.influxclient.query(qry,database='beta')
                    return result.raw

                if itempath[0]=="last":
                    self.log.info('getting last info for %s - query: %s' % (itempath, query))
                    if query:
                        elist=json.loads(query)
                        rgx="~ /%s/" % "|".join(elist)
                        qry="select endpoint,last(%s) from controller_property where endpoint=%s group by endpoint" % (itempath[1], rgx)
                    else:
                        self.log.info('getting last info for %s' % itempath)
                        if len(itempath)>2:
                            qry="select endpoint,last(%s) from controller_property where endpoint='%s'" % (itempath[2], itempath[1])
                            if len(itempath)>3:
                                qry=qry+" AND %s='%s'" % (itempath[2], itempath[3])
                        else:
                            qry="select endpoint,last(%s) from controller_property" % itempath[1]

                    self.log.info('Running query: %s' % qry)
                    result=self.influxclient.query(qry,database='beta')

                    if query:
                        response={}
                        responselist=list(result.get_points())
                        for dp in responselist:
                            response[dp['endpoint']]=dp
                            
                    else:
                        response=list(result.get_points())[0]
                    #return result.raw
                    return response

                if itempath[0]=="history":
                    if len(itempath)>3:
                        offset=int(itempath[3])*50
                    else:
                        offset=0

                        qry="select endpoint,%s from controller_property where endpoint='%s' ORDER BY time DESC LIMIT 50 OFFSET %s" % (itempath[2],itempath[1],offset)
                    self.log.info('Running history query: %s' % qry)
                    result=self.influxclient.query(qry,database='beta')
                    response=list(result.get_points())
                    #return result.raw
                    return response


                if itempath[0]=="query":
                    self.log.info('influx query: %s' % query)
                    qry=query
                    result=self.influxclient.query(qry,database='beta')
                    response=list(result.get_points())
                    #return result.raw
                    return response
                    #return result.raw

                if itempath[0]=="querylist":
                    self.log.info('influx query: %s' % query)
                    
                    qry=json.loads(query)['query']
                    result=self.influxclient.query(qry,epoch='s',database='beta')
                    response=list(result.get_points())
                    #return result.raw
                    return response
                    
                return {}

            except:
                self.log.error('Error getting virtual controller types for %s' % itempath, exc_info=True)



        def virtualControllerProperty(self, nativeObj, controllerProp):
        
            # Scenes have no properties
            return None

        

if __name__ == '__main__':
    adapter=influxServer(name='influx')
    adapter.start()