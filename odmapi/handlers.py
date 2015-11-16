# -*- coding: utf-8 -*-
# Copyright 2009-2010 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import division
from bson.son import SON
from pymongo import Connection, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, ConfigurationError, OperationFailure, AutoReconnect,PyMongoError
from bson import json_util

import re
import inspect
import sys
import difflib
import bisect
import numpy
from datetime import datetime,timedelta,date,time
from dateutil.relativedelta import relativedelta
from collections import defaultdict,OrderedDict
import time
import operator
# import pytz
# from operator import itemgetter
from scipy.stats.stats import pearsonr

#lists of predefined stuff, e.g. non-proprietary formats, machine readable
import def_formatLists
from string_diff import comp as odm_comp

#all_functions = inspect.getmembers(module, inspect.isfunction)
#print all_functions

try:
    import json
except ImportError:
    import simplejson as json

class MongoHandler:
    mh = None
    _cursor_id = 0
    db_name = 'odm'
    raw_collection_name = 'odm'
    collection_name = 'odm_harmonised'
    # collection_name = 'odm_harmonised_demo'
    jobs_collection_name = 'jobs'
    # jobs_collection_name = 'jobs_demo'

    batchSize=10000

    def __init__(self, mongos):
        self.db_name = MongoHandler.db_name
        self.collection_name = MongoHandler.collection_name
        self.job_collection_name = MongoHandler.jobs_collection_name

        self.connections = {}
        self.countryStats={}

        for host in mongos:
            args = MongoFakeFieldStorage({"server" : host})

            out = MongoFakeStream()
            if len(mongos) == 1:
                name = "default"
            else:
                name = host.replace(".", "")
                name = name.replace(":", "")

            self._connect(args, out.ostream, name = name)

        self.__collect_country_stats('default')


    def _set_collection(self,name):
        # print('from handler...yolo')
        self.collection_name = name


    def _reset_collection(self,name):
        self.collection_name = MongoHandler.collection_name


    def __date_handler(self, obj):
        return obj.isoformat() if hasattr(obj, 'isoformat') else obj


    def __collect_country_stats(self,name):
        conn = self._get_connection(name)
        if conn == None:
            out('{"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}')
            return

        results=conn[self.db_name]['countries'].find(spec={'odm':{'$exists':True}},
                fields={'ISO':1,'Country':1,'Population':1,'odm':1})

        for result in results:
            stats={}
            if 'odm' in result.keys():
                if 'population' in result['odm'].keys():
                    stats['population']={}
                    for date in result['odm']['population']:
                        stats['population'][date]=result['odm']['population'][date]
                if 'GDP' in result['odm'].keys():
                    stats['GDP']={}
                    for date in result['odm']['GDP']:
                        try:
                            stats['GDP'][date]=result['odm']['GDP'][date]
                        except ValueError as e:
                            stats['GDP'][date]=-1
                if 'HDI' in result['odm'].keys():
                    stats['HDI']={}
                    for date in result['odm']['HDI']:
                        pass
                        try:
                            stats['HDI'][date]=result['odm']['HDI'][date]
                        except ValueError as e:
                            stats['HDI'][date]=-1
                self.countryStats[result['ISO']]=stats


    def _get_connection(self, name = None, uri='mongodb://localhost:27017'):
        if name == None:
            name = "default"

        if name in self.connections:
            return self.connections[name]

        try:
            connection = Connection(uri, sockettimeoutms = 60000)
        except (ConnectionFailure, ConfigurationError):
            return None

        self.connections[name] = connection
        return connection

    def _get_host_and_port(self, server):
        host = "localhost"
        port = 27017

        if len(server) == 0:
            return (host, port)

        m = re.search('([^:]+):([0-9]+)?', server)
        if m == None:
            return (host, port)

        handp = m.groups()

        if len(handp) >= 1:
            host = handp[0]
        if len(handp) == 2 and handp[1] != None:
            port = int(handp[1])

        return (host, port)

    def sm_object_hook(obj):
        if "$pyhint" in obj:
            temp = SON()
            for pair in obj['$pyhint']:
                temp[pair['key']] = pair['value']
            return temp
        else:
            return json_util.object_hook(obj)


    def _get_son(self, str, out):
        try:
            obj = json.loads(str, object_hook=json_util.object_hook)
        except (ValueError, TypeError):
            out('{"ok" : 0, "errmsg" : "couldn\'t parse json: %s"}' % str)
            return None

        if getattr(obj, '__iter__', False) == False:
            out('{"ok" : 0, "errmsg" : "type is not iterable: %s"}' % str)
            return None

        return obj


    def _cmd(self, args, out, name = None, db = None, collection = None):
        if name == None:
            name = "default"

        conn = self._get_connection(name)
        if conn == None:
            out('{"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}')
            return

        cmd = self._get_son(args.getvalue('cmd'), out)
        if cmd == None:
            return

        try:
            result = conn[db].command(cmd, check=False)
        except AutoReconnect:
            out('{"ok" : 0, "errmsg" : "wasn\'t connected to the db and '+
                'couldn\'t reconnect", "name" : "%s"}' % name)
            return
        except (OperationFailure, error):
            out('{"ok" : 0, "errmsg" : "%s"}' % error)
            return

        # debugging
        if result['ok'] == 0:
            result['cmd'] = args.getvalue('cmd')

        out(json.dumps(result, default=json_util.default))

    def _hello(self, args, out, name = None, version = None, db = None, collection = None):
        out('{"ok" : 1, "msg" : "Uh, we had a slight weapons malfunction, but ' +
            'uh... everything\'s perfectly all right now. We\'re fine. We\'re ' +
            'all fine here now, thank you. How are you?"}')
        return

    def _status(self, args, out, name = None, version = None, db = None, collection = None):
        result = {"ok" : 1, "connections" : {}}

        for name, conn in self.connections.iteritems():
            result['connections'][name] = "%s:%d" % (conn.host, conn.port)

        out(json.dumps(result))

    def _connect(self, args, out, name = None, version = None, db = None, collection = None):
        """
        connect to a mongod
        """

        if type(args).__name__ == 'dict':
            out('{"ok" : 0, "errmsg" : "_connect must be a POST request"}')
            return

        if "server" in args:
            try:
                uri = args.getvalue('server')
            except Exception, e:
                print uri
                print e
                out('{"ok" : 0, "errmsg" : "invalid server uri given", "server" : "%s"}' % uri)
                return
        else:
            uri = 'mongodb://localhost:27017'

        if name == None:
            name = "default"

        conn = self._get_connection(name, uri)
        if conn != None:
            out('{"ok" : 1, "server" : "%s", "name" : "%s"}' % (uri, name))
        else:
            out('{"ok" : 0, "errmsg" : "could not connect", "server" : "%s", "name" : "%s"}' % (uri, name))

    def _authenticate(self, args, out, name = None, db = None, collection = None):
        """
        authenticate to the database.
        """

        if type(args).__name__ == 'dict':
            out('{"ok" : 0, "errmsg" : "_find must be a POST request"}')
            return

        conn = self._get_connection(name)
        if conn == None:
            out('{"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}')
            return

        if db == None:
            out('{"ok" : 0, "errmsg" : "db must be defined"}')
            return

        if not 'username' in args:
            out('{"ok" : 0, "errmsg" : "username must be defined"}')

        if not 'password' in args:
            out('{"ok" : 0, "errmsg" : "password must be defined"}')

        if not conn[db].authenticate(args.getvalue('username'), args.getvalue('password')):
            out('{"ok" : 0, "errmsg" : "authentication failed"}')
        else:
            out('{"ok" : 1}')

    def _find(self, args, out, name = None, version = None, db = None, collection = None):
        """
        query the database.
        """
        db = self.db_name
        collection = self.collection_name

        if type(args).__name__ != 'dict':
            out('{"ok" : 0, "errmsg" : "_find must be a GET request"}')
            return

        conn = self._get_connection(name)
        if conn == None:
            out('{"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}')
            return

        if db == None or collection == None:
           out('{"ok" : 0, "errmsg" : "db and collection must be defined"}')
           return

        criteria = {}
        if 'criteria' in args:
            criteria = self._get_son(args['criteria'][0], out)
            if criteria == None:
                return

        if 'odm_id' in args:
            criteria = {'id': args['odm_id'][0]}

        # fields = None
        fields = {
                '_id':False,
                'id':True,'title':True,'notes':True,'num_tags':True,
                'tags':True,'organization.id':True,'organization.title':True,
                'author':True,'author_email':True,'license_id':True,'catalogue_url':True,
                'metadata_created':True,'metadata_modified':True,'num_resources':True,
                'extras.language':True,
                'country':True,'extras.state':True,'city':True,
                'extras.date_released':True,'extras.date_updated':True,'extras.update_frequency':True,
                'category':True,'sub_category':True,
                'platform':True,'extras.version':True,
                'resources.mimetype':True,'resources.hash':True,'resources.url':True,
                'resources.size':True, 'resources.format':True,
                'owner_org':True,
                'ckan_url':True,'url':True,
                'isopen':True,'private':True,
                'is_duplicate':True,
                }
        # if 'fields' in args:
        #     fields = self._get_son(args['fields'][0], out)
        #     if fields == None:
        #         return

        limit = 0
        # if 'limit' in args:
        #     limit = int(args['limit'][0])

        batch_size = 15
        if 'batch_size' in args:
            batch_size = int(args['batch_size'][0])

        skip = 0
        if 'offset' in args:
            skip = int(args['offset'][0])
            limit = batch_size
        elif 'page' in args:
            skip = batch_size * (int(args['page'][0])-1)
            limit = batch_size

        cat_url=None
        if 'catalogue_url' in args:
            # criteria = {'catalogue_url':args['catalogue_url'][0]}
            criteria['catalogue_url']=args['catalogue_url'][0]

        if 'attribute' in args:
            if args['attribute'][0] in ['date_released','date_updated','metadata_created','metadata_modified']:
                # end_date=datetime.now()
                end_date=''
                start_date=''
                if "end_date" in args:
                    try:
                        end_date=datetime.strptime(args['end_date'][0], '%Y-%m-%d')
                    except ValueError as e:
                        print (e)

                # start_date=end_date + relativedelta(years=-1)
                if "start_date" in args:
                    try:
                        start_date=datetime.strptime(args['start_date'][0], '%Y-%m-%d')
                    except ValueError as e:
                        print (e)
                if start_date or end_date:
                    if 'metadata' in args['attribute'][0]:
                        extras_date=args['attribute'][0]
                    else:
                        extras_date='extras.'+args['attribute'][0]
                    params=[]
                    params.append({extras_date:{'$type':9}})
                    if start_date and end_date:
                        params.append({extras_date:{'$gte':start_date,'$lte':end_date}})
                    elif  start_date:
                        params.append({extras_date:{'$gte':start_date}})
                    else:
                        params.append({extras_date:{'$lte':end_date}})
                    criteria['$and']=params

        cursor = conn[db][collection].find(spec=criteria, fields=fields, limit=limit, skip=skip)

        sort = None
        if 'sort' in args:
            sort = self._get_son(args['sort'][0], out)
            if sort == None:
                return

            stupid_sort = []

            for field in sort:
                if sort[field] == -1:
                    stupid_sort.append([field, DESCENDING])
                else:
                    stupid_sort.append([field, ASCENDING])

            cursor.sort(stupid_sort)

        if 'explain' in args and bool(args['explain'][0]):
            out(json.dumps({"results" : [cursor.explain()], "ok" : 1}, default=json_util.default))


        if not hasattr(self, "cursors"):
            setattr(self, "cursors", {})

        id = MongoHandler._cursor_id
        MongoHandler._cursor_id = MongoHandler._cursor_id + 1

        cursors = getattr(self, "cursors")
        cursors[id] = cursor
        setattr(cursor, "id", id)


        if 'count' in args and args['count'][0].lower() in ['1','true']:
            self.__output_results(cursor, out, batch_size, conn[db][collection].find(spec=criteria, fields=fields, limit=limit, skip=skip).count())
        else:
            self.__output_results(cursor, out, batch_size)


    def _find_raw(self, args, out, name = None, version = None, db = None, collection = None):
        """
        query the database.
        """
        db = self.db_name
        collection = self.raw_collection_name

        if type(args).__name__ != 'dict':
            out('{"ok" : 0, "errmsg" : "_find must be a GET request"}')
            return

        conn = self._get_connection(name)
        if conn == None:
            out('{"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}')
            return

        if db == None or collection == None:
           out('{"ok" : 0, "errmsg" : "db and collection must be defined"}')
           return

        criteria = { 'deleted_dataset': { '$ne': True }}
        # if 'criteria' in args:
        #     criteria = self._get_son(args['criteria'][0], out)
        #     if criteria == None:
        #         return

        if 'odm_id' in args:
            criteria = {'id': args['odm_id'][0]}

        fields = {
                '_id':0,
                'deleted_dataset':0,'attrs_counter':0,'copied':0,'harmonised':0,
                # 'id':True,'title':True,'notes':True,'num_tags':True,
                # 'tags':True,'organization.id':True,'organization.title':True,
                # 'author':True,'author_email':True,'license_id':True,'catalogue_url':True,
                # 'metadata_created':True,'metadata_modified':True,'num_resources':True,
                # 'extras.language':True,
                # 'country':True,'extras.state':True,'city':True,
                # 'extras.date_released':True,'extras.date_updated':True,'extras.update_frequency':True,
                # 'category':True,'sub_category':True,
                # 'platform':True,'extras.version':True,
                # 'resources.mimetype':True,'resources.hash':True,'resources.url':True,
                # 'resources.size':True, 'resources.format':True,
                # 'owner_org':True,
                # 'ckan_url':True,'url':True,
                # 'isopen':True,'private':True,
                # 'is_duplicate':True,
                }

        limit = 0
        # if 'limit' in args:
        #     limit = int(args['limit'][0])

        batch_size = 15
        if 'batch_size' in args:
            batch_size = int(args['batch_size'][0])

        skip = 0
        if 'offset' in args:
            skip = int(args['offset'][0])
            limit = batch_size
        elif 'page' in args:
            skip = batch_size * (int(args['page'][0])-1)
            limit = batch_size

        cat_url=None
        if 'catalogue_url' in args:
            criteria['catalogue_url']=args['catalogue_url'][0]

        # if 'attribute' in args:
        #     if args['attribute'][0] in ['date_released','date_updated','metadata_created','metadata_updated']:
        #         # end_date=datetime.now()
        #         end_date=''
        #         start_date=''
        #         if "end_date" in args:
        #             try:
        #                 end_date=datetime.strptime(args['end_date'][0], '%Y-%m-%d')
        #             except ValueError as e:
        #                 print (e)
        #
        #         # start_date=end_date + relativedelta(years=-1)
        #         if "start_date" in args:
        #             try:
        #                 start_date=datetime.strptime(args['start_date'][0], '%Y-%m-%d')
        #             except ValueError as e:
        #                 print (e)
        #         if start_date or end_date:
        #             if 'metadata' in args['attribute'][0]:
        #                 extras_date=args['attribute'][0]
        #             else:
        #                 extras_date='extras.'+args['attribute'][0]
        #             params=[]
        #             params.append({extras_date:{'$type':9}})
        #             if start_date and end_date:
        #                 params.append({extras_date:{'$gte':start_date,'$lte':end_date}})
        #             elif  start_date:
        #                 params.append({extras_date:{'$gte':start_date}})
        #             else:
        #                 params.append({extras_date:{'$lte':end_date}})
        #         criteria['$and']=params

        cursor = conn[db][collection].find(spec=criteria, fields=fields, limit=limit, skip=skip)

        sort = None
        if 'sort' in args:
            sort = self._get_son(args['sort'][0], out)
            if sort == None:
                return

            stupid_sort = []

            for field in sort:
                if sort[field] == -1:
                    stupid_sort.append([field, DESCENDING])
                else:
                    stupid_sort.append([field, ASCENDING])

            cursor.sort(stupid_sort)

        if 'explain' in args and bool(args['explain'][0]):
            out(json.dumps({"results" : [cursor.explain()], "ok" : 1}, default=json_util.default))


        if not hasattr(self, "cursors"):
            setattr(self, "cursors", {})

        id = MongoHandler._cursor_id
        MongoHandler._cursor_id = MongoHandler._cursor_id + 1

        cursors = getattr(self, "cursors")
        cursors[id] = cursor
        setattr(cursor, "id", id)


        if 'count' in args and args['count'][0].lower() in ['1','true']:
            self.__output_raw_results(cursor, out, batch_size, conn[db][collection].find(spec=criteria, fields=fields, limit=limit, skip=skip).count())
        else:
            self.__output_raw_results(cursor, out, batch_size)


    def _more(self, args, out, name = None, version= None, db = None, collection = None):
        """
        Get more results from a cursor
        """

        if type(args).__name__ != 'dict':
            out('{"ok" : 0, "errmsg" : "_more must be a GET request"}')
            return

        if 'id' not in args:
            out('{"ok" : 0, "errmsg" : "no cursor id given"}')
            return


        id = int(args["id"][0])
        cursors = getattr(self, "cursors")

        if id not in cursors:
            out('{"ok" : 0, "errmsg" : "couldn\'t find the cursor with id %d"}' % id)
            return

        cursor = cursors[id]

        batch_size = 15
        if 'batch_size' in args:
            batch_size = int(args['batch_size'][0])

        self.__output_results(cursor, out, batch_size)


    def __output_results(self, cursor, out, batch_size=15, count=0):
        """
        Iterate through the next batch
        """
        batch = []

        try:
            while len(batch) < batch_size:
                batch.append(cursor.next())
        except AutoReconnect:
            out(json.dumps({"ok" : 0, "errmsg" : "auto reconnecting, please try again"}))
            return
        except OperationFailure, of:
            out(json.dumps({"ok" : 0, "errmsg" : "%s" % of}))
            return
        except StopIteration:
            # this is so stupid, there's no has_next?
            pass


        list_batch=[]
        for i in batch:
            if 'author' in i.keys() and i['author']==None:
                i['author']=''
            if 'author_email' in i.keys() and i['author_email']==None:
                i['author_email']=''
            if 'platform' in i.keys() and i['platform']==None:
                i['platform']=''
            if 'country' in i.keys() and i['country']==None:
                i['country']=''
            if 'state' in i.keys() and i['state']==None:
                i['state']=''
            if 'city' in i.keys() and i['city']==None:
                i['city']=''
            if 'extras' in i.keys():
                if 'city' in i['extras'].keys():
                    i['city']=i['extras.city']
                    del i['extras']['city']
                if 'date_updated' in i['extras'].keys():
                    try:
                        # i['date_updated']=datetime.strptime(i['extras']['date_updated'],'%Y-%m-%d %H:%M:%S')
                        i['date_updated']=i['extras']['date_updated']
                    except ValueError:
                        print (i['extras']['date_updated'])
                    del i['extras']['date_updated']
                # if 'country' in i['extras'].keys():
                #     i['country']=i['extras']['country']
                #     del i['extras']['country']
                # if 'state' in i['extras'].keys():
                #     i['state']=i['extras']['state']
                #     del i['extras']['state']
                if 'date_released' in i['extras'].keys():
                    i['date_released']=i['extras']['date_released']
                    del i['extras']['date_released']
                if 'update_frequency' in i['extras'].keys():
                    i['update_frequency']=i['extras']['update_frequency']
                    del i['extras']['update_frequency']
                # if 'platform' in i['extras'].keys():
                #     i['platform']=i['extras']['platform']
                    # del i['extras']['platform']
                # if 'version' in i['extras'].keys():
                #     i['version']=i['extras']['version']
                #     del i['extras']['version']
                if 'language' in i['extras'].keys():
                    i['language']=i['extras']['language']
                    del i['extras']['language']
                del i['extras']

            sorted_batch=OrderedDict()
            sorted_batch['odm_id']=i['id'] if 'id' in i.keys() else ''
            sorted_batch['title']=i['title'] if 'title' in i.keys() else ''
            sorted_batch['notes']=i['notes'] if 'notes' in i.keys() else ''
            # sorted_batch['owner_org']=i['owner_org'] if 'owner_org' in i.keys() else ''
            sorted_batch['author']=i['author'] if 'author' in i.keys() else ''
            sorted_batch['author_email']=i['author_email'] if 'author_email' in i.keys() else ''
            sorted_batch['license_id']=i['license_id'] if 'license_id' in i.keys() else ''
            sorted_batch['dataset_url']=i['ckan_url'] if 'ckan_url' in i.keys() and i['ckan_url']!=None else (i['url'] if 'url' in i else '')
            sorted_batch['catalogue_url']=i['catalogue_url'] if 'catalogue_url' in i.keys() else ''
            sorted_batch['metadata_created']=i['metadata_created'] if 'metadata_created' in i.keys() else ''
            sorted_batch['metadata_modified']=i['metadata_modified'] if 'metadata_modified' in i.keys() else ''
            sorted_batch['num_tags']=i['num_tags'] if 'num_tags' in i.keys() else 0
            sorted_batch['tags']=i['tags'] if 'tags' in i.keys() else []
            sorted_batch['num_resources']=i['num_resources'] if 'num_resources' in i.keys() else 0
            sorted_batch['resources']=i['resources'] if 'resources' in i.keys() else []
            sorted_batch['date_released']=i['date_released'] if 'date_released' in i.keys() else ''
            sorted_batch['date_updated']=i['date_updated'] if 'date_updated' in i.keys() else ''
            sorted_batch['update_frequency']=i['update_frequency'] if 'update_frequency' in i.keys() else ''
            if 'organization' in i.keys():
                organization_batch=OrderedDict()
                organization_batch['id']=i['organization']['id'] if 'id' in i['organization'].keys() else ''
                organization_batch['title']=i['organization']['title'] if 'title' in i['organization'].keys() else ''
                sorted_batch['organization']=organization_batch
            else:
                sorted_batch['organization']={'id':'','title':''}
            sorted_batch['country']=i['country'] if 'country' in i.keys() else ''
            sorted_batch['state']=i['state'] if 'state' in i.keys() else ''
            sorted_batch['city']=i['city'] if 'city' in i.keys() else ''
            sorted_batch['category']=i['category'] if 'category' in i.keys() else []
            sorted_batch['sub_category']=i['sub_category'] if 'sub_category' in i.keys() else []
            sorted_batch['platform']=i['platform'] if 'platform' in i.keys() else ''
            sorted_batch['language']=i['language'] if 'language' in i.keys() else ''
            sorted_batch['isopen']=i['isopen'] if 'isopen' in i.keys() else True
            sorted_batch['is_duplicate']=i['is_duplicate'] if 'is_duplicate' in i.keys() else False
            # sorted_batch['version']=i['version'] if 'version' in i.keys() else ''

            list_batch.append(sorted_batch)

        # out(json.dumps({"results" : list_batch, "id" : cursor.id, "ok" : 1}, default=json_util.default))
        if count:
            out(json.dumps({"count": count, "results" : list_batch, "id" : cursor.id, "ok" : 1}, default=self.__date_handler))
        else:
            out(json.dumps({"results" : list_batch, "id" : cursor.id, "ok" : 1}, default=self.__date_handler))


    def __output_raw_results(self, cursor, out, batch_size=15, count=0):
        """
        Iterate through the next batch
        """
        batch = []

        try:
            while len(batch) < batch_size:
                batch.append(cursor.next())
        except AutoReconnect:
            out(json.dumps({"ok" : 0, "errmsg" : "auto reconnecting, please try again"}))
            return
        except OperationFailure, of:
            out(json.dumps({"ok" : 0, "errmsg" : "%s" % of}))
            return
        except StopIteration:
            # this is so stupid, there's no has_next?
            pass

        if count:
            out(json.dumps({"count": count, "results" : batch, "id" : cursor.id, "ok" : 1}, default=self.__date_handler))
        else:
            out(json.dumps({"results" : batch, "id" : cursor.id, "ok" : 1}, default=self.__date_handler))


    def _insert(self, args, out, name = None, db = None, collection = None):
        """
        insert a doc
        """

        if type(args).__name__ == 'dict':
            out('{"ok" : 0, "errmsg" : "_insert must be a POST request"}')
            return

        conn = self._get_connection(name)
    def _insert(self, args, out, name = None, db = None, collection = None):
        """
        insert a doc
        """

        if type(args).__name__ == 'dict':
            out('{"ok" : 0, "errmsg" : "_insert must be a POST request"}')
            return

        conn = self._get_connection(name)
        if conn == None:
            out('{"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}')
            return

        if db == None or collection == None:
            out('{"ok" : 0, "errmsg" : "db and collection must be defined"}')
            return

        if "docs" not in args:
            out('{"ok" : 0, "errmsg" : "missing docs"}')
            return

        docs = self._get_son(args.getvalue('docs'), out)
        if docs == None:
            return

        safe = False
        if "safe" in args:
            safe = bool(args.getvalue("safe"))

        result = {}
        result['oids'] = conn[db][collection].insert(docs)
        if safe:
            result['status'] = conn[db].last_status()

        out(json.dumps(result, default=json_util.default))


    def __safety_check(self, args, out, db):
        safe = False
        if "safe" in args:
            safe = bool(args.getvalue("safe"))

        if safe:
            result = db.last_status()
            out(json.dumps(result, default=json_util.default))
        else:
            out('{"ok" : 1}')


    def _update(self, args, out, name = None, db = None, collection = None):
        """
        update a doc
        """

        if type(args).__name__ == 'dict':
            out('{"ok" : 0, "errmsg" : "_update must be a POST request"}')
            return

        conn = self._get_connection(name)
        if conn == None:
            out('{"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}')
            return

        if db == None or collection == None:
            out('{"ok" : 0, "errmsg" : "db and collection must be defined"}')
            return

        if "criteria" not in args:
            out('{"ok" : 0, "errmsg" : "missing criteria"}')
            return
        criteria = self._get_son(args.getvalue('criteria'), out)
        if criteria == None:
            return

        if "newobj" not in args:
            out('{"ok" : 0, "errmsg" : "missing newobj"}')
            return
        newobj = self._get_son(args.getvalue('newobj'), out)
        if newobj == None:
            return

        upsert = False
        if "upsert" in args:
            upsert = bool(args.getvalue('upsert'))

        multi = False
        if "multi" in args:
            multi = bool(args.getvalue('multi'))

        conn[db][collection].update(criteria, newobj, upsert=upsert, multi=multi)

        self.__safety_check(args, out, conn[db])

    def _remove(self, args, out, name = None, db = None, collection = None):
        """
        remove docs
        """

        if type(args).__name__ == 'dict':
            out('{"ok" : 0, "errmsg" : "_remove must be a POST request"}')
            return

        conn = self._get_connection(name)
        if conn == None:
            out('{"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}')
            return

        if db == None or collection == None:
            out('{"ok" : 0, "errmsg" : "db and collection must be defined"}')
            return

        criteria = {}
        if "criteria" in args:
            criteria = self._get_son(args.getvalue('criteria'), out)
            if criteria == None:
                return

        result = conn[db][collection].remove(criteria)

        self.__safety_check(args, out, conn[db])

    def _batch(self, args, out, name = None, db = None, collection = None):
        """
        batch process commands
        """

        if type(args).__name__ == 'dict':
            out('{"ok" : 0, "errmsg" : "_batch must be a POST request"}')
            return

        requests = self._get_son(args.getvalue('requests'), out)
        if requests == None:
            return

        out("[")

        first = True
        for request in requests:
            if "cmd" not in request:
                continue

            cmd = request['cmd']
            method = "GET"
            if 'method' in request:
                method = request['method']

            db = None
            if 'db' in request:
                db = request['db']

            collection = None
            if 'collection' in request:
                collection = request['collection']

            args = {}
            name = None
            if 'args' in request:
                args = request['args']
                if 'name' in args:
                    name = args['name']

            if method == "POST":
                args = MongoFakeFieldStorage(args)

            func = getattr(MongoHandler.mh, cmd, None)
            if callable(func):
                output = MongoFakeStream()
                func(args, output.ostream, name = name, db = db, collection = collection)
                if not first:
                    out(",")
                first = False

                out(output.get_ostream())
            else:
                continue

        out("]")

    ##returns total number of catalogues
    def _catfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetTotalNumberOfCatalogues(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##returns total number of catalogues
    def _totaldistribsfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetTotalDistribsFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##returns frequency of catalogues using specific software platforms
    def _catsoftfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetFreqOfCatalogues(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##returns proportion of catalogues using specific software platforms
    def _catsoftprop(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetProportionOfCatalogues(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ## returns the number of catalogued datasets
    def _catdatasetsfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetNumberOfDatasets(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ## returns the number of catalogued datasets
    def _catdatasetstotfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetDatasetsTotFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ## returns the number of catalogued datasets
    def _cddatasetsfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetCDDatasetsFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ## returns the number of catalogued datasets
    def _catdatasets(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetDatasets(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ## returns the number of unique organisations publishing data (publishers)
    def _catpublishersfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetNumberOfUniquePublishers(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ## returns the number of unique organisations publishing data (publishers)
    def _cdpublishersfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetCDPublishersFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##returns number of distributions with an explicity set license
    def _catlicensedfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetNumberOfDatasetsWithLicense(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##returns proportion of distributions with an explicity set license
    def _catlicensedprop(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetProportionOfDatasetsWithLicense(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ## returns the number of datasets with Open License
    ## (list from http://opendefinition.org/licenses)
    def _catopenlicfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetNumberOfDatasetsWithOpenLicense(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    def _edopenlicfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetEDOpenLicenseFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    def _cdopenlicfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetCDOpenLicenseFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ## returns the proportion of datasets with Open License (list from http://opendefinition.org/licenses)
    def _catopenlicprop(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetProportionOfDatasetsWithOpenLicense(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ## returns the number of datasets with Open License
    ## (list from http://opendefinition.org/licenses)
    def _openlicdatefreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetOpenLicenseFreqPerDate(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##Get frequency of datasets by license type
    def _catdsbylicensefreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetFrequencyOfDatasetsByLicenseType(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))



    ##Get Proportion of datases by license type
    def _catdsbylicenseprop(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result = metric.GetProportionOfDatasetsByLicenseType(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##Get number of unique publishers contributing to the catalogue
    def _catuniqpublishersfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetNumOfUniquePublishersContributingToCatalogue(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##Get number of unique publishers relative to the catalogue
    def _catuniqpublishersprop(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetNumOfUniquePublishersRelativeToCatalogue(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    ##Get Frequency of datasets by file format
    def _catfileformatfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetFrequencyOfDatasetsByFileFormat(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    def _ednonproprformatfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetEDNonProprietaryFormatFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    def _catnonproprformatfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatNonProprietaryFormatFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##Get Proportion of datasets by file format
    def _catfileformatprop(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetProportionOfDatasetsByFileFormat(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##Get all catalogue urls
    def _catalogues(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatalogues(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##Median age of catalogues
    def _catmedageyears(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatMedianAge(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##Mean age of catalogues
    def _catmeanageyears(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatMeanAge(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##New catalogues per month
    def _catnewmonthfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatNewMonthFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##Frequency of catalogued distributions
    def _catdistribsfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatDistribsFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    ##Frequency of catalogued distributions
    def _catdistribstotfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatDistribsTotFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    ##Frequency of catalogued distributions
    def _cddistribsfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCDDistribsFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    ##Frequency of catalogued distributions
    def _catdistribs(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatDistribs(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    ##Total distribution size in a catalogue
    def _catdatasizetotal(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatDataSizeTotal(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##Total distribution size in a catalogue
    def _eddistribsize(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetEDDistribSize(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##Total distribution size in a catalogue
    def _cddistribsize(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCDDistribSize(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##Median distribution size
    def _catdatasizemed(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatMedianDatasize(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##Mean distribution size
    def _catdatasizemean(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatMeanDatasize(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##Maximum distribution size
    def _catdatasizemax(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatMaxDatasize(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##Standard deviation of distribution sizes
    def _catdatasizestddev(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatStddevDatasize(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    ##Frequency of distributions in a machine-readable file format
    def _catmachinereadformatfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatMachineFormatFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    def _edmachinereadformatfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetEDMachineReadFormatFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    def _cdmachinereadformatfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCDMachineReadFormatFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    ##Proportion of distributions in a machine-readable file format
    def _catmachinereadformatprop(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatMachineFormatProp(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    ##Frequency of distributions by MIME type of data file
    def _catmimetypefreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatMimeFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    ##Proportion of distributions by MIME type of data file
    def _catmimetypeprop(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatMimeProp(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    ##Catalogues per geographic region
    def _catgeofreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcatgeofreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    ##Catalogues per capita per country
    def _catcapitafreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcatcapitafreq(conn, args, self.countryStats, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    ##Catalogues & per-capita GDP correlation (Pearson and Spearman’s rank)
    def _catgdpcorr(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcatgdpcorr(conn, args, self.countryStats, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    ##Catalogues & HDI correlation (Pearson and Spearman’s rank)
    def _cathdicorr(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcathdicorr(conn, args, self.countryStats, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    ##Publishers
    def _publishers(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getpublishers(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    ##Categories
    def _categories(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcategories(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    def _dssize(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getdssize(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    def _dspopulatedmdfields(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getdspopulatedmdfields(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    def _catlangs(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcatlangs(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    def _catupdatefreqprop(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcatupdatefreqprop(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    def _catupdatefreqfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcatupdatefreqfreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    def _catlastupdatebyyearfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcatlastupdatebyyearfreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    def _catmedsincenewdays(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcatmedsincenewdays(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    def _catmedsinceupdatedays(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcatmedsinceupdatedays(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    def _catstatcodeprop(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcatstatcodeprop(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    def _catbrokenlinksprop(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcatbrokenlinksprop(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    def _catuniqprop(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcatuniqprop(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    def _catduplprop(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcatduplprop(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    def _catcountrynewmonthfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcatcountrynewmonthfreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    def _catsitepagerank(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.Getcatsitepagerank(conn, args, version, self.db_name, self.job_collection_name)

        out(json.dumps(result, default=json_util.default))


    def _edcoremetadatafreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetEDCoreMetadataFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    def _catcoremetadatafreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatCoreMetadataFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))


    def _cataccessabilityfreq(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        metric = Metrics()
        result=metric.GetCatAccessabilityFreq(conn, args, version, self.db_name, self.collection_name)

        out(json.dumps(result, default=json_util.default))

    def _cataloguesinfo(self, args, out, name = None, version = None, db = None, collection = None):
        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        conn = self._get_connection(name)
        if conn == None:
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        dates={}
        # if 'date_created' in args and args['date_created'][0].lower().strip() in ['1','true']:
        if "created_end_date" in args:
            try:
                dates.update({'catalogue_date_created':{'$lt':datetime.strptime(args['created_end_date'][0], '%Y-%m-%d')}})
            except ValueError as e:
                print (e)

        # start_date=end_date + relativedelta(years=-1)
        if "created_start_date" in args:
            try:
                dates.update({'catalogue_date_created':{'$gt':datetime.strptime(args['created_start_date'][0], '%Y-%m-%d')}})
            except ValueError as e:
                print (e)

        if "updated_end_date" in args:
            try:
                dates.update({'catalogue_date_updated':{'$lt':datetime.strptime(args['updated_end_date'][0], '%Y-%m-%d')}})
            except ValueError as e:
                print (e)

        # start_date=end_date + relativedelta(years=-1)
        if "updated_start_date" in args:
            try:
                dates.update({'catalogue_date_updated':{'$gt':datetime.strptime(args['updated_start_date'][0], '%Y-%m-%d')}})
            except ValueError as e:
                print (e)

        # print(dates)
        metric = Metrics()
        result=metric.Getcatsinfo(conn, args, version, self.db_name, self.job_collection_name,dates)

        out(json.dumps(result,default=self.__date_handler))

    def _commands(self, args, out, name = None, version = None, db = None, collection = None):
        reserved_names = [
            "_MongoHandler__output_results",
            "_MongoHandler__safety_check",
            "_MongoHandler__collect_country_stats",
            "_MongoHandler__date_handler",
            "_MongoHandler__normalize_values",
            "_MongoHandler__pearson",
            "_MongoHandler__reject_outliers",
            "_MongoHandler__reset_collection",
            "_MongoHandler__set_collection",
            "__init__",
            "_aggregate",
            "_authenticate",
            "_batch",
            "_cmd",
            "_commands",
            "_connect",
            "_find",
            "_get_connection",
            "_get_host_and_port",
            "_get_son",
            "_hello",
            "_insert",
            "_more",
            "_remove",
            "_status",
            "_totalNumOfCatalogue",
            "_update",
            "sm_object_hook",
            "_reset_collection",
            "_set_collection",
            "*__*",
        ]
        all_functions = inspect.getmembers(self.__class__, inspect.ismethod)

        metric = Metrics()
        methods_list = []
        for name,obj in all_functions:
            if name not in reserved_names:
                data = {name:{"help":metric.GetHelp(name)}}
                methods_list.append(data)
        str = json.dumps(methods_list, default=json_util.default)
#        out(json.dumps('{"ok":1, result:[%s]}' % methods_list, default=json_util.default))a
        out('{"ok":1, "result": %s}' % str)
#            out('{"ok" : 0, "errmsg" : "couldn\'t parse json: %s"}' % str)



class Metrics:
    def GetNumberOfDatasetsWithLicense(self, conn, args, version = None, db = None, collection = None):
        # pipeline_args = {'group': '$license_title'}
        # document=self._aggregate(conn, args, pipeline_args, version, self.db_name, 'odm')
        #
        # if document['ok']==1:
        #     catlicensedfreq=0
        #     i=0
        #     while i<len(document['result']):
        #         catlicensedfreq+=document['result'][i]['counter']
        #         i+=1
        #
        #     return {'ok': 1, 'result': catlicensedfreq}
        # else:
        #     return document
        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url': {'$nin': ['',None]}}},
            {'$match': {'license_id': {'$nin': ['',None]}}},
            {'$unwind': '$resources'},
            { '$group': {'_id': '$catalogue_url', 'counter' : {'$sum' : 1}}},
            { '$sort': { 'counter': -1 } },
            # {'$limit': 10}
            ])

        return result


    ##returns proportion of datasets with an explicity set license
    def GetProportionOfDatasetsWithLicense(self, conn, args, version = None, db = None, collection = None):
        # total_distrbs_per_catalogue=self.GetCatDistribsFreq(conn, args,version,db,collection,False)

        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url': {'$nin': ['',None]}}},
            # {'$match': {'license_title': {'$nin': ['',None]}}},
            {'$unwind': '$resources'},
            { '$group': {'_id': '$catalogue_url', 'licenses': {'$push': '$license_id' },'counter' : {'$sum' : 1}}},
            { '$sort': { 'counter': -1 } },
            # {'$limit': 10}
            ])

        total_freqs=[]
        if result['ok']==1:
            for i in range(0,len(result['result'])):
                nullfree_licenses=filter(bool, result['result'][i]['licenses'])

                total_freqs.append({'_id':result['result'][i]['_id'],
                    'licenses': len(nullfree_licenses)*100.00/result['result'][i]['counter']})

            return {'ok':1, 'result': total_freqs}
        else:
            return result



    def GetNumOfUniquePublishersContributingToCatalogue(self, conn, args, version = None, db = None, collection = None,
            limit = True):
        # pipeline_args = {'group': '$author'}
        # document=self._aggregate(conn, args, pipeline_args, version, self.db_name, 'odm')
        #
        # catuniquepublishersfreq=[]
        # if document['ok']==1:
        #     i=0
        #     while i<len(document['result']):
        #         bisect.insort(catuniquepublishersfreq, document['result'][i]['_id'])
        #         i+=1
        # else:
        #     return document
        #
        # pipeline_args = {'group': '$maintainer'}
        # document=self._aggregate(conn, args, pipeline_args, version, self.db_name, 'odm')
        #
        # if document['ok']==1:
        #     i=0
        #     while i<len(document['result']):
        #         try:
        #             catuniquepublishersfreq.index(document['result'][i]['_id'])
        #         except ValueError:
        #             bisect.insort(catuniquepublishersfreq, document['result'][i]['_id'])
        #         i+=1
        #
        #     return {'ok': 1, 'result': len(catuniquepublishersfreq)}
        # else:
        #     return document
        #
        if db == None or collection == None:
            return {"ok" : 0, "errmsg" : "db and collection must be defined"}

        pipeline = []
        if limit:
            pipeline.extend(({"$match": {'catalogue_url': { "$nin": [None,""]}}},
                { '$group' :{'_id' : "$catalogue_url", 'authors': {'$addToSet': '$author'},
                    'maintainers': {'$addToSet': '$maintainer'}, 'counter': {'$sum': 1}}},
                {'$sort':{'counter':-1}},
                # {'$limit':10}
                ))
        else:
            pipeline.extend(({"$match": {'catalogue_url': { "$nin": [None,""]}}},
                { '$group' :{'_id' : "$catalogue_url", 'authors': {'$addToSet': '$author'},
                    'maintainers': {'$addToSet': '$maintainer'}, 'counter': {'$sum': 1}}},
                ))
        result = conn[db][collection].aggregate(pipeline, allowDiskUse=True)
        # return result

        if result['ok']==1:
            uniquepublishers=[]
            for i in range(0,len(result['result'])):
                free_authors=filter(None,result['result'][i]['authors'] )
                free_maintainers=filter(None,result['result'][i]['maintainers'])
                mergelist=list(set(free_maintainers+free_authors))
                uniquepublishers.append({'_id': result['result'][i]['_id'], 'counter': len(mergelist)})

            return  {'ok': 1, 'result': uniquepublishers}
        else:
            return result


    ##Get number of unique publishers relative to the catalogue
    def GetNumOfUniquePublishersRelativeToCatalogue(self, conn, args, version = None, db = None, collection = None):
        # publishers=self.GetNumOfUniquePublishersContributingToCatalogue(conn, args, version, self.db_name, 'odm',False)
        pipeline=[]
        pipeline.extend(({"$match": {'catalogue_url': { "$nin": [None,""]}}},
            { '$group' :{'_id' : "$catalogue_url", 'all_authors': {'$push': '$author'},
                'all_maintainers': {'$push': '$maintainer'},'counter': {'$sum': 1}}},
            {'$sort':{'counter':-1}},
            # {'$limit':10}
            ))
        result = conn[db][collection].aggregate(pipeline, allowDiskUse=True)

        # pipeline_args = {'group': '$catalogue_url','limit':10}
        # datasets=self._aggregate(conn, args, pipeline_args, version, self.db_name, 'odm', False)

        catuniqpublishersprop=[]
        # if publishers['ok']==1 and datasets['ok']==1:
        if result['ok']==1:
            i=0
            # while i<len(publishers['result']):
            while i<len(result['result']):
                free_authors=filter(None,result['result'][i]['all_authors'] )
                free_maintainers=filter(None,result['result'][i]['all_maintainers'])
                mergelist=list(set(free_maintainers+free_authors))

                catuniqpublishersprop.append({'_id':result['result'][i]['_id'],
                    'counter':(len(mergelist)*100.00)/(result['result'][i]['counter'])})
                # catuniqpublishersprop.append({'_id':result['result'][i]['_id'],'counter':(len(mergelist)*100.00)/(len(result['result'][i]['authors'])+
                #     len(result['result'][i]['maintainers']))})

                # j=0
                # while j<len(datasets['result']):
                #     if datasets['result'][j]['_id'] == publishers['result'][i]['_id']:
                #         catuniqpublishersprop.append({'_id':datasets['result'][j]['_id'],'counter':(publishers['result'][i]['counter']*100.00)/(datasets['result'][j]['counter'])})
                #         del datasets['result'][j]
                #         break
                    # j+=1
                i+=1

            return {'ok': 1, 'result': catuniqpublishersprop}
        # elif publishers['ok']!=1:
        #     return publishers
        else:
            return result


    def GetNumberOfDatasets(self, conn, args, version = None, db = None, collection = None):
        # pipeline_args = {'group': '$title'}
        # document=self._aggregate(conn, args, pipeline_args, version, self.db_name, 'odm', False)
        #
        # if document['ok']==1:
        #     return {'ok': 1, 'result': len(document['result'])}
        # else:
        #     return document
        # result=conn[db][collection].aggregate([
        #     {'$match': {'catalogue_url': {'$nin':['',None]}}},
        #     { '$group': { '_id': '$catalogue_url', 'cat_size': {'$push':1 }}},
        #     { '$unwind':"$cat_size" },
        #     { '$group' : {'_id' : "$_id", 'counter' : {'$sum' : 1} } },
        #     { '$sort': { 'counter': -1 } },
        #     # {'$limit': 10}
        #     ])

        start_time=time.time()

        match={'$and':[{'extras.date_released':{'$type':9}}]}
        end_date=datetime.now()
        if "end_date" in args:
            try:
                end_date=datetime.strptime(args['end_date'][0], '%Y-%m-%d')
                match['$and'].append({'extras.date_released':{'$lte':end_date}})
            except ValueError as e:
                print (e)
        start_date=end_date + relativedelta(years=-1)
        if "start_date" in args:
            try:
                start_date=datetime.strptime(args['start_date'][0], '%Y-%m-%d')
                match['$and'].append({'extras.date_released':{'$gte':start_date}})
            except ValueError as e:
                print (e)

        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url': {'$nin': ['', None]}}},
            {'$match': match},
            {'$group': { '_id': {'year':{'$year':'$extras.date_released'},'month':{'$month':'$extras.date_released'}},
                    'catalogues':{'$push':'$catalogue_url'}, 'counter': {'$sum': 1}}},
            {'$sort':{'_id.year':-1,'_id.month':-1}}
            ])


        if result['ok']==1:
            date_catalogues=[]
            d=defaultdict(list)
            for i in range(0,len(result['result'])):
                cat_dict={}
                for cat in result['result'][i]['catalogues']:
                    try:
                        cat_dict[cat]+=1
                    except KeyError:
                        cat_dict[cat]=1

                result['result'][i]['catalogues']=cat_dict
                result['result'][i]['date']=str(date(result['result'][i]['_id']['year']
                        ,result['result'][i]['_id']['month'],1))
                del result['result'][i]['counter']
                del result['result'][i]['_id']

            print (time.time() - start_time)

            return {'ok':1, 'result': result['result']}
        else:
            return result


    def GetDatasetsTotFreq(self, conn, args, version = None, db = None, collection = None):
        match_dict =  {'catalogue_url': {'$nin': ['', None]},
                'extras.unpublished':{'$ne':"true"},
                'is_duplicate':{'$ne':True}
                }

        if 'include_dupls' in args:
            dupl_val = args['include_dupls'][0]
            if dupl_val.lower() in ['true','1']:
                del match_dict['is_duplicate']

        result=conn[db][collection].aggregate([
            { '$match': match_dict },
            { '$group': { '_id': '$catalogue_url', 'count': {'$sum':1 }}},
            { '$sort': {'count':-1 }},
        ])

        return result


    def GetCDDatasetsFreq(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$match': {'country': {'$nin': ['', None]},
                'is_duplicate':{'$ne':True},
                }},
            { '$group': { '_id': '$country', 'freq': {'$sum':1 }}},
            { '$sort': {'freq':-1 }},
        ])

        return result


    def GetDatasets(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url': {'$nin': ['', None]},
                'is_duplicate':{'$ne':True},
                }},
            { '$group': { '_id': 0, 'freq': {'$sum':1 }}},
            { '$sort': {'freq':-1 }},
            { '$project': { '_id':0, 'freq':1 }},
        ])

        return result


    def GetTotalNumberOfCatalogues(self, conn, args, version = None, db = None, collection = None):
        # pipeline_args = {'group': '$cat_url'}
        # document=self._aggregate(conn, args, pipeline_args, version, 'odm', 'jobs', False)
        #
        # if document['ok']==1:
        #     return {'ok': 1, 'result': len(document['result'])}
        # else:
        #     return document
        # return {'ok': 1, 'result': conn[db][collection].find().count()}
        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url': {'$nin':['',None]}}},
            { '$group': { '_id': '', 'cat_size': {'$addToSet':'$catalogue_url' }}},
            { '$project': { '_id':0, 'count': {'$size': '$cat_size'} } },
            ])

        # if result['ok']==1:
        #     return {'ok':1, 'result': result['result'][0]['counter']}
        # else:
        return result


    def GetTotalDistribsFreq(self, conn, args, version = None, db = None, collection = None):
        # EXCLUDE_COUNTRIES=re.compile('\.eu|datahub\.io')
        result=conn[db][collection].aggregate([
            {'$match': {'$and':[{'catalogue_url': {'$nin': ['', None]}},
                {'resources':{'$exists':True}},
                {'$or':[{'is_duplicate':False},{'is_duplicate':{'$exists':False}}]},
                # {'catalogue_url':{'$not':EXCLUDE_COUNTRIES}}
                ]}},
            { '$group': {'_id' : 0, 'counter': {'$sum': {'$size': '$resources'}}}},
            {'$project':{'_id':0,'counter':1}}
        ])

        if result['ok']==1:
            return {'ok':1, 'result': result['result'][0]['counter']}
        else:
            return result


    def GetFreqOfCatalogues(self, conn, args, version = None, db = None, collection = None):
        # pipeline_args = {'group': '$type'}
        # document=self._aggregate(conn, args, pipeline_args, version, db, collection, True, 'ckan')
        #
        # return document
        pipeline=[]
        catalogue=''
        if 'platform' in args:
            pipeline.append({'$match': {'platform': args['platform'][0]}})

        pipeline.extend((
            {'$match': {'catalogue_url': {'$nin':['',None]}}},
            {'$group': {'_id': '$catalogue_url', 'platform_types': {'$addToSet': '$platform'}}},
            {'$group': {'_id': '$platform_types', 'counter': {'$sum': 1}}},
            {'$sort': {'counter': -1}}))
        # result = conn[db][collection].find({'type':catalogue}).count()
        result = conn[db][collection].aggregate(pipeline)

        if result['ok']==1:
            cats=[]
            for i in range(0,len(result['result'])):
                if len(result['result'][i]['_id'])>0:
                    cats.append({'_id':result['result'][i]['_id'][0],'counter':result['result'][i]['counter']})
                else:
                    cats.append({'_id':None,'counter':result['result'][i]['counter']})

            return {'ok':1,'result':cats}
        else:
            # return {'ok': 1, 'result': {'_id':catalogue, 'counter': result}}
            return result


    ##returns proportion of catalogues using specific software platforms
    def GetProportionOfCatalogues(self, conn, args, version = None, db = None, collection = None):
        # pipeline_args = {'group': '$type'}
        # document=self._aggregate(conn, args, pipeline_args, version, 'odm', 'jobs', False, 'ckan')
        #
        # if document['ok']==1:
        #     catalogues=self.GetTotalNumberOfCatalogues(conn, args, version, 'odm', 'jobs')
        #     if catalogues['ok']==1:
        #         if document['result']:
        #             catsoftprop=(document['result'][0]['counter']*100.00)/(catalogues['result'])
        #         else:
        #             catsoftprop = 0.00
        #         return {'ok': 1, 'result': catsoftprop}
        #     else:
        #         return catalogues
        # else:
        #     return document

        pipeline=[]
        catalogue=''
        if 'platform' in args:
            pipeline.append({'$match': {'platform': args['platform'][0]}})

        pipeline.extend((
            {'$match': {'catalogue_url': {'$nin':['',None]}}},
            {'$group': {'_id': '$catalogue_url', 'platform_types': {'$addToSet': '$platform'}}},
            {'$group': {'_id': '$platform_types', 'counter': {'$sum': 1}}},
            {'$sort': {'counter': -1}}))
        # result = conn[db][collection].find({'type':catalogue}).count()
        result = conn[db][collection].aggregate(pipeline)

        catalogues=self.GetTotalNumberOfCatalogues(conn, args, version, db, collection)

        if catalogues['ok']==1:
            cats=[]
            for i in range(0,len(result['result'])):
                if len(result['result'][i]['_id'])>0:
                    cats.append({'_id':result['result'][i]['_id'][0],
                        # 'counter':result['result'][i]['counter']*100.00/catalogues['result'][0]['counter']})
                        'counter':result['result'][i]['counter']*100.00/catalogues['result']})
                else:
                    # cats.append({'_id':None,'counter':result['result'][i]['counter']*100.00/catalogues['result'][0]['counter']})
                    cats.append({'_id':None,'counter':result['result'][i]['counter']*100.00/catalogues['result']})
                # result['result'][i]['counter']=result['result'][i]['counter']*100.00/catalogues['result'][0]['numberOfCatalogues']
        # return {'ok': 1, 'result': {'_id':catalogue, 'counter': result}}

            return {'ok':1,'result':cats}
        else:
            return result


    ## returns the number of unique organisations publishing data (publishers)
    def GetNumberOfUniquePublishers(self, conn, args, version = None, db = None, collection = None):
        # pipeline_args = {'group': '$author'}
        # document=self._aggregate(conn, args, pipeline_args, version, 'odm', 'odm')
        #
        # if document['ok']==1:
        #     return {'ok': 1, 'result': len(document['result'])}
        # else:
        #     return document
        #
        match_dict =  {
                'organization.title': {'$nin': ['',None]},
                'catalogue_url': {'$nin': ['', None]},
                'extras.unpublished':{'$ne':"true"},
                'is_duplicate':{'$ne':True}
                }

        if 'include_dupls' in args:
            dupl_val = args['include_dupls'][0]
            if dupl_val.lower() in ['true','1']:
                del match_dict['is_duplicate']

        result=conn[db][collection].aggregate([
            {'$match': match_dict},
            { '$group': { '_id': '$catalogue_url', 'unique_org_size': {'$addToSet': '$organization.title' }}},
            { '$unwind':"$unique_org_size" },
            { '$group' : {'_id' : "$_id", 'count' : {'$sum' : 1} } },
            { '$sort': { 'count': -1 } },
        ])

        return result



    ## returns the number of unique organisations publishing data (publishers)
    def GetCDPublishersFreq(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$match': {'organization.title': {'$nin': ['',None]},
                'country': {'$nin': ['', None]},
                'is_duplicate':{'$ne':True},
                }},
            { '$group': { '_id': '$country', 'unique_org_size': {'$addToSet': '$organization.title' }}},
            { '$unwind':"$unique_org_size" },
            { '$group' : {'_id' : "$_id", 'freq' : {'$sum' : 1} } },
            { '$sort': { 'freq': -1 } },
        ])

        return result


    ## returns the number of datasets with Open License
    ## (list from http://opendefinition.org/licenses)
    def GetNumberOfDatasetsWithOpenLicense(self, conn, args, version = None, db = None, collection = None):
        open_licenses = [x.upper() for x in def_formatLists.ODM_Licenses().get_open_licenses()]

        tmp_collection='tmp_coll_openlic'

        start_time=time.time()

        combined_version = [u'CC BY-SA',u'CC BY',u'CC0',u'GNU GPL']

        start_time=time.time()

        match_dict =  {'catalogue_url': {'$nin': ['', None]},
                'extras.unpublished':{'$ne':"true"},
                'license_id': {'$nin': ['',None]},
                'is_duplicate':{'$ne':True}
                }

        if 'include_dupls' in args:
            dupl_val = args['include_dupls'][0]
            if dupl_val.lower() in ['true','1']:
                del match_dict['is_duplicate']

        result=conn[db][collection].aggregate([
            {'$match': match_dict},
            {'$unwind': '$resources'},
            { '$group': {'_id': '$catalogue_url', 'licenses': {'$push': '$license_id' },'freq' : {'$sum' : 1}}},
            { '$sort': { 'freq': -1 } },
            {'$out':tmp_collection},
            ])

        total_freqs=[]
        # tot_counter={}
        if result['ok']==1:
            result=conn[db][tmp_collection].find()
            if result.count()>0:
            # for i in range(0,len(result['result'])):
                for res in result:
                    dict_licenses={}
                    counter=0
                    tot_counter=0
                    dict_licenses={}
                    # for j in range(0,len(result['result'][i]['licenses'])):
                    for j in range(0,len(res['licenses'])):
                        # db_lic=result['result'][i]['licenses'][j]
                        db_lic=res['licenses'][j].upper()
                        original_lic = db_lic
                        for comb in combined_version:
                            if db_lic.startswith(comb) and len(db_lic) != len(comb):
                                del_diff,add_diff = odm_comp(comb,db_lic)
                                if len(del_diff) == 0 and (len(add_diff) >= 4 and add_diff[1]['char'].isdigit() and add_diff[3]['char'].isdigit()):
                                    db_lic = comb
                                    break
                        for open_lic in open_licenses:
                            if open_lic==db_lic:
                                if original_lic in dict_licenses:
                                    dict_licenses[original_lic]+=1
                                else:
                                    dict_licenses[original_lic]=1

                                counter+=1
                                break
                        tot_counter+=1

                    sorted_licenses=OrderedDict()
                    for key, value in sorted(dict_licenses.iteritems(), key=lambda (k,v): (v,k),reverse=True):
                        # sorted_licenses[key]=value/tot_counter
                        sorted_licenses[key]=value
                    total_freqs.append({'_id':res['_id'],'licenses': sorted_licenses,
                        'count':counter,'tot_count':tot_counter})

            conn[db].drop_collection(tmp_collection)

            print (time.time()-start_time)
            return {'ok':1, 'result': total_freqs}
        else:
            return result


        # total_freqs=[]
        # if result['ok']==1:
        #     for i in range(0,len(result['result'])):
        #         dict_licenses={}
        #         for j in range(0,len(result['result'][i]['licenses'])):
        #             key=result['result'][i]['licenses'][j]
        #             if key not in ['',None]:
        #                 if key in dict_licenses:
        #                     dict_licenses[key]+=1
        #                 else:
        #                     dict_licenses[key]=1
        #
        #         del_licenses=[]
        #         for key_license in dict_licenses:
        #             not_open_license=True
        #             k=0
        #             try:
        #                 while k<len(open_licenses):
        #                     string_matching=difflib.SequenceMatcher(None,open_licenses[k].encode('utf8'),
        #                             str(key_license.encode('utf-8'))).ratio()
        #                     if string_matching>0.9:
        #                         not_open_license = False
        #                         break
        #                     k+=1
        #             except UnicodeEncodeError as e:
        #                 print (e,key_license)
        #             if not_open_license:
        #                 del_licenses.append(key_license)
        #
        #         for key in del_licenses:
        #             del dict_licenses[key]
        #
        #         sorted_licenses=OrderedDict()
        #         for key, value in sorted(dict_licenses.iteritems(), key=lambda (k,v): (v,k),reverse=True):
        #             sorted_licenses[key]=value
        #         total_freqs.append({'_id':result['result'][i]['_id'],'licenses': sorted_licenses})



    def GetEDOpenLicenseFreq(self, conn, args, version = None, db = None, collection = None):
        open_licenses = [x.upper() for x in def_formatLists.ODM_Licenses().get_open_licenses()]

        combined_version = [u'CC BY-SA',u'CC BY',u'CC0',u'GNU GPL']

        start_time=time.time()

        match_dict =  {'catalogue_url': {'$nin': ['', None]},
                'extras.unpublished':{'$ne':"true"},
                'is_duplicate':{'$ne':True}
                }

        if 'include_dupls' in args:
            dupl_val = args['include_dupls'][0]
            if dupl_val.lower() in ['true','1']:
                del match_dict['is_duplicate']

        results=conn[db][collection].find(
                match_dict,
            # {'$unwind': '$resources'},
            # { '$group': {'_id': 0, 'licenses': {'$push': '$license_id' },'freq' : {'$sum' : 1}}},
            {'_id':0,'license_id':1,'resources.url':1,'num_resources':1,
                # 'catalogue_url':1,
                },
            )
        results.batch_size(MongoHandler.batchSize)

        total_freqs=[]
        tot_counter=0
        if results.count()>0:
            dict_licenses={}
            for result in results:
                # if 'num_resources' in result and 'resources' in result and 'license_id' in result:
                try:
                    if 'resources' in result and 'license_id' in result:
                        db_lic = result['license_id'].upper()
                        # original_lic = db_lic
                        for comb in combined_version:
                            if db_lic.startswith(comb) and len(db_lic) != len(comb):
                                del_diff,add_diff = odm_comp(comb,db_lic)
                                if len(del_diff) == 0 and (len(add_diff) >= 4 and add_diff[1]['char'].isdigit() and add_diff[3]['char'].isdigit()):
                                    db_lic = comb
                                    break
                        for open_lic in open_licenses:
                            # distribs_size=result['num_resources']
                            distribs_size=len(result['resources'])
                            if open_lic==db_lic:
                                if db_lic in dict_licenses:
                                    dict_licenses[db_lic]+=distribs_size
                                else:
                                    dict_licenses[db_lic]=distribs_size
                                break
                        tot_counter+=distribs_size
                except AttributeError,e:
                    print(e)


            sorted_licenses=OrderedDict()
            counter=0
            if tot_counter>0:
                for key, value in sorted(dict_licenses.iteritems(), key=lambda (k,v): (v,k),reverse=True):
                    counter+=value
                    sorted_licenses[key]=value/tot_counter
                total_freqs.append({'licenses': sorted_licenses,'freq':counter/tot_counter})
            else:
                total_freqs.append({'licenses': sorted_licenses,'freq':counter})

            print (time.time()-start_time)
            return {'ok':1, 'result': total_freqs}
        else:
            return result


    def GetCDOpenLicenseFreq(self, conn, args, version = None, db = None, collection = None):
        open_licenses = [x.upper() for x in def_formatLists.ODM_Licenses().get_open_licenses()]

        start_time=time.time()

        combined_version = [u'CC BY-SA',u'CC BY',u'CC0',u'GNU GPL']

        start_time=time.time()

        result=conn[db][collection].aggregate([
            {'$match': {'country': {'$nin': ['', None]},
                'license_id': {'$nin': ['',None]},
                'is_duplicate':{'$ne':True},
                }},
            {'$unwind': '$resources'},
            { '$group': {'_id': '$country', 'licenses': {'$push': '$license_id' },'freq' : {'$sum' : 1}}},
            { '$sort': { 'freq': -1 } },
            ])

        total_freqs=[]
        tot_counter={}
        if result['ok']==1:
            for i in range(0,len(result['result'])):
                tot_counter=0
                dict_licenses={}
                for j in range(0,len(result['result'][i]['licenses'])):
                    db_lic=result['result'][i]['licenses'][j].upper()
                    for comb in combined_version:
                        if db_lic.startswith(comb):
                            db_lic = comb
                            break
                    for open_lic in open_licenses:
                        if open_lic==db_lic:
                            if db_lic in dict_licenses:
                                dict_licenses[db_lic]+=1
                            else:
                                dict_licenses[db_lic]=1
                            break
                    tot_counter+=1

                sorted_licenses=OrderedDict()
                for key, value in sorted(dict_licenses.iteritems(), key=lambda (k,v): (v,k),reverse=True):
                    sorted_licenses[key]=value/tot_counter
                total_freqs.append({'_id':result['result'][i]['_id'],'licenses': sorted_licenses})

            print (time.time()-start_time)
            return {'ok':1, 'result': total_freqs}
        else:
            return result


    ## returns the proportion of datasets with Open License (list from http://opendefinition.org/licenses)
    def GetProportionOfDatasetsWithOpenLicense(self, conn, args, version = None, db = None, collection = None):
        open_licenses = [x.upper() for x in def_formatLists.ODM_Licenses().get_open_licenses()]

        # pipeline_args = {'group': '$license_title'}
        # document=self._aggregate(conn, args, pipeline_args, version, 'odm', 'odm', False)
        #
        # if document['ok']==1:
        #     sum=0
        #     i=0
        #     while i<len(document['result']):
        #         sum+=document['result'][i]['counter']
        #         i+=1
        #
        #     catopenlicfreq=self.GetNumberOfDatasetsWithOpenLicense(conn, args, version, 'odm', 'odm')
        #     if catopenlicfreq['ok']==1:
        #         catopenlicprop=(catopenlicfreq['result']*100.00)/(sum)
        #
        #         return {'ok': 1, 'result': catopenlicprop}
        #     else:
        #         return catopenlocfreq
        # else:
            # return document

        # total_distrbs_per_catalogue=self.GetCatDistribsFreq(conn, args,version,db,collection,False)
        start_time=time.time()

        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url': {'$nin': ['',None]}}},
            # {'$match': {'license_title': {'$nin': ['',None]}}},
            {'$unwind': '$resources'},
            { '$group': {'_id': '$catalogue_url', 'licenses': {'$push': '$license_id' },'counter' : {'$sum' : 1}}},
            { '$sort': { 'counter': -1 } },
            ])

        total_freqs=[]
        if result['ok']==1:
            for i in range(0,len(result['result'])):
                dict_licenses={}
                for j in range(0,len(result['result'][i]['licenses'])):
                    key=result['result'][i]['licenses'][j].upper()
                    if key not in ['',None]:
                        if key in dict_licenses:
                            dict_licenses[key]+=1
                        else:
                            dict_licenses[key]=1

                del_licenses=[]
                for key_license in dict_licenses:
                    not_open_license=True
                    k=0
                    try:
                        while k<len(open_licenses):
                            string_matching=difflib.SequenceMatcher(None,open_licenses[k].encode('utf8'),
                                    str(key_license.encode('utf8'))).ratio()
                            if string_matching>0.9:
                                not_open_license = False
                                break
                            k+=1
                    except UnicodeEncodeError as e:
                        print (e,key_license)

                    if not_open_license:
                        del_licenses.append(key_license)

                for key in del_licenses:
                    del dict_licenses[key]

                # cat_distributions=0
                # for k in range(0,len(total_distrbs_per_catalogue['result'])):
                #     if total_distrbs_per_catalogue['result'][k]['_id']==result['result'][i]['_id']:
                #         cat_distributions=total_distrbs_per_catalogue['result'][k]['counter']
                #         del total_distrbs_per_catalogue['result'][k]
                #         break

                cat_distributions=len(result['result'][i]['licenses'])
                sorted_licenses=OrderedDict()
                for key, value in sorted(dict_licenses.iteritems(), key=lambda (k,v): (v,k),reverse=True):
                    sorted_licenses[key]=value*100.00/cat_distributions
                total_freqs.append({'_id':result['result'][i]['_id'],'licenses': sorted_licenses})

            print (time.time()-start_time)
            return {'ok':1, 'result': total_freqs}
        else:
            return result


    def GetOpenLicenseFreqPerDate(self, conn, args, version = None, db = None, collection = None):
        open_licenses = [x.upper() for x in def_formatLists.ODM_Licenses().get_open_licenses()]

        start_time=time.time()

        match={'$and':[{'catalogue_url': {'$nin': ['',None]}},
            {'license_id':{'$nin':['',None]}},
            {'extras.date_released':{'$type':9}}]}
        # end_date=datetime.now()
        if "end_date" in args:
            try:
                end_date=datetime.strptime(args['end_date'][0], '%Y-%m-%d')
                match['$and'].append({'extras.date_released':{'$lte':end_date}})
            except ValueError as e:
                print (e)
        # start_date=end_date + relativedelta(years=-1)
        if "start_date" in args:
            try:
                start_date=datetime.strptime(args['start_date'][0], '%Y-%m-%d')
                match['$and'].append({'extras.date_released':{'$gte':start_date}})
            except ValueError as e:
                print (e)

            # {'$group': { '_id': {'year':{'$year':'$extras.date_released'},'month':{'$month':'$extras.date_released'}},
            #         'catalogues':{'$push':'$catalogue_url'}, 'counter': {'$sum': 1}}},
        result=conn[db][collection].aggregate([
            # {'$match': {'$and':[{'catalogue_url': {'$nin': ['',None]}},
            #     {'extras.date_released':{'$type':9}},{'license_id':{'$nin':['',None]}}]}},
            {'$match': match},
            {'$unwind': '$resources'},
            { '$group': {'_id': 0, 'licenses': {'$push': {'lic':'$license_id','date':'$extras.date_released'} },
                'counter' : {'$sum' : 1}}},
        ])

        total_freqs=[]
        if result['ok']==1:
            datedict={}
            for i in range(0,len(result['result'])):
                dict_licenses={}
                for j in range(0,len(result['result'][i]['licenses'])):
                    key=result['result'][i]['licenses'][j]['lic'].upper()

                    lic_dt=result['result'][i]['licenses'][j]['date']
                    roundup_lic_dt=date(lic_dt.year,
                            lic_dt.month,1)
                    # print(roundup_lic_dt)
                    if roundup_lic_dt not in datedict:
                        datedict[roundup_lic_dt]=dict()
                    if key in datedict[roundup_lic_dt]:
                        datedict[roundup_lic_dt][key]+=1
                    else:
                        datedict[roundup_lic_dt][key]=1

            del_dts=[]
            for _dts in datedict:
                del_licenses=[]
                # print(_dts)
                for key_license in datedict[_dts]:
                    not_open_license=True
                    k=0
                    try:
                        while k<len(open_licenses):
                            string_matching=difflib.SequenceMatcher(None,open_licenses[k].encode('utf8'),
                                    str(key_license.encode('utf-8'))).ratio()
                            if string_matching>0.9:
                                not_open_license = False
                                break
                            k+=1
                    except UnicodeEncodeError as e:
                        print (e,key_license)
                    if not_open_license:
                        del_licenses.append(key_license)

                for key in del_licenses:
                    del datedict[_dts][key]

                if len(datedict[_dts])==0:
                    del_dts.append(_dts)

            for key in del_dts:
                del datedict[key]

            # sorted_licenses=OrderedDict()
            # for key, value in sorted(dict_licenses.iteritems(), key=lambda (k,v): (v,k),reverse=True):
            #     sorted_licenses[key]=value
            # total_freqs.append({'_id':result['result'][i]['_id'],'licenses': sorted_licenses})

            total_freqs=[]
            sorted_datedict=(sorted(datedict.items(),key=operator.itemgetter(0)))
            sorted_datedict.reverse()
            for _dt in sorted_datedict:
                total_freqs.append({'date':str(_dt[0]),'freq':_dt[1]})

            print (time.time()-start_time)
            return {'ok':1, 'result': total_freqs}
        else:
            return result


    ##Get frequency of datasets by license type
    def GetFrequencyOfDatasetsByLicenseType(self, conn, args, version = None, db = None, collection = None):
        tmp_collection='tmp_catdsbylicensefreq'
        # pipeline_args = {'group': '$license_title','limit': 10}
        # document=self._aggregate(conn, args, pipeline_args, version, db, collection
        start_time = time.time()

        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url': {'$nin': ['',None]}}},
            {'$unwind': '$resources'},
            { '$group': {'_id': '$catalogue_url', 'licenses': {'$push': '$license_id' },'counter' : {'$sum' : 1}}},
            { '$sort': { 'counter': -1 } },
            {'$out':tmp_collection},
            ])

        total_freqs=[]
        if result['ok']==1:
            result=conn[db][tmp_collection].find()
            if result.count()>0:
                for res in result:
                    dict_licenses={}
                    for j in range(0,len(res['licenses'])):
                        key=res['licenses'][j]
                        if key=='':
                            continue
                        if key in dict_licenses:
                            dict_licenses[key]+=1
                        else:
                            # try:
                            #     dict_licenses[str(key)]=1
                            # except UnicodeEncodeError:
                            #     print ('error with key: %s' % key)
                            dict_licenses[key]=1

                    sorted_licenses=OrderedDict()
                    for key, value in sorted(dict_licenses.iteritems(), key=lambda (k,v): (v,k),reverse=True):
                        sorted_licenses[key]=value
                    total_freqs.append({'_id':res['_id'],'licenses': sorted_licenses})

            conn[db].drop_collection(tmp_collection)
            print (time.time()-start_time)

            return {'ok':1, 'result': total_freqs}
        else:
            conn[db].drop_collection(tmp_collection)
            return result



    ##Get Proportion of datases by license type
    def GetProportionOfDatasetsByLicenseType(self, conn, args, version = None, db = None, collection = None):
        start_time = time.time()

        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url': {'$nin': ['',None]}}},
            {'$unwind': '$resources'},
            { '$group': {'_id': '$catalogue_url', 'licenses': {'$push': '$license_id' },'counter' : {'$sum' : 1}}},
            { '$sort': { 'counter': -1 } },
            # {'$limit': 10}
            ])

        total_freqs=[]
        if result['ok']==1:
            for i in range(0,len(result['result'])):
                dict_licenses={}
                for j in range(0,len(result['result'][i]['licenses'])):
                    key=result['result'][i]['licenses'][j]
                    if key=='':
                        continue
                    if key in dict_licenses:
                        dict_licenses[key]+=1
                    else:
                        dict_licenses[key]=1

                cat_distributions=len(result['result'][i]['licenses'])
                sorted_licenses=OrderedDict()
                for key, value in sorted(dict_licenses.iteritems(), key=lambda (k,v): (v,k),reverse=True):
                    sorted_licenses[key]=value*100.00/cat_distributions
                total_freqs.append({'_id':result['result'][i]['_id'],'licenses': sorted_licenses})

            print (time.time()-start_time)
            return {'ok':1, 'result': total_freqs}
        else:
            return result



    ##Get Frequency of datasets by file format
    def GetFrequencyOfDatasetsByFileFormat(self, conn, args, version = None, db = None, collection = None):
        # pipeline_args = {'group': '$resources.format', 'unwind': '$resources'}
        # document=self._aggregate(conn, args, pipeline_args, version, 'odm', 'odm')
        #
        # return document
        start_time=time.time()

        result=conn[db][collection].aggregate([
            # {'$match': {'resources.0': {'$exists': True}}},
            {'$match': {'catalogue_url': {'$nin': ['',None]}}},
            {'$unwind':'$resources'},
            # {'$match': {'resources.format': {'$nin': ['',None]}, 'catalogue_url': {'$nin': ['',None]}}},
            { '$group': { '_id': '$catalogue_url', 'resources': {'$push': '$resources.format' },'counter' : {'$sum' : 1}}},
            { '$sort': { 'counter': -1 } },
            ])

        # print (time.time() - start_time)
        # return result

        if result['ok']==1:
            total=[]
            for i in range(0,len(result['result'])):
                dict_formats={}
                for j in range(0,len(result['result'][i]['resources'])):
                    format_key=result['result'][i]['resources'][j]
                    if format_key=='':
                        continue

                    if format_key in dict_formats:
                        dict_formats[format_key]+=1
                    else:
                        dict_formats[format_key]=1

                sorted_formats=OrderedDict()
                for key, value in sorted(dict_formats.iteritems(), key=lambda (k,v): (v,k),reverse=True):
                    sorted_formats[key]=value
                total.append({'_id':result['result'][i]['_id'],'formats': sorted_formats})

            print (time.time() - start_time)

            return {'ok':1, 'result': total}
        else:
            return result


    def GetEDNonProprietaryFormatFreq(self, conn, args, version = None, db = None, collection = None):
        # pipeline_args = {'group': '$resources.format', 'unwind': '$resources'}
        # document=self._aggregate(conn, args, pipeline_args, version, 'odm', 'odm')
        #
        # return document
        non_propr=def_formatLists.ODM_formats().get_non_proprietary()

        regex = re.compile("[.-]+ *")

        start_time=time.time()

        match_dict =  {'catalogue_url': {'$nin': ['', None]},
                'extras.unpublished':{'$ne':"true"},
                'is_duplicate':{'$ne':True}
                }

        if 'include_dupls' in args:
            dupl_val = args['include_dupls'][0]
            if dupl_val.lower() in ['true','1']:
                del match_dict['is_duplicate']

        result=conn[db][collection].aggregate([
            {'$match': match_dict},
            {'$unwind':'$resources'},
            { '$group': { '_id': 0, 'resources': {'$push': '$resources.format' },'counter' : {'$sum' : 1}}},
            # { '$sort': { 'counter': -1 } },
            ])

        if result['ok']==1:
            total=[]
            for i in range(0,len(result['result'])):
                tot_count=0
                # dict_formats={}
                format_count=0
                for j in range(0,len(result['result'][i]['resources'])):
                    tot_count+=1
                    format_key=result['result'][i]['resources'][j]
                    try:
                        format_key=regex.sub('',format_key)
                    except TypeError,e:
                        print(e,format_key)
                        continue
                    format_key=format_key.lower()
                    if format_key in non_propr:
                        format_count+=1
                    # if format_key not in non_propr:
                    #     continue
                    #
                    # if format_key in dict_formats:
                    #     dict_formats[format_key]+=1
                    # else:
                    #     dict_formats[format_key]=1

                # sorted_formats=OrderedDict()
                # for key, value in sorted(dict_formats.iteritems(), key=lambda (k,v): (v,k),reverse=True):
                #     sorted_formats[key]=value/tot_count

                # total.append({'_id':result['result'][i]['_id'],'formats': sorted_formats})
                total.append({'freq': format_count/tot_count})

            print (time.time() - start_time)

            return {'ok':1, 'result': total}
        else:
            return result


    def GetCatNonProprietaryFormatFreq(self, conn, args, version = None, db = None, collection = None):
        non_propr=def_formatLists.ODM_formats().get_non_proprietary()

        regex = re.compile("[.-]+ *")

        start_time=time.time()

        match_dict =  {'catalogue_url': {'$nin': ['', None]},
                'extras.unpublished':{'$ne':"true"},
                'is_duplicate':{'$ne':True}
                }

        if 'include_dupls' in args:
            dupl_val = args['include_dupls'][0]
            if dupl_val.lower() in ['true','1']:
                del match_dict['is_duplicate']

        result=conn[db][collection].aggregate([
            {'$match': match_dict},
            {'$unwind':'$resources'},
            { '$group': { '_id': '$catalogue_url', 'resources': {'$push': '$resources.format' },'count' : {'$sum' : 1}}},
            { '$sort': { 'count': -1 } },
            ])

        if result['ok']==1:
            total=[]
            for i in range(0,len(result['result'])):
                tot_count=0
                # dict_formats={}
                format_count=0
                for j in range(0,len(result['result'][i]['resources'])):
                    tot_count+=1
                    format_key=result['result'][i]['resources'][j]
                    try:
                        format_key=regex.sub('',format_key)
                    except TypeError,e:
                        print(e,format_key)
                        continue
                    format_key=format_key.lower()
                    if format_key in non_propr:
                        format_count+=1
                    # if format_key not in non_propr:
                    #     continue
                    #
                    # if format_key in dict_formats:
                    #     dict_formats[format_key]+=1
                    # else:
                    #     dict_formats[format_key]=1

                # sorted_formats=OrderedDict()
                # for key, value in sorted(dict_formats.iteritems(), key=lambda (k,v): (v,k),reverse=True):
                #     sorted_formats[key]=value/tot_count

                total.append({'_id':result['result'][i]['_id'],'count': format_count,
                    'tot_count':tot_count})

            print (time.time() - start_time)

            return {'ok':1, 'result': total}
        else:
            return result


    ##Get Proportion of datasets by file format
    def GetProportionOfDatasetsByFileFormat(self, conn, args, version = None, db = None, collection = None):
        # pipeline_args = {'group': '$resources.format', 'unwind': '$resources'}
        # document=self._aggregate(conn, args, pipeline_args, version, 'odm', 'odm', False)
        #
        # if document['ok']==1:
        #      document1=document['result']
        #      i=0
        #      datasets_counter=0
        #      while i<len(document1):
        #              datasets_counter+=document1[i]['counter']
        #              i+=1
        #
        #      i=0
        #      while i<len(document1):
        #        document1[i]['counter']=(document1[i]['counter']*100.00)/(datasets_counter)
        #        i+=1
        #
        #      return {'ok': 1, 'result': document1}
        # else:
        #     return document
        start_time=time.time()

        # total_distrbs_per_catalogue=self.GetCatDistribsFreq(conn, args,version,db,collection,False)

        # if total_distrbs_per_catalogue['ok']==1:
        result=conn[db][collection].aggregate([
            # {'$match': {'resources.0': {'$exists': True}}},
            {'$match': {'catalogue_url': {'$nin': ['',None]}}},
            {'$unwind':'$resources'},
            # {'$match': {'resources.format': {'$nin': ['',None]}, 'catalogue_url': {'$nin': ['',None]}}},
            { '$group': { '_id': '$catalogue_url', 'resources': {'$push': '$resources.format' },'counter' : {'$sum' : 1}}},
            { '$sort': { 'counter': -1 } },
            # {'$limit': 10}
            ])


        if result['ok']==1:
            total=[]
            for i in range(0,len(result['result'])):
                dict_formats={}
                for j in range(0,len(result['result'][i]['resources'])):
                    format_key=result['result'][i]['resources'][j]
                    if format_key=='':
                        continue

                    if format_key in dict_formats:
                        dict_formats[format_key]+=1
                    else:
                        dict_formats[format_key]=1

                sorted_formats=OrderedDict()
                # cat_distributions=0
                # for k in range(0,len(total_distrbs_per_catalogue['result'])):
                #     if total_distrbs_per_catalogue['result'][k]['_id']==result['result'][i]['_id']:
                #         cat_distributions=total_distrbs_per_catalogue['result'][k]['counter']
                #         del total_distrbs_per_catalogue['result'][k]
                #         break

                cat_distributions=len(result['result'][i]['resources'])
                for key, value in sorted(dict_formats.iteritems(), key=lambda (k,v): (v,k),reverse=True):
                    sorted_formats[key]=(value*100.00)/cat_distributions
                total.append({'_id':result['result'][i]['_id'],'formats': sorted_formats})

            print (time.time() - start_time)
            return {'ok':1,'result':total}
        else:
            return result
        # else:
        #     return total_distrbs_per_catalogue


    ##Get all catalogues
    def GetCatalogues(self, conn, args, version = None, db = None, collection = None):
        pipeline_args = {'group': '$catalogue_url'}
        document=self._aggregate(conn, args, pipeline_args, version, db, collection)

        return document


    def GetCatMedianAge(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url':{'$nin':['',None]}}},
            {'$match': {'extras.date_released':{'$type':9}}},
            { '$group': {'_id' : '$catalogue_url', 'years': {'$push': {'$year': '$extras.date_released'}},
                'counter': {'$sum': 1}}},
            {'$sort': { 'counter':-1}},
            # {'$limit': 10}
            ])

        if result['ok']==1:
            curr_date=datetime.now()
            cat_mean=[]
            for i in range(0,len(result['result'])):
                # cat_mean.append({'catalogue':result['result'][i]['_id'],'median_age':numpy.median(result['result'][i]['years'])})
                # print('%d) before: %s' % (i,numpy.array(result['result'][i]['years'])))
                dates=self.__reject_outliers(numpy.array(result['result'][i]['years']),m=3)
                # print('%d) after : %s' % (i,dates))
                dates=dates.tolist()

                # print(type(dates))
                dates.sort(key=int)

                # result['result'][i]['dates']=dates
                try:
                    cat_mean.append(curr_date.year-dates[0])
                except IndexError:
                    pass

            return {'ok':1, 'result': numpy.median(cat_mean)}
        else:
            return result



    def GetCatMeanAge(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url':{'$nin':['',None]}}},
            {'$match': {'extras.date_released':{'$type':9}}},
            { '$group': {'_id' : '$catalogue_url', 'years': {'$push': {'$year': '$extras.date_released'}},
                'counter': {'$sum': 1}}},
            {'$sort': { 'counter':-1}},
            # {'$limit': 10}
            ])

        if result['ok']==1:
            curr_date=datetime.now()
            cat_mean=[]
            for i in range(0,len(result['result'])):
                # cat_mean.append({'catalogue':result['result'][i]['_id'],'mean_age':numpy.mean(result['result'][i]['years'])})
                # dates=result['result'][i]['years']
                dates=self.__reject_outliers(numpy.array(result['result'][i]['years']),m=3)
                dates=dates.tolist()

                # print(type(dates))
                dates.sort(key=int)

                # result['result'][i]['dates']=dates
                try:
                    cat_mean.append(curr_date.year-dates[0])
                except IndexError:
                    pass

            # return {'ok':1, 'result': result['result']}
            return {'ok':1, 'result': numpy.mean(cat_mean)}
        else:
            return result


    def GetCatNewMonthFreq(self, conn, args, version = None, db = None, collection = None):
        # months={1: 'January',2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 7: 'July', 8: 'August', 9: 'September',
        #         10: 'October', 11: 'November', 12: 'December'}
        end_date=datetime.now()
        if "end_date" in args:
            try:
                end_date=datetime.strptime(args['end_date'][0], '%Y-%m-%d')
            except ValueError as e:
                print (e)
        # start_date=end_date + relativedelta(years=-1)
        start_date=datetime(1900,1,1,0,0,0)
        if "start_date" in args:
            try:
                start_date=datetime.strptime(args['start_date'][0], '%Y-%m-%d')
            except ValueError as e:
                print (e)

        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url':{'$nin':['',None]}}},
            {'$match': {'extras.date_released':{'$type':9}}},
            {'$match': {'extras.date_released': {'$gt': start_date, '$lte': end_date}}},
            { '$group': {'_id' : '$catalogue_url', 'dates': {'$push': '$extras.date_released'}, 'counter': {'$sum': 1}}},
            {'$sort': { 'counter':-1}},
            # {'$limit': 12}
            ])

        if result['ok']==1:
            date_catalogues=[]
            d=defaultdict(list)
            for i in range(0,len(result['result'])):
                # cat_date=min(result['result'][i]['dates'])
                # # cat_initiated({datetime.date(year=cat_date.year,month=cat_date=month): result['result'][i]['_id']})
                # d[date(cat_date.year,cat_date.month,1)].append(result['result'][i]['_id'])

                dates=result['result'][i]['dates']

                years=[t.year for t in dates]
                years=self.__reject_outliers(numpy.array(years),m=2)
                years=years.tolist()
                years.sort(key=int)

                if years:
                    cat_date=None
                    dates.sort()
                    for j in dates:
                        if j.year==years[0]:
                            cat_date=j
                            break

                    d[date(cat_date.year,cat_date.month,1)].append(result['result'][i]['_id'])

            i=date(end_date.year,end_date.month,1)
            while i > date(start_date.year,start_date.month,1):
                if len(d[i])>0:
                    date_catalogues.append({'month':str(i),'catalogues': d[i], 'counter': len(d[i])})
                    # date_catalogues({'date': datetime.fromtimestamp(cat_initiated['first_dataset']/1e3),'catalogues': {cat_initiated}})
                i-=relativedelta(months=1)

            return {'ok':1, 'result': date_catalogues}
        else:
            return result



    def GetCatDistribsFreq(self, conn, args, version = None, db = None, collection = None, limit= True):
        # result=conn[db][collection].aggregate([
        #     {'$match': {'resources':{'$exists':True}}},
        #     { '$group': {'_id' : 0, 'counter': {'$sum': {'$size': '$resources'}}}},
        #     {'$sort': { 'counter':-1}},
        #     {'$project': {'_id':0,'counter':1}}
        #     ])
        #
        # return result
        #
        # if result['ok']==1:
        #     cat_mean=[]
        #     for i in range(0,len(result['result'])):
        #         cat_mean.append({'catalogue':result['result'][i]['_id'],'mean_age':numpy.mean(result['result'][i]['years'])})
        #
        #     return {'ok':1, 'result': cat_mean}
        # else:
            # return result
        start_time=time.time()

        match={'$and':[{'extras.date_released':{'$type':9}}]}
        end_date=datetime.now()
        if "end_date" in args:
            try:
                end_date=datetime.strptime(args['end_date'][0], '%Y-%m-%d')
                match['$and'].append({'extras.date_released':{'$lte':end_date}})
            except ValueError as e:
                print (e)
        start_date=end_date + relativedelta(years=-1)
        if "start_date" in args:
            try:
                start_date=datetime.strptime(args['start_date'][0], '%Y-%m-%d')
                match['$and'].append({'extras.date_released':{'$gte':start_date}})
            except ValueError as e:
                print (e)

        if not limit:
            result=conn[db][collection].aggregate([
                # {'$match': {'resources.0': {'$exists': True}}},
                {'$match': {'catalogue_url': {'$nin': ['', None]},
                    'is_duplicate':{'$ne':True},
                    }},
                {'$unwind': '$resources'},
                { '$group': { '_id': '$catalogue_url', 'counter': {'$sum': 1}}},
                # { '$group': { '_id': '$catalogue_url', 'distrib_size': {'$push':1 }}},
                # { '$unwind':"$distrib_size" },
                # { '$group' : {'_id' : "$_id", 'counter' : {'$sum' : 1} } },
                ])
        else:
            result=conn[db][collection].aggregate([
                {'$match': {'catalogue_url': {'$nin': ['', None]},
                    'is_duplicate':{'$ne':True},
                    }},
                {'$match': match},
                {'$unwind': '$resources'},
                { '$group': { '_id': {'year':{'$year':'$extras.date_released'},'month':{'$month':'$extras.date_released'}},
                        'catalogues':{'$push':'$catalogue_url'}, 'counter': {'$sum': 1}}},
                {'$sort':{'_id.year':-1,'_id.month':-1}}
                ])


        if result['ok']==1:
            date_catalogues=[]
            d=defaultdict(list)
            for i in range(0,len(result['result'])):
                cat_dict={}
                for cat in result['result'][i]['catalogues']:
                    try:
                        cat_dict[cat]+=1
                    except KeyError:
                        cat_dict[cat]=1

                result['result'][i]['catalogues']=cat_dict
                result['result'][i]['date']=str(date(result['result'][i]['_id']['year']
                        ,result['result'][i]['_id']['month'],1))
                del result['result'][i]['counter']
                del result['result'][i]['_id']

            print (time.time() - start_time)

            return {'ok':1, 'result': result['result']}
        else:
            return result



    def GetCatDistribsTotFreq(self, conn, args, version = None, db = None, collection = None, limit= True):
        match_dict =  {'catalogue_url': {'$nin': ['', None]},
                'extras.unpublished':{'$ne':"true"},
                'is_duplicate':{'$ne':True}
                }

        if 'include_dupls' in args:
            dupl_val = args['include_dupls'][0]
            if dupl_val.lower() in ['true','1']:
                del match_dict['is_duplicate']

        result=conn[db][collection].aggregate([
            {'$match': match_dict},
            {'$match': {'resources':{'$exists':True}}},
            { '$group': {'_id' : '$catalogue_url', 'count': {'$sum': {'$size': '$resources'}}}},
            {'$sort':{'count':-1}},
        ])

        return result



    def GetCDDistribsFreq(self, conn, args, version = None, db = None, collection = None, limit= True):
        result=conn[db][collection].aggregate([
            {'$match': {'country': {'$nin': ['', None]},
                'is_duplicate':{'$ne':True},
                }},
            {'$match': {'resources':{'$exists':True}}},
            { '$group': {'_id' : '$country', 'freq': {'$sum': {'$size': '$resources'}}}},
            {'$sort':{'freq':-1}},
        ])

        return result



    def GetCatDistribs(self, conn, args, version = None, db = None, collection = None, limit= True):
        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url': {'$nin': ['', None]},
                'is_duplicate':{'$ne':True},
                }},
            {'$match': {'resources':{'$exists':True}}},
            { '$group': {'_id' : 0, 'freq': {'$sum': {'$size': '$resources'}}}},
            {'$sort':{'freq':-1}},
            {'$project': {'_id':0,'freq':1}}
        ])

        return result



    def GetCatDataSizeTotal(self, conn, args, version = None, db = None, collection = None):
        match_dict =  {'catalogue_url': {'$nin': ['', None]},
                'extras.unpublished':{'$ne':"true"},
                'is_duplicate':{'$ne':True}
                }

        if 'include_dupls' in args:
            dupl_val = args['include_dupls'][0]
            if dupl_val.lower() in ['true','1']:
                del match_dict['is_duplicate']

        result=conn[db][collection].aggregate([
            {'$match': match_dict},
            # {'$match': {'resources.0': {'$exists': True}}},
            {'$unwind': "$resources" },
            # {'$match': {'resources.size':{'$nin':['',None]}}},
            { '$group': {'_id' : '$catalogue_url', 'size': {'$sum': '$resources.size'}, 'count': {'$sum': 1}}},
            {'$sort': {'size': -1}},
            {'$project': {'_id': 1, 'count': {'$divide': ['$size',1024]}}}
            ])

        return result


    def GetEDDistribSize(self, conn, args, version = None, db = None, collection = None):
        match_dict =  {'catalogue_url': {'$nin': ['', None]},
                'extras.unpublished':{'$ne':"true"},
                'is_duplicate':{'$ne':True}
                }

        if 'include_dupls' in args:
            dupl_val = args['include_dupls'][0]
            if dupl_val.lower() in ['true','1']:
                del match_dict['is_duplicate']

        result=conn[db][collection].aggregate([
            {'$match': match_dict},
            {'$unwind': "$resources" },
            { '$group': {'_id' : 0, 'size': {'$sum': '$resources.size'}, 'freq': {'$sum': 1}}},
            {'$sort': {'freq': -1}},
            {'$project': {'_id': 0, 'size': {'$divide': ['$size',1024]},'freq':1}}
            ])

        return result


    def GetCDDistribSize(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$match': {'country': {'$nin': ['', None]},
                'is_duplicate':{'$ne':True},
                }},
            {'$unwind': "$resources" },
            { '$group': {'_id' : '$country', 'size': {'$sum': '$resources.size'}, 'freq': {'$sum': 1}}},
            {'$sort': {'freq': -1}},
            {'$project': {'_id': 1, 'size': {'$divide': ['$size',1024]},'freq':1}}
            ])

        return result


    def GetCatMedianDatasize(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url':{'$nin':['',None]}}},
            # {'$match': {'resources.0': {'$exists': True}}},
            {'$unwind': "$resources" },
            # {'$match': {'resources.size':{'$nin':['',None]}}},
            { '$group': {'_id' : '$catalogue_url', 'median_size': {'$push': '$resources.size'}, 'counter': {'$sum': 1}}},
            {'$sort': {'counter': -1}},
            # {'$limit': 10}
            ])

        if result['ok']==1:
            cat_median=[]
            for i in range(0,len(result['result'])):
                values=filter(None, result['result'][i]['median_size'])
                values=[int(j) for j in values if str(j).isdigit()]
                if len(values)>0:
                    cat_median.append({'_id': result['result'][i]['_id'], 'counter':numpy.median(values)/float(1<<10)
                        # ,'counter': len(values)
                        })
                # result['result'][i]['median_size']=numpy.median(result['result'][i]['median_size'])/float(1<<10)

            return {'ok':1, 'result': cat_median}
        else:
            return result


    def GetCatMaxDatasize(self, conn, args, version = None, db = None, collection = None):
        start_time = time.time()

        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url':{'$nin':['',None]}}},
            #{'$match': {'resources.0': {'$exists': True}}},
            {'$unwind': "$resources" },
            #{'$match': {'resources.size':{'$nin':['',None]}}},
            { '$group': {'_id' : '$catalogue_url', 'max_size': {'$push': '$resources.size'}, 'counter': {'$sum': 1}}},
            {'$sort': {'counter': -1}},
            # {'$limit': 10}
            ])

        # return result

        if result['ok']==1:
            maxdatasize=[]
            for i in range(0,len(result['result'])):
                values=filter(None, result['result'][i]['max_size'])
                values=[int(j) for j in values if str(j).isdigit()]
                # result['result'][i]['max_size']=max(result['result'][i]['max_size'])/float(1<<10)
                if len(values)>0:
                    try:
                        maxdatasize.append({'_id': result['result'][i]['_id'], 'counter':max(values)/float(1<<10)
                            # ,'counter': len(values)
                            })
                    except TypeError as e:
                        print (result['result'][i]['_id'], e)

            print (time.time() - start_time)

            # return {'ok':1, 'result': result}
            return {'ok':1, 'result': maxdatasize}
        else:
            return result


    def GetCatStddevDatasize(self, conn, args, version = None, db = None, collection = None):
        start_time = time.time()

        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url':{'$nin':['',None]}}},
            #{'$match': {'resources.0': {'$exists': True}}},
            {'$unwind': "$resources" },
            #{'$match': {'resources.size':{'$nin':['',None]}}},
            { '$group': {'_id' : '$catalogue_url', 'stddev_size': {'$push': '$resources.size'}, 'counter': {'$sum': 1}}},
            {'$sort': {'counter': -1}},
            # {'$limit': 10}
            ])

        if result['ok']==1:
            stddev=[]
            for i in range(0,len(result['result'])):
                values=filter(None, result['result'][i]['stddev_size'])
                values=[int(j) for j in values if str(j).isdigit()]
                if len(values)>0:
                    stddev.append({'_id': result['result'][i]['_id'], 'counter':numpy.std(values)/float(1<<10)
                        # ,'counter': len(values)
                        })
                # result['result'][i]['stddev_size']=numpy.std(result['result'][i]['stddev_size'])/float(1<<10)

            print (time.time() - start_time)

            return {'ok':1, 'result': stddev}
        else:
            return result

    def GetCatMeanDatasize(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url':{'$nin':['',None]}}},
            # {'$match': {'resources.0': {'$exists': True}}},
            {'$unwind': "$resources" },
            # {'$match': {'resources.size':{'$nin':['',None]}}},
            { '$group': {'_id' : '$catalogue_url', 'mean_size': {'$push': '$resources.size'}, 'counter': {'$sum': 1}}},
            {'$sort': {'counter': -1}},
            # {'$limit': 10}
            ])

        if result['ok']==1:
            cat_mean=[]
            for i in range(0,len(result['result'])):
                values=filter(None, result['result'][i]['mean_size'])
                values=[int(j) for j in values if str(j).isdigit()]
                if len(values)>0:
                    cat_mean.append({'_id': result['result'][i]['_id'], 'counter':numpy.mean(values)/float(1<<10)
                        # ,'counter': len(values)
                        })
                # result['result'][i]['mean_size']=numpy.mean(result['result'][i]['mean_size'])/1e3

            return {'ok':1, 'result': cat_mean}
        else:
            return result


    def GetCatMachineFormatFreq(self, conn, args, version = None, db = None, collection = None):
        machine_readable=def_formatLists.ODM_formats().get_machine_readable()

        start_time=time.time()

        match_dict =  {'catalogue_url': {'$nin': ['', None]},
                'extras.unpublished':{'$ne':"true"},
                'is_duplicate':{'$ne':True}
                }

        if 'include_dupls' in args:
            dupl_val = args['include_dupls'][0]
            if dupl_val.lower() in ['true','1']:
                del match_dict['is_duplicate']

        result=conn[db][collection].find(
                match_dict
            ,
            {'_id':0,'catalogue_url':1,'resources.format':1},
            )
        # aggregate([
        #     {'$match': {'catalogue_url': {'$nin': ['', None]},
        #         'is_duplicate':{'$ne':True},
        #         }},
        #     # { '$unwind': "$resources" },
        #     { '$group': { '_id': "$_id", 'cat':{'$addToSet':'$catalogue_url'},
        #         'resources': {'$push': '$resources.format'}, 'freq': { '$sum': 1 } } },
            # { '$sort': { 'freq': -1 } },
            # ])

        total_freqs=[]
        if result.count()>0:
            country_format_freq={}
            tot_counter={}
            for _dd in result:
                format_freq=0
                # format_freq={'CSV':0, 'TSV':0, 'JSON':0, 'XML':0, 'RDF':0}
                resource_size=0
                if 'resources' in _dd:
                    resource_size=len(_dd['resources'])
                if resource_size>0:
                    for  _res in _dd['resources']:
                        try:
                            if 'format' in _res and \
                                    str(_res['format']).lower() in machine_readable:
                                # format_freq[str(result['result'][i]['resources'][j]).upper()]+=1
                                format_freq=1
                                break
                        except AttributeError,e:
                            print(e)
                        except KeyError,e:
                            print(e,_dd['_id'])

                country_name=_dd['catalogue_url']
                if country_name in country_format_freq:
                    country_format_freq[country_name]+=format_freq
                    tot_counter[country_name]+=1
                else:
                    country_format_freq[country_name]=format_freq
                    tot_counter[country_name]=1

                # total_freqs.append({'_id': result['result'][i]['_id'], 'freq': format_freq})
            for cn in country_format_freq:
                total_freqs.append({'_id':cn,'count': country_format_freq[cn],
                    'tot_count':tot_counter[cn]})

            print (time.time() - start_time)
            return {'ok':1, 'result': total_freqs}
        else:
            return result


    def GetEDMachineReadFormatFreq(self, conn, args, version = None, db = None, collection = None):
        machine_readable=def_formatLists.ODM_formats().get_machine_readable()

        start_time=time.time()

        match_dict =  {'catalogue_url': {'$nin': ['', None]},
                'extras.unpublished':{'$ne':"true"},
                'is_duplicate':{'$ne':True}
                }

        if 'include_dupls' in args:
            dupl_val = args['include_dupls'][0]
            if dupl_val.lower() in ['true','1']:
                del match_dict['is_duplicate']

        result=conn[db][collection].find(
                match_dict,
            {
                '_id':0,'resources.format':1},
            )

        total_freqs=[]
        if result.count()>0:
            format_freq=0
            tot_counter=0
            for _dd in result:
                # format_freq={'CSV':0, 'TSV':0, 'JSON':0, 'XML':0, 'RDF':0}
                resource_size=0
                if 'resources' in _dd:
                    resource_size=len(_dd['resources'])
                if resource_size>0:
                    for  _res in _dd['resources']:
                        try:
                            if 'format' in _res and \
                                    str(_res['format']).lower() in machine_readable:
                                format_freq+=1
                                break
                        except AttributeError,e:
                            print(e)
                tot_counter+=1

                # total_freqs.append({'_id': result['result'][i]['_id'], 'freq': format_freq})
            total_freqs.append({'freq': format_freq/tot_counter})

            print (time.time() - start_time)
            return {'ok':1, 'result': total_freqs}
        else:
            return result


    def GetCDMachineReadFormatFreq(self, conn, args, version = None, db = None, collection = None):
        machine_readable=def_formatLists.ODM_formats().get_machine_readable()

        start_time=time.time()

        result=conn[db][collection].aggregate([
            {'$match': {'country': {'$nin': ['', None]},
                'is_duplicate':{'$ne':True},
                'extras.unpublished':{'$ne':"true"},
                }},
            # { '$unwind': "$resources" },
            { '$group': { '_id': "$_id", 'country':{'$addToSet':'$country'},
                'resources': {'$push': '$resources.format'}, 'freq': { '$sum': 1 } } },
            # { '$sort': { 'freq': -1 } },
            ])

        total_freqs=[]
        if result['ok']==1:
            country_format_freq={}
            tot_counter={}
            for i in range(0,len(result['result'])):
                format_freq=0
                # format_freq={'CSV':0, 'TSV':0, 'JSON':0, 'XML':0, 'RDF':0}
                resource_size=len(result['result'][i]['resources'])
                if resource_size>0:
                    for  j in range(0,len(result['result'][i]['resources'][0])):
                        if str(result['result'][i]['resources'][0][j].encode('utf-8')).lower() in machine_readable:
                            # format_freq[str(result['result'][i]['resources'][j]).upper()]+=1
                            format_freq=1
                            break

                country_name=result['result'][i]['country'][0]
                if country_name in country_format_freq:
                    country_format_freq[country_name]+=format_freq
                    tot_counter[country_name]+=1
                else:
                    country_format_freq[country_name]=format_freq
                    tot_counter[country_name]=1

                # total_freqs.append({'_id': result['result'][i]['_id'], 'freq': format_freq})
            for cn in country_format_freq:
                total_freqs.append({'_id':cn,'freq': country_format_freq[cn]/tot_counter[cn]})

            print (time.time() - start_time)
            return {'ok':1, 'result': total_freqs}
        else:
            return result


    def GetCatMachineFormatProp(self, conn, args, version = None, db = None, collection = None):
        start_time=time.time()

        # total_distrbs_per_catalogue=self.GetCatDistribsFreq(conn, args,version,db,collection,False)
        result=conn[db][collection].aggregate([
            { '$match': { 'catalogue_url': { '$nin': ['', None] }}},
            # { '$match': { 'resources.0': { '$exists': True } } },
            { '$unwind': "$resources" },
            # { '$match': { 'resources.format': { '$in': [ 'CSV', 'TSV', 'JSON', 'XML', 'RDF' ] } }} ,
            { '$group': { '_id': "$catalogue_url", 'resources': {'$push': '$resources.format'}, 'counter': { '$sum': 1 } } },
            { '$sort': { 'counter': -1 } },
            # {'$limit': 10}
            ])

        total_freqs=[]
        if result['ok']==1:
            for i in range(0,len(result['result'])):
                format_freq={'CSV':0, 'TSV':0, 'JSON':0, 'XML':0, 'RDF':0}
                for j in range(0,len(result['result'][i]['resources'])):
                    if result['result'][i]['resources'][j].encode('utf-8').upper() in format_freq:
                        format_freq[result['result'][i]['resources'][j].upper()]+=1

                # cat_distributions=0
                # for k in range(0,len(total_distrbs_per_catalogue['result'])):
                #     if total_distrbs_per_catalogue['result'][k]['_id']==result['result'][i]['_id']:
                #         cat_distributions=total_distrbs_per_catalogue['result'][k]['counter']
                #         del total_distrbs_per_catalogue['result'][k]
                #         break

                cat_distributions=len(result['result'][i]['resources'])
                for key in format_freq:
                    format_freq[key]=format_freq[key]*100.00/cat_distributions

                total_freqs.append({'_id': result['result'][i]['_id'], 'formats': format_freq})

            print (time.time() - start_time)
            return {'ok':1, 'result': total_freqs}
        else:
            return result


    def GetCatMimeFreq(self, conn, args, version = None, db = None, collection = None):
        start_time=time.time()

        result=conn[db][collection].aggregate([
            # {'$match': {'resources.0': {'$exists': True}}},
            {'$match': {'catalogue_url': {'$nin': ['',None]}}},
            {'$unwind':'$resources'},
            # {'$match': {'resources.mimetype': {'$nin': ['',None]}}},
            { '$group': {'_id': '$catalogue_url', 'resources': {'$push': '$resources.mimetype' },'counter' : {'$sum' : 1}}},
            { '$sort': { 'counter': -1 } },
            # {'$limit': 10}
            ])

        total_freqs=[]
        if result['ok']==1:
            for i in range(0,len(result['result'])):
                dict_mimetypes={}
                for j in range(0,len(result['result'][i]['resources'])):
                    mime_key=result['result'][i]['resources'][j]
                    if mime_key!='':
                        if mime_key in dict_mimetypes:
                            dict_mimetypes[mime_key]+=1
                        else:
                            dict_mimetypes[mime_key]=1

                sorted_mimes=OrderedDict()
                for key, value in sorted(dict_mimetypes.iteritems(), key=lambda (k,v): (v,k),reverse=True):
                    sorted_mimes[key]=value
                total_freqs.append({'_id':result['result'][i]['_id'],'mimetypes': sorted_mimes})

            print (time.time() - start_time)
            return {'ok':1, 'result': total_freqs}
        else:
            return result

    def GetCatMimeProp(self, conn, args, version = None, db = None, collection = None):
        # total_distrbs_per_catalogue=self.GetCatDistribsFreq(conn, args,version,db,collection,False)
        result=conn[db][collection].aggregate([
            {'$match': {'catalogue_url': {'$nin': ['',None]}}},
            # {'$match': {'resources.0': {'$exists': True}}},
            {'$unwind':'$resources'},
            # {'$match': {'resources.mimetype': {'$nin': ['',None]}}},
            { '$group': {'_id': '$catalogue_url', 'resources': {'$push': '$resources.mimetype' },'counter' : {'$sum' : 1}}},
            { '$sort': { 'counter': -1 } },
            # {'$limit': 10}
            ])

        total_freqs=[]
        if result['ok']==1:
            for i in range(0,len(result['result'])):
                dict_mimetypes={}
                for j in range(0,len(result['result'][i]['resources'])):
                    mime_key=result['result'][i]['resources'][j]
                    if mime_key!='':
                        if mime_key in dict_mimetypes:
                            dict_mimetypes[mime_key]+=1
                        else:
                            dict_mimetypes[mime_key]=1

                # cat_distributions=0
                # for k in range(0,len(total_distrbs_per_catalogue['result'])):
                #     if total_distrbs_per_catalogue['result'][k]['_id']==result['result'][i]['_id']:
                #         cat_distributions=total_distrbs_per_catalogue['result'][k]['counter']
                #         del total_distrbs_per_catalogue['result'][k]
                #         break

                cat_distributions=len(result['result'][i]['resources'])
                sorted_mimes=OrderedDict()
                for key, value in sorted(dict_mimetypes.iteritems(), key=lambda (k,v): (v,k),reverse=True):
                    sorted_mimes[key]=value*100.00/cat_distributions
                total_freqs.append({'_id':result['result'][i]['_id'],'mimetypes': sorted_mimes})

            return {'ok':1, 'result': total_freqs}
        else:
            return result

    def Getcatgeofreq(self, conn, args, version = None, db = None, collection = None):
        EXCLUDE_COUNTRIES=re.compile('\.eu|datahub\.io')
        result=conn[db][collection].aggregate([
            {'$match': {'country': {'$nin': ['',None]},
                'catalogue_url':{'$not':EXCLUDE_COUNTRIES},
                # 'is_duplicate':{'$ne':True},
                # '$or':[{'is_duplicate':False},{'is_duplicate':{'$exists':False}}]
                }},
            # {'$match': {'catalogue_url':{'$not':EXCLUDE_COUNTRIES}}},
            {'$group': {'_id':'$country','cat_urls':{'$addToSet':'$catalogue_url'}}},
            {'$unwind':'$cat_urls'},
            {'$group': {'_id':'$_id','counter':{'$sum':1},'catalogues':{'$addToSet':'$cat_urls'}}},
            {'$sort':{'counter':-1}}
            ])

        return result

    def Getcatcapitafreq(self, conn, args, countryStats, version = None, db = None, collection = None):
        EXCLUDE_COUNTRIES=re.compile('\.eu|datahub\.io')
        results=conn[db][collection].aggregate([
            {'$match': {'country': {'$nin': ['',None]}}},
            {'$match': {'catalogue_url':{'$not':EXCLUDE_COUNTRIES},
                '$or':[{'extras.date_released':{'$type':9}},{'extras.date_updated':{'$type':9}}],
                }},
            {'$group': {'_id':'$catalogue_url','country':{'$addToSet':'$country'},
                'date_release':{'$push':'$extras.date_released'},'date_update':{'$push':'$extras.date_updated'}}},
            # {'$group': {'_id':'$country','catalogues':{'$addToSet':'$catalogue_url'}}},
            # {'$unwind':'$catalogues'},
            # {'$group': {'_id':'$_id','counter':{'$sum':1}}},
            # {'$sort':{'counter':-1}}
         ])

        if results['ok']==1:
            # total_freq=[]
            datedict=dict()
            countrydict = defaultdict(list)
            min_val=1
            max_val=0
            min_date=date.today().timetuple().tm_year
            for result in results['result']:
                dates1=[x for x in result['date_release'] if isinstance(x,date)]
                dates2=[x for x in result['date_update'] if isinstance(x,date)]
                dates=dates1 \
                        if len(dates1) >= len(dates2) \
                        else dates2

                years=[t.year for t in dates]
                years=self.__reject_outliers(numpy.array(years),m=2)
                years=years.tolist()
                years.sort(key=int)

                if years:
                    cat_date=None
                    dates.sort()
                    for j in dates:
                        if j.year==years[0]:
                            cat_date=j.year
                            if min_date > j.year:
                                min_date=j.year

                    countrydict[result['country'][0]].append(cat_date)

            for key,val in countrydict.iteritems():
                try:
                    print(key,val)
                    # print(min_date)
                    for dt_key,pop_val in countryStats[key]['population'].iteritems():
                        if min_date > int(dt_key):
                            continue
                        if dt_key not in datedict.keys():
                            datedict[dt_key]=dict()
                        curr_val=(sum(x<=int(dt_key) for x in val))/(pop_val*1.00)
                        datedict[dt_key][key]=curr_val
                        if min_val > curr_val:
                            min_val = curr_val
                        elif max_val < curr_val:
                            max_val = curr_val

                # try:
                #     for _dt in countryStats[result['_id']]['population']:
                #         if _dt not in datedict:
                #             datedict[_dt]=dict()
                #         curr_val=result['counter']/(countryStats[result['_id']]['population'][_dt]*1.00)
                #         datedict[_dt][result['_id']]=curr_val
                #         if min_val > curr_val:
                #             min_val = curr_val
                #         elif max_val < curr_val:
                #             max_val = curr_val

                    # if countryStats[result['_id']]['population']!=0:
                    #     total_freq.append({'country':result['_id'],
                    #         'freq':result['counter']/(countryStats[result['_id']]['population']*1.00)})
                    # else:
                    #     print('no population for country: %s' % result['_id'])
                except KeyError as e:
                    print('No country in db with country_code %s' % result['_id'])

            total=[]
            sorted_datedict=(sorted(datedict.items(),key=operator.itemgetter(0)))
            sorted_datedict.reverse()
            for _dt in sorted_datedict:
                dictlist = []
                for key,value in _dt[1].iteritems():
                    # dictlist.append(value)
                    _dt[1][key]=self.__normalize_values(value,min_val,max_val,0,100)
                total.append({'year':_dt[0],'perCapita':_dt[1]})

            return {'ok':1,'result':total}
        else:
            return results


    def Getcatgdpcorr(self, conn, args, countryStats, version = None, db = None, collection = None):
        EXCLUDE_COUNTRIES=re.compile('\.eu|datahub\.io')
        results=conn[db][collection].aggregate([
            {'$match': {'country': {'$nin': ['',None]}}},
            {'$match': {'catalogue_url':{'$not':EXCLUDE_COUNTRIES},
                '$or':[{'extras.date_released':{'$type':9}},{'extras.date_updated':{'$type':9}}],
                }},
            {'$group': {'_id':'$catalogue_url','country':{'$addToSet':'$country'},
                'date_release':{'$push':'$extras.date_released'},'date_update':{'$push':'$extras.date_updated'}}},
            # {'$group': {'_id':'$country','no_catalogues':{'$addToSet':'$catalogue_url'}}},
            # {'$unwind':'$no_catalogues'},
            # {'$group': {'_id':'$_id','counter':{'$sum':1}}},
            # {'$sort':{'counter':-1}}
        ])

        if results['ok']==1:
            # total_freq=[]
            # datedict={}
            countrydict = defaultdict(list)
            min_date=date.today().timetuple().tm_year
            for result in results['result']:
                dates1=[x for x in result['date_release'] if isinstance(x,date)]
                dates2=[x for x in result['date_update'] if isinstance(x,date)]
                dates=dates1 \
                        if len(dates1) >= len(dates2) \
                        else dates2

                years=[t.year for t in dates]
                years=self.__reject_outliers(numpy.array(years),m=2)
                years=years.tolist()
                years.sort(key=int)

                if years:
                    cat_date=None
                    dates.sort()
                    for j in dates:
                        if j.year==years[0]:
                            cat_date=j.year
                            if min_date > j.year:
                                min_date=j.year

                    countrydict[result['country'][0]].append(cat_date)

            catalogues_evolve=defaultdict(list)
            gdp_evolve=defaultdict(list)
            for key,val in countrydict.iteritems():
                try:
                    print(key,val)
                    # print(min_date)
                    for gdp_key,gdp_value in countryStats[key]['GDP'].iteritems():
                        if min_date > int(gdp_key):
                            continue
                        # if dt_key not in datedict.keys():
                        #     datedict[dt_key]=dict()
                        curr_val=sum(x<=int(gdp_key) for x in val)
                        catalogues_evolve[key].append(curr_val)
                        gdp_evolve[key].append(gdp_value)

                # try:
                #     for _dt in countryStats[result['_id']]['GDP']:
                #         if _dt not in datedict:
                #             datedict[_dt]=dict()
                #         datedict[_dt][result['_id']]=result['counter']/(countryStats[result['_id']]['GDP'][_dt]*1.00)
                #
                #     # if countryStats[result['_id']]['GDP']!=0:
                #     #     total_freq.append({'country':result['_id'],
                #     #         'freq':result['counter']/(countryStats[result['_id']]['GDP']*1.00)})
                #     # else:
                #     #     print('no GDP value for country: %s' % result['_id'])
                except KeyError as e:
                    print('No country in db with country_code %s' % result['_id'])

            rp=[]
            for country in catalogues_evolve:
                (r,p) = pearsonr(catalogues_evolve[country],gdp_evolve[country])
                rp.append({country:{'r':r,'p':p}})

            # total=[]
            # sorted_datedict=(sorted(datedict.items(),key=operator.itemgetter(0)))
            # sorted_datedict.reverse()
            # for _dt in sorted_datedict:
            #     total.append({'year':_dt[0],'corr':_dt[1]})

            return {'ok':1,'result':rp}
        else:
            return results


    def Getcathdicorr(self, conn, args, countryStats, version = None, db = None, collection = None):
        EXCLUDE_COUNTRIES=re.compile('\.eu|datahub\.io')
        results=conn[db][collection].aggregate([
            {'$match': {'country': {'$nin': ['',None]}}},
            {'$match': {'catalogue_url':{'$not':EXCLUDE_COUNTRIES}}},
            {'$group': {'_id':'$country','no_catalogues':{'$addToSet':'$catalogue_url'}}},
            {'$unwind':'$no_catalogues'},
            {'$group': {'_id':'$_id','counter':{'$sum':1}}},
            {'$sort':{'counter':-1}}
            ])

        if results['ok']==1:
            datedict={}
            for result in results['result']:
                try:
                    for _dt in countryStats[result['_id']]['HDI']:
                        if _dt not in datedict:
                            datedict[_dt]=dict()
                        datedict[_dt][result['_id']]=result['counter']/(countryStats[result['_id']]['HDI'][_dt]*1.00)

                    # if countryStats[result['_id']]['HDI']!=0:
                    #     total_freq.append({'country':result['_id'],
                    #         'freq':result['counter']/(countryStats[result['_id']]['HDI']*1.00)})
                    # else:
                    #     print('no HDI value for country: %s' % result['_id'])
                except KeyError as e:
                    print('No country in db with country_code %s' % result['_id'])

            total=[]
            sorted_datedict=(sorted(datedict.items(),key=operator.itemgetter(0)))
            sorted_datedict.reverse()
            for _dt in sorted_datedict:
                total.append({'year':_dt[0],'corr':_dt[1]})

            return {'ok':1,'result':total}
        else:
            return results


    def Getpublishers(self, conn, args, version = None, db = None, collection = None):
        start_time=time.time()

        desc=False
        if 'desc' in args:
            desc=True if args['desc'][0].strip()=='1' else False

        result=conn[db][collection].aggregate([
             {'$match':{'organization.title':{'$nin':['',None]}}},
             {'$group':{'_id':'$organization.title', 'count':{'$sum':1},
                 'description':{'$addToSet':'$organization.description'}}},
             {'$unwind':'$description'},
             {'$group':{'_id':'$count','organizations':{'$addToSet':'$_id'},'description':{'$push':'$description'}}},
             {'$sort':{'_id':-1}},
             {'$project':{'_id':0,'no':'$_id','organizations':1,'description':1}}
        ])

        if result['ok']==1:
            total_freq=[]
            # dist_cats=OrderedDict()
            check_list=[]
            for metadata in result['result']:
                for publisher in metadata['organizations']:
                    # u_theme=publisher.encode('utf-8')
                    try:
                        check_list.index(publisher.strip())
                        print('Already existed publisher: %s' % publisher.strip())
                    except ValueError:
                        # dist_cats[publisher.strip()]=int(metadata['no'])
                        if desc:
                            clean_desc=filter(None,metadata['description'])
                            total_freq.append({'publisher':publisher.strip(),'counter':metadata['no'],'desc':clean_desc})
                        else:
                            total_freq.append({'publisher':publisher.strip(),'counter':metadata['no']})

                        check_list.append(publisher.strip())

            print(time.time()-start_time)
            return {'ok':1,'result':total_freq}
        else:
            return result


    def Getcategories(self, conn, args, version = None, db = None, collection = None):
        # EXCLUDE_COUNTRIES=re.compile('\.eu|datahub\.io')
        try:
            match_dict =  {'catalogue_url': {'$nin': ['', None]},
                    'extras.unpublished':{'$ne':"true"},
                    # 'catalogue_url':{'$not':EXCLUDE_COUNTRIES}}
                    'catalogue_url':{'$nin':[
                        # 'https://www.dati.lombardia.it'
                        # ,'https://opendata.bristol.gov.uk'
                        # ,'https://gavaobert.gavaciutat.cat'
                        # ,'http://portal.openbelgium.be/'
                        # ,'https://data.eindhoven.nl'
                    ]},
                    'is_duplicate':{'$ne':True}
                    }

            if 'include_dupls' in args:
                dupl_val = args['include_dupls'][0]
                if dupl_val.lower() in ['true','1']:
                    del match_dict['is_duplicate']

            result=conn[db][collection].aggregate([
                {'$match': match_dict},
                {'$unwind':'$category' },
                {'$group':{'_id':'$catalogue_url', 'themes':{'$push':'$category'},'count':{'$sum':1}}},
                {'$sort':{'count':-1}}
            ])
        except OperationFailure as e:
            return e.details

        if result['ok']==1:
            total_freq=[]
            # dist_cats=OrderedDict()
            for metadata in range(0,len(result['result'])):
                theme_counter={}
                for theme in result['result'][metadata]['themes']:
                    if theme in theme_counter:
                        theme_counter[theme]+=1
                    else:
                        theme_counter[theme]=1

                result['result'][metadata]['category']=(theme_counter)
                del result['result'][metadata]['themes']

            return result
        else:
            return result


    def Getdspopulatedmdfields(self, conn, args, version = None, db = None, collection = None):
        batch_size = 15
        if 'batch_size' in args:
            batch_size = int(args['batch_size'][0])

        skip = 0
        if 'offset' in args:
            try:
                skip = int(args['offset'][0]) if int(args['offset'][0])>0 else 0
            except ValueError:
                pass
        elif 'page' in args:
            try:
                skip = batch_size * (int(args['page'][0])-1 if int(args['page'][0])>0 else 0)
            except ValueError:
                pass

        odm_id=None
        counter=1
        criteria = {'$or':[{'is_duplicate':False},{'is_duplicate':{'$exists':False}}]}
        # criteria = {'is_duplicate':False,'is_duplicate':{'$exists':False}}
        if 'odm_id' in args:
            criteria['id']=args['odm_id'][0]
            skip=0
        else:
            counter=conn[db][collection].find().count()

        populate_fields = [
            'title','notes',
            'tags',
            'organization.title',
            'author','author_email','maintainer_email',
            'license_id',
            'resources',
            'extras.language',
            'extras.date_released','extras.date_updated','extras.update_frequency',
            'category',
            'url',
        ]
        no_fields = len(populate_fields)

        result = conn[db][collection].find(spec=criteria, limit=batch_size, skip=skip, sort=[('_id',1)])

        total_res=[]
        for dataset in result:
            res=dict((k,v) for k,v in dataset.iteritems()
                    if (isinstance(v,list) and len(v)>0) or (not isinstance(v,list) and v not in [None,'','null']))
            res=list(set(populate_fields).difference(res.keys()))
            # res=list(set(populate_fields).intersection(res.keys()))
            total_res.append({'_id':dataset['id'],'fields':res,'not_populated':no_fields - len(res)})

        return {'ok':1,'result':total_res, 'counter':counter}


    def Getdssize(self, conn, args, version = None, db = None, collection = None):
        batch_size = 15
        if 'batch_size' in args:
            batch_size = int(args['batch_size'][0])

        skip = 0
        if 'offset' in args:
            try:
                skip = int(args['offset'][0]) if int(args['offset'][0])>0 else 0
            except ValueError:
                pass
        elif 'page' in args:
            try:
                skip = batch_size * (int(args['page'][0])-1 if int(args['page'][0])>0 else 0)
            except ValueError:
                pass

        odm_id=None
        counter=1
        match={'resources.0':{'$exists':True},
                '$or':[{'is_duplicate':False},{'is_duplicate':{'$exists':False}}]}
        if 'odm_id' in args:
            odm_id=args['odm_id'][0]
            match['id']=args['odm_id'][0]
            skip=0
        else:
            counter=conn[db][collection].find().count()

        result=conn[db][collection].aggregate([
            {'$match':match},
            {'$sort':{'_id':1}},
            {'$skip':skip},
            {'$limit':batch_size},
            {'$unwind':'$resources'},
            {'$group':{'_id':'$_id','size':{'$sum':'$resources.size'},'odm_id':{'$addToSet':'$id'}}},
            {'$unwind':'$odm_id'},
            {'$project':{'_id':'$odm_id','size':{'$divide': ['$size',1024]}}}
        ])

        if result['ok']==1:
            return {'ok':1,'result':result['result'],'datasets':counter}
        else:
            return result


    def Getcatlangs(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$match':{'extras.language':{'$nin':['',None]},
                # '$or':[{'is_duplicate':False},{'is_duplicate':{'$exists':False}}]
                }},
            {'$group': {'_id':'$catalogue_url','languages':{'$push':'$extras.language'},'counter':{'$sum':1}}},
            {'$sort':{'counter':-1}},
            ])

        if result['ok']==1:
            for metadata in range(0,len(result['result'])):
                lang_counter={}
                for language in result['result'][metadata]['languages']:
                    if language in lang_counter:
                        lang_counter[language]+=1
                    else:
                        lang_counter[language]=1

                del result['result'][metadata]['languages']
                result['result'][metadata]['languages']=lang_counter

            return result
        else:
            return result


    def Getcatupdatefreqfreq(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$match':{'extras.update_frequency':{'$nin':['',None]}}},
            {'$group': {'_id':'$catalogue_url','frequency':{'$push':'$extras.update_frequency'},'counter':{'$sum':1}}},
            {'$sort':{'counter':-1}},
            ])

        if result['ok']==1:
            for metadata in range(0,len(result['result'])):
                freq_counter={}
                for freq in result['result'][metadata]['frequency']:
                    if freq in freq_counter:
                        freq_counter[freq]+=1
                    else:
                        freq_counter[freq]=1

                del result['result'][metadata]['frequency']
                result['result'][metadata]['update_frequency']=freq_counter

            return result
        else:
            return result


    def Getcatupdatefreqprop(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$group': {'_id':'$catalogue_url','frequency':{'$push':'$extras.update_frequency'},'counter':{'$sum':1}}},
            {'$sort':{'counter':-1}},
            ])

        if result['ok']==1:
            for metadata in range(0,len(result['result'])):
                freq_counter={}
                for freq in result['result'][metadata]['frequency']:
                    if freq in freq_counter.keys():
                        freq_counter[freq]+=1
                    else:
                        freq_counter[freq]=1

                for i in freq_counter:
                    freq_counter[i]=(freq_counter[i]*100.00)/result['result'][metadata]['counter']

                result['result'][metadata]['counter']=(len(result['result'][metadata]['frequency'])*100.00)/result['result'][metadata]['counter']
                del result['result'][metadata]['frequency']
                result['result'][metadata]['update_frequency']=freq_counter

            return result
        else:
            return result


    def Getcatlastupdatebyyearfreq(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$match':{'extras.date_updated':{'$nin':['',None]},'extras.date_updated':{'$type':9}}},
            {'$group':{'_id':'$catalogue_url','dates':{'$push':{'$year':'$extras.date_updated'}},'counter':{'$sum':1}}},
            {'$sort':{'counter':-1}}
            ])

        if result['ok']==1:
            for metadata in range(0,len(result['result'])):
                date_freq={}
                for date in result['result'][metadata]['dates']:
                    try:
                        date_freq[date]+=1
                    except KeyError:
                        date_freq[date]=1

                del result['result'][metadata]['dates']
                result['result'][metadata]['frequency']=OrderedDict(sorted(date_freq.items(),reverse=True))

            return result
        else:
            return result


    def Getcatmedsincenewdays(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$match':{'extras.date_released':{'$nin':['',None]},'extras.date_released':{'$type':9}}},
            {'$group':{'_id':'$catalogue_url','dates':{'$push':'$extras.date_released'}}}
            ])

        if result['ok']==1:
            curr_date=datetime.now()
            # all_dates=[]
            for metadata in range(0,len(result['result'])):
                # latest_date=datetime(1900,1,1)
                # for date in result['result'][metadata]['dates']:
                #     if date > latest_date:
                #         latest_date=date
                #
                # all_dates.append((curr_date-latest_date).days)
                days_diff=[]
                for date in result['result'][metadata]['dates']:
                    days_diff.append((curr_date-date).days)

                result['result'][metadata]['days']=numpy.median(days_diff)
                del result['result'][metadata]['dates']

            return {'ok':1,'result':result['result']}
        else:
            return result


    def Getcatmedsinceupdatedays(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$match':{'extras.date_updated':{'$nin':['',None]},'extras.date_updated':{'$type':9}}},
            {'$group':{'_id':'$catalogue_url','dates':{'$push':'$extras.date_updated'}}}
            ])

        if result['ok']==1:
            curr_date=datetime.now()
            for metadata in range(0,len(result['result'])):
                days_diff=[]
                # latest_date=datetime(1900,1,1)
                # for date in result['result'][metadata]['dates']:
                #     if date > latest_date:
                #         latest_date=date
                #
                # all_dates.append((curr_date-latest_date).days)
                for date in result['result'][metadata]['dates']:
                    days_diff.append((curr_date-date).days)

                result['result'][metadata]['days']=numpy.median(days_diff)
                del result['result'][metadata]['dates']

            return {'ok':1,'result':result['result']}
        else:
            return result


    def Getcatstatcodeprop(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$unwind':'$resources'},
            # {'$match':{'resources.status_code':{'$exists':True}}},
            {'$group' :{'_id':'$catalogue_url','status_codes':{'$push':'$resources.status_code'},'counter':{'$sum':1}}},
            {'$sort':{'counter':-1}}
            ])


        if result['ok']==1:
            codes_freq={}
            del_list=[]
            for metadata in range(0,len(result['result'])):
                if len(result['result'][metadata]['status_codes'])>0:
                    for status_code in result['result'][metadata]['status_codes']:
                        try:
                            codes_freq[status_code]+=1
                        except KeyError:
                            codes_freq[status_code]=1

                    codes_prop={}
                    for i in codes_freq.keys():
                        codes_prop[i]=(codes_freq[i]*100.00)/result['result'][metadata]['counter']

                    result['result'][metadata]['frequency']=codes_freq
                    result['result'][metadata]['prop']=codes_prop

                    del result['result'][metadata]['status_codes']
                else:
                    del_list.append(metadata)

            for i in reversed(del_list):
                del result['result'][i]

            return {'ok':1,'result':result['result']}
        else:
            return result


    def Getcatbrokenlinksprop(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$unwind':'$resources'},
            {'$group' :{'_id':'$catalogue_url','status_codes':{'$push':'$resources.status_code'},'counter':{'$sum':1}}},
            {'$sort':{'counter':-1}}
            ])

        if result['ok']==1:
            del_list=[]
            for metadata in range(0,len(result['result'])):
                codes_freq=0
                if len(result['result'][metadata]['status_codes'])>0:
                    for status_code in result['result'][metadata]['status_codes']:
                        if status_code>=400:
                            codes_freq+=1

                    result['result'][metadata]['frequency']=codes_freq
                    result['result'][metadata]['prop']=(codes_freq*100.00)/result['result'][metadata]['counter']

                    del result['result'][metadata]['status_codes']
                else:
                    del_list.append(metadata)

            for i in reversed(del_list):
                del result['result'][i]

            return {'ok':1,'result':result['result']}
        else:
            return result



    def Getcatuniqprop(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$match':{'catalogue_url':{'$nin':['',None]}}},
            {'$unwind':'$resources'},
            {'$group': {'_id':'$catalogue_url',
                'freq': {'$sum': { '$cond': [ {'$or':[{ '$ne': [ "$is_duplicate", True] },
                    ]} , 1, 0 ] }},
                'tot_counter': {'$sum': 1}}},
            {'$sort':{'tot_counter':-1}},
        ])
        # result=conn[db][collection].aggregate([
        #     {'$unwind':'$resources'},
        #     {'$group' :{'_id':'$catalogue_url','checksums':{'$push':'$resources.file_hash'},'counter':{'$sum':1}}},
        #     {'$sort':{'counter':-1}}
        #     ])
        #
        if result['ok']==1:
            # ix_checksums=defaultdict(list)
            # for metadata in range(0,len(result['result'])):
            #     if len(result['result'][metadata]['checksums'])>0:
            #         for checksum in result['result'][metadata]['checksums']:
            #             if checksum not in ['',None]:
            #                 try:
            #                     ix_checksums[checksum].index(result['result'][metadata]['_id'])
            #                 except ValueError:
            #                     ix_checksums[checksum].append(result['result'][metadata]['_id'])

            # del_list=[]
            for metadata in range(0,len(result['result'])):
            #     dublicates=0
            #     if len(result['result'][metadata]['checksums'])>0:
            #         for ix in result['result'][metadata]['checksums']:
            #             if len(ix_checksums[ix])>1:
            #                 dublicates+=1
            #
            #         result['result'][metadata]['freq']=result['result'][metadata]['counter']-dublicates
            #         result['result'][metadata]['prop']=((result['result'][metadata]['counter']-dublicates)*100.00)/result['result'][metadata]['counter']
            #         del result['result'][metadata]['checksums']
            #     else:
            #         del_list.append(metadata)
            #
            # for i in reversed(del_list):
            #     del result['result'][i]
                result['result'][metadata]['prop']=(
                    (result['result'][metadata]['freq'])*100.00) \
                    /result['result'][metadata]['tot_counter']
                # del result['result'][metadata]['counter']

            return {'ok':1,'result':result['result']}
        else:
            return result


    def Getcatduplprop(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
            {'$match':{'catalogue_url':{'$nin':['',None]}}},
            {'$unwind':'$resources'},
            {'$group': {'_id':'$catalogue_url',
                'freq': {'$sum': { '$cond': [ { '$eq': [ "$is_duplicate", True] } , 1, 0 ] }},
                'counter': {'$sum': 1}}},
            {'$sort':{'counter':-1}},
        ])
        # result=conn[db][collection].aggregate([
        #     {'$unwind':'$resources'},
        #     {'$group' :{'_id':'$catalogue_url','checksums':{'$push':'$resources.file_hash'},'counter':{'$sum':1}}},
        #     {'$sort':{'counter':-1}}
        #     ])

        if result['ok']==1:
            # ix_checksums=defaultdict(list)
            # for metadata in range(0,len(result['result'])):
            #     if len(result['result'][metadata]['checksums'])>0:
            #         for checksum in result['result'][metadata]['checksums']:
            #             if checksum not in ['',None]:
            #                 try:
            #                     ix_checksums[checksum].index(result['result'][metadata]['_id'])
            #                 except ValueError:
            #                     ix_checksums[checksum].append(result['result'][metadata]['_id'])

            # del_list=[]
            for metadata in range(0,len(result['result'])):
            #     dublicates=0
            #     if len(result['result'][metadata]['checksums'])>0:
            #         for ix in result['result'][metadata]['checksums']:
            #             if len(ix_checksums[ix])>1:
            #                 dublicates+=1
            #
            #         result['result'][metadata]['freq']=dublicates
            #         result['result'][metadata]['prop']=(dublicates*100.00)/result['result'][metadata]['counter']
            #         del result['result'][metadata]['checksums']
            #     else:
            #         del_list.append(metadata)
            #
            # for i in reversed(del_list):
            #     del result['result'][i]
                result['result'][metadata]['prop']=(
                    (result['result'][metadata]['freq'])*100.00) \
                    /result['result'][metadata]['counter']
                del result['result'][metadata]['counter']

            return {'ok':1,'result':result['result']}
        else:
            return result


    def Getcatcountrynewmonthfreq(self, conn, args, version = None, db = None, collection = None):
        match={'catalogue_url':{'$nin':['',None]},'$and':[{'extras.date_released':{'$type':9}}]}
        # end_date=datetime.now()
        end_date = None
        if "end_date" in args:
            try:
                end_date=datetime.strptime(args['end_date'][0], '%Y-%m-%d')
                # match['$and'].append({'extras.date_released':{'$lte':end_date}})
            except ValueError as e:
                print (e)
        # start_date=end_date + relativedelta(years=-1)
        start_date = None
        if "start_date" in args:
            try:
                start_date=datetime.strptime(args['start_date'][0], '%Y-%m-%d')
                # match['$and'].append({'extras.date_released':{'$gte':start_date}})
            except ValueError as e:
                print (e)

        result=conn[db][collection].aggregate([
            # {'$match': {'catalogue_url':{'$nin':['',None]},'extras.date_released':{'$type':9}}},
            {'$match': match},
            {'$group': {
                '_id' : '$catalogue_url',
                'dates': {'$push': '$extras.date_released'},
                'country':{'$addToSet':'$country'},
                'counter': {'$sum': 1}}},
            # {'$group': { '_id': {'year':{'$year':'$extras.date_released'},'month':{'$month':'$extras.date_released'}},
            #         'catalogues':{'$push':'$catalogue_url'}, 'counter': {'$sum': 1}}},
            # {'$sort':{'_id.year':-1,'_id.month':-1}}
            # {'$sort': { 'counter':-1}},
            ])

        # then = datetime(1970,1,1)
            # for i in range(0,len(result['result'])):
            #     cat_dict={}
            #     for cat in result['result'][i]['catalogues']:
            #         try:
            #             cat_dict[cat]+=1
            #         except KeyError:
            #             cat_dict[cat]=1
            #
            #     result['result'][i]['catalogues']=cat_dict
            #     result['result'][i]['date']=str(date(result['result'][i]['_id']['year']
            #             ,result['result'][i]['_id']['month'],1))
            #     del result['result'][i]['counter']
            #     del result['result'][i]['_id']

        if result['ok']==1:
            date_catalogues=[]
            d=defaultdict(list)
            end_date_in_data = date.today()
            start_date_in_data = date.today()
            for i in range(0,len(result['result'])):
                dates=result['result'][i]['dates']
                # find outliers with timedelta
                # dates=[(t-then).total_seconds() for t in dates]
                # dates=self.__reject_outliers(numpy.array(dates),m=1)
                # dates=dates.tolist()
                # if dates:
                #     cat_dates=[]
                #     for dd in dates:
                #         cat_dates.append(then+timedelta(seconds=dd))
                #     cat_date=min(cat_dates)

                # find outliers with years
                years=[t.year for t in dates]
                years=self.__reject_outliers(numpy.array(years),m=1)
                years=years.tolist()
                years.sort(key=int)

                if years:
                    cat_date=None
                    dates.sort()
                    for j in dates:
                        if j.year==years[0]:
                            # cat_date=j
                            cat_date = date(j.year,j.month,1)
                            break

                    if len(result['result'][i]['country'])==1:
                        d[date(cat_date.year,cat_date.month,1)].append([result['result'][i]['country'][0],
                            result['result'][i]['_id']])
                    else:
                        d[date(cat_date.year,cat_date.month,1)].append(['Unkown',result['result'][i]['_id']])

                    if end_date == None and end_date_in_data < cat_date:
                        end_date_in_data = cat_date
                    if start_date == None and start_date_in_data > cat_date:
                        start_date_in_data = cat_date

            if end_date == None:
                i = date(end_date_in_data.year,end_date_in_data.month,1)
            else:
                i=date(end_date.year,end_date.month,1)
            if start_date == None:
                term_cond = date(start_date_in_data.year,start_date_in_data.month,1)
            else:
                term_cond = date(start_date.year,start_date.month,start_date.day)
            while i >= term_cond:
                if len(d[i])>0:
                    percountry=defaultdict(list)
                    for j in d[i]:
                        percountry[j[0]].append(j[1])

                    date_catalogues.append({'month':str(i),'catalogues': percountry, 'counter': len(d[i])})
                    # date_catalogues({'date': datetime.fromtimestamp(cat_initiated['first_dataset']/1e3),'catalogues': {cat_initiated}})
                i-=relativedelta(months=1)

            return {'ok':1, 'result': date_catalogues}
        else:
            return result



    def Getcatsitepagerank(self, conn, args, version = None, db = None, collection = None):
        result=conn[db][collection].aggregate([
             # {'$group':{'_id':'$catalogue_url', 'pagerank':{'$addToSet':'$pagerank'}}},
             {'$group':{'_id':'$cat_url', 'pagerank':{'$addToSet':
                 {'GooglePageRank':
                     {'$cond':[{'$gte':['$pagerank.GooglePageRank',0]},'$pagerank.GooglePageRank',-1]},
                     'AlexaTrafficRank':
                     {'$cond':[{'$gte':['$pagerank.AlexaTrafficRank',0]},'$pagerank.AlexaTrafficRank',-1]}
                     }}},
                 },
            ])

        return result



    def GetEDCoreMetadataFreq(self, conn, args, version = None, db = None, collection = None):
        start_time=time.time()

        match_dict =  {'catalogue_url': {'$nin': ['', None]},
                'extras.unpublished':{'$ne':"true"},
                'is_duplicate':{'$ne':True}
                }

        if 'include_dupls' in args:
            dupl_val = args['include_dupls'][0]
            if dupl_val.lower() in ['true','1']:
                del match_dict['is_duplicate']

        result=conn[db][collection].find(
                match_dict,
            {'_id':0,'license_id':1,'author':1,'maintainer':1,
                'extras.date_released':1,'extras.date_updated':1,
                # 'catalogue_url':1,
                'organization.title':1,
                }
            )

        if result.count()>0:
            log_op='and'
            if 'logical_op' in args and args['logical_op'][0] in ['and','or']:
                log_op = args['logical_op'][0]

            total=[]
            tot_count=0
            complete_meta_count=0
            for dd in result:
                tot_count+=1

                if log_op=='and':
                        if ( ('license_id' in dd and dd['license_id'] not in ['',None]) ):
                            complete_meta_count+=0.25
                        if ( ('author' in dd and dd['author'] not in ['',None])
                                    # or ('author_email' in dd and dd['author_email'] not in ['',None])
                                    or ('maintainer' in dd and dd['maintainer'] not in ['',None])
                                    # or ('maintainer_email' in dd and dd['maintainer_email'] not in ['',None])
                                    ):
                            complete_meta_count+=0.25
                        if ('organization' in dd and dd['organization'] is not None
                                and 'title' in dd['organization'] and dd['organization']['title'] not in ['',None]):
                            complete_meta_count+=0.25
                        if ( 'extras' in dd and (('date_released' in dd['extras'] and dd['extras']['date_released'] not in ['',None])
                                or ('date_updated' in dd['extras'] and dd['extras']['date_updated'] not in ['',None]))
                                ):
                            complete_meta_count+=0.25
                else:
                    if ( ('license_id' in dd and dd['license_id'] not in ['',None])
                        or ( ('author' in dd and dd['author'] not in ['',None])
                                # or ('author_email' in dd and dd['author_email'] not in ['',None])
                                or ('maintainer' in dd and dd['maintainer'] not in ['',None])
                                # or ('maintainer_email' in dd and dd['maintainer_email'] not in ['',None])
                                )
                        or ('organization' in dd and dd['organization'] is not None and 'title' in dd['organization'] and dd['organization']['title'] not in ['',None])
                        or ( 'extras' in dd and (('date_released' in dd['extras'] and dd['extras']['date_released'] not in ['',None])
                            or ('date_updated' in dd['extras'] and dd['extras']['date_updated'] not in ['',None])) )
                    ):
                        complete_meta_count+=1

            # print(tot_count)
            total.append({'freq': complete_meta_count/tot_count})

            print (time.time() - start_time)

            return {'ok':1, 'result': total}
        else:
            return result



    def GetCatCoreMetadataFreq(self, conn, args, version = None, db = None, collection = None):
        start_time=time.time()

        match_dict =  {'catalogue_url': {'$nin': ['', None]},
                'extras.unpublished':{'$ne':"true"},
                'is_duplicate':{'$ne':True}
                }

        if 'include_dupls' in args:
            dupl_val = args['include_dupls'][0]
            if dupl_val.lower() in ['true','1']:
                del match_dict['is_duplicate']

        result=conn[db][collection].find(
                match_dict,
            {'_id':0,'license_id':1,'author':1,'maintainer':1,
                'extras.date_released':1,'extras.date_updated':1,
                'catalogue_url':1,
                'organization.title':1,
                }
            )

        if result.count()>0:
            log_op='and'
            if 'logical_op' in args and args['logical_op'][0] in ['and','or']:
                log_op = args['logical_op'][0]

            total=[]
            tot_count={}
            # catalogues=set([])
            complete_meta_count={}
            for dd in result:
                # catalogues.add(dd['catalogue_url'])
                if dd['catalogue_url'] in tot_count:
                    tot_count[dd['catalogue_url']]+=1
                else:
                    tot_count[dd['catalogue_url']]=1

                if dd['catalogue_url'] not in complete_meta_count:
                    complete_meta_count[dd['catalogue_url']]=0

                if log_op=='and':
                    # if ( ('organization' in dd and dd['organization'] is not None) and 'extras' in dd):
                        if ( ('license_id' in dd and dd['license_id'] not in ['',None]) ):
                            complete_meta_count[dd['catalogue_url']]+=0.25
                        if ( ('author' in dd and dd['author'] not in ['',None])
                                    # or ('author_email' in dd and dd['author_email'] not in ['',None])
                                    or ('maintainer' in dd and dd['maintainer'] not in ['',None])
                                    # or ('maintainer_email' in dd and dd['maintainer_email'] not in ['',None])
                                    ):
                            complete_meta_count[dd['catalogue_url']]+=0.25
                        if ('organization' in dd and dd['organization'] is not None
                                and 'title' in dd['organization'] and dd['organization']['title'] not in ['',None]):
                            complete_meta_count[dd['catalogue_url']]+=0.25
                        if ( 'extras' in dd and (('date_released' in dd['extras'] and dd['extras']['date_released'] not in ['',None])
                                or ('date_updated' in dd['extras'] and dd['extras']['date_updated'] not in ['',None]))
                                ):
                            complete_meta_count[dd['catalogue_url']]+=0.25
                else:
                    if ( ('license_id' in dd and dd['license_id'] not in ['',None])
                        or ( ('author' in dd and dd['author'] not in ['',None])
                                # or ('author_email' in dd and dd['author_email'] not in ['',None])
                                or ('maintainer' in dd and dd['maintainer'] not in ['',None])
                                # or ('maintainer_email' in dd and dd['maintainer_email'] not in ['',None])
                                )
                        or ('organization' in dd and dd['organization'] is not None and
                            'title' in dd['organization'] and dd['organization']['title'] not in ['',None])
                        or ( ('date_released' in dd['extras'] and dd['extras']['date_released'] not in ['',None])
                            or ('date_updated' in dd['extras'] and dd['extras']['date_updated'] not in ['',None]) )
                    ):
                        if dd['catalogue_url'] in complete_meta_count:
                            complete_meta_count[dd['catalogue_url']]+=1
                        else:
                            complete_meta_count[dd['catalogue_url']]=1

            for i in tot_count:
            # for i in catalogues:
                if i in complete_meta_count:
                    total.append({'_id':i,'count': complete_meta_count[i],'tot_count':tot_count[i]})
                    # total.append({'_id':i,'count': complete_meta_count[i]})
                else:
                    # total.append({'_id':i,'freq': 0/tot_count[i]})
                        total.append({'_id':i,'count': 0,'tot_count':tot_count[i]})

            print (time.time() - start_time)

            return {'ok':1, 'result': total}
        else:
            return result


    def GetCatAccessabilityFreq(self, conn, args, version = None, db = None, collection = None):
        start_time=time.time()

        match_dict =  {'catalogue_url': {'$nin': ['', None]},
                'extras.unpublished':{'$ne':"true"},
                'is_duplicate':{'$ne':True}
                }

        if 'include_dupls' in args:
            dupl_val = args['include_dupls'][0]
            if dupl_val.lower() in ['true','1']:
                del match_dict['is_duplicate']

        result=conn[db][collection].find(
                match_dict,
            {'_id':0,
                # 'author_email':1,'maintainer_email':1,
                'author':1,'organization.title':1,
                'catalogue_url':1,'resources.status_code':1,
                'notes':1
                }
            )

        # print(result)
        if result.count()>0:
            result.batch_size(MongoHandler.batchSize)

            total=[]
            tot_count={}
            accessible_count={}
            for dd in result:
                if 'resources' in dd and len(dd['resources']) > 0:
                    if dd['catalogue_url'] in tot_count:
                        tot_count[dd['catalogue_url']]+=1
                    else:
                        tot_count[dd['catalogue_url']]=1

                    not_broken=False
                    for res in dd['resources']:
                        if 'status_code' in res and res['status_code'] < 400:
                            not_broken=True
                            break

                    if ( not_broken
                            and (
                                ('author' in dd and dd['author'])
                                or ('organization' in dd and 'title' in dd['organization'] and dd['organization']['title'] not in ['',None])
                                )
                            and ('notes' in dd and dd['notes'])
                    ):
                        if dd['catalogue_url'] in accessible_count:
                            accessible_count[dd['catalogue_url']]+=1
                        else:
                            accessible_count[dd['catalogue_url']]=1

            # # print(tot_count)
            for i in tot_count:
                if i in accessible_count:
                    total.append({'_id':i,'count': accessible_count[i],
                        'tot_count':tot_count[i]})
                else:
                    total.append({'_id':i,'count': 0,
                        'tot_count':tot_count[i]})

            print (time.time() - start_time)

            return {'ok':1, 'result': total}
        else:
            return result


    def Getcatsinfo(self, conn, args, version = None, db = None, collection = None,dates={}):
        result=conn[db][collection].aggregate([{'$match':dates},
            {'$project':
                {'_id':0,'title':1,'description':1,'url':'$cat_url',
                    'source_type':'$type','language':1,'country':1,
                    # 'date_created':'$catalogue_date_created','date_updated':'$catalogue_date_updated',
                    'update_frequency':'$frequency',
                    'date_harvested':1,'date_updated':'$date_reharvested',
                    'official':{'$ifNull':['$official','U']},
                    'is_harmonized':{'$ifNull':['$available',False]},
                    }}
            ])

        return result


    ## returns the number of catalogued datasets
    def _aggregate(self, conn, args, pipeline_args, version = None, db = None, collection = None,
            non_empty = True, default_match = None):
        """
        Get aggregated data
        """

        if type(args).__name__ != 'dict':
            return {"ok" : 0, "errmsg" : "_agregate must be a GET request"}

        #conn = self._get_connection(name)
        if conn == None:
#            out('{"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}')
            return {"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}

        if db == None or collection == None:
            return {"ok" : 0, "errmsg" : "db and collection must be defined"}

        #obj = json.loads(str, object_hook=json_util.object_hook)
        #fields = 'catalogue_url'
        #if 'fields' in args:
        #    fields = args['fields'][0]
        #    if fields == None:
        #        return

        #fields = '$' + fields

        pipeline = []
        if non_empty:
            pipeline.append({"$match": {pipeline_args["group"].replace('$',''): { "$nin": [None,""]}}})

        if 'unwind' in pipeline_args:
            pipeline.append({"$unwind": pipeline_args["unwind"]})

#        if non_empty:
#            pipeline.append({"$match": {pipeline_args["group"].replace('$',''): { "$nin": [None,""]}}})

        if default_match != None:
            if 'cat' not in args:
                pipeline.append({"$match": {pipeline_args["group"].replace('$',''): default_match} })
            else:
                pipeline.append({"$match": {pipeline_args["group"].replace('$',''): args['cat'][0]} })

        pipeline.extend(({"$group": {"_id": pipeline_args["group"], "counter": {"$sum": 1}}},
                {"$sort": {"counter": -1}})
                )

        if 'limit' in pipeline_args:
            pipeline.append({"$limit": pipeline_args["limit"]})

        cursor = conn[db][collection].aggregate(pipeline, allowDiskUse=True)

        #out(json.dumps(cursor, default=json_util.default))
        #self.__output_results(cursor,out,batch_size);
        return cursor

    def GetHelp(self, func_name):
        if func_name == '_catfreq':
            return 'Total number of catalogues'
        elif func_name == '_catsectfreq':
            return 'Frequency of catalogues by sector of publishing organisation'
        elif func_name == '_catsectprop':
            return 'Proportion of catalogues by sector of publishing organisation'
        elif func_name == '_catsoftfreq':
            return 'Frequency of catalogues using specific software platforms'
        elif func_name == '_catsoftprop':
            return 'Proportion of catalogues using specific software platforms'
        elif func_name == '_catmedageyears':
            return 'Median age of catalogues'
        elif func_name == '_catmeanageyears':
            return 'Mean age of catalogues'
        elif func_name == '_catnewmonthfreq':
            return 'New catalogues per month'
        elif func_name == '_catfreqranktop':
            return 'Highest frequency of catalogues per country'
        elif func_name == '_catfreqrankbottom':
            return 'Lowest frequency of catalogues per country'
        elif func_name == '_catcapitaranktop':
            return 'Highest frequency of catalogues per capita per country'
        elif func_name == '_catcapitarankbottom':
            return 'Lowest frequency of catalogues per capita per country'
        elif func_name == '_catgeofreq':
            return 'Catalogues per geographic region'
        elif func_name == '_catcapitafreq':
            return 'Catalogues per capita per country'
        elif func_name == '_catgdpcorr':
            return "Catalogues & per-capita GDP correlation (Pearson and Spearman\'s rank)"
        elif func_name == '_cathdicorr':
            return 'Catalogues & HDI correlation (Pearson and Spearman\'s rank)'
        elif func_name == '_catcountrysectfreq':
            return 'Frequency of catalogues by sector of publishing organisation'
        elif func_name == '_catcountrysectprop':
            return 'Proportion of catalogues by sector of publishing organisation'
        elif func_name == '_catcountrynewmonthfreq':
            return 'New catalogues per country per month'
        elif func_name == '_cat___total':
            return 'Total (sum)'
        elif func_name == '_cat___med':
            return 'Median '
        elif func_name == '_cat___mean':
            return 'Mean '
        elif func_name == '_cat___min':
            return 'Minimum'
        elif func_name == '_cat___max':
            return 'Maximum'
        elif func_name == '_cat___stddev':
            return 'Standard deviation'
        elif func_name == '_catdatasetsfreq':
            return 'Frequency of catalogued datasets'
        elif func_name == '_catdistribsfreq':
            return 'Frequency of catalogued distributions'
        elif func_name == '_catpublishersfreq':
            return 'Frequency of unique organisations publishing data'
        elif func_name == '_catdatasizetotal':
            return 'Total distribution size in a catalogue'
        elif func_name == '_catdatasizemed':
            return 'Median distribution size'
        elif func_name == '_catdatasizemean':
            return 'Mean distribution size'
        elif func_name == '_catdatasizemax':
            return 'Maximum distribution size'
        elif func_name == '_catdatasizestddev':
            return 'Standard deviation of distribution sizes'
        elif func_name == '_catduplprop':
            return 'Proportion of distributions in each catalogue that are listed in other catalogues'
        elif func_name == '_catuniqprop':
            return 'Proportion of distributions in each catalogue that are not listed in any other catalogues'
        elif func_name == '_catbrokenlinksprop':
            return 'Proportion of data file links that are broken'
        elif func_name == '_catstatcodeprop':
            return 'Proportion of different HTTP status codes for data file URIs'
        elif func_name == '_catfileformatfreq':
            return 'Frequency of distributions by file format'
        elif func_name == '_catfileformatprop':
            return 'Proportion of distributions by file format'
        elif func_name == '_catmachinereadformatfreq':
            return 'Frequency of distributions in a machine-readable file format'
        elif func_name == '_catmachinereadformatprop':
            return 'Proportion of distributions in a machine-readable file format'
        elif func_name == '_catmimetypefreq':
            return 'Frequency of distributions by MIME type of data file'
        elif func_name == '_catmimetypeprop':
            return 'Proportion of distributions by MIME type of data file'
        elif func_name == '_catmachinereadfreq':
            return 'Frequency of distributions that are machine-readable'
        elif func_name == '_catmachinereadprop':
            return 'Proportion of distributions that are machine-readable'
        elif func_name == '_catlicensedfreq':
            return 'Frequency of distributions with an explicitly set license'
        elif func_name == '_catlicensedprop':
            return 'Proportion of distributions with an explicitly set license'
        elif func_name == '_catopenlicfreq':
            return 'Frequency of distributions with an open license'
        elif func_name == '_catopenlicprop':
            return 'Proportion of distributions with an open license (excluding and including distributions with missing licenses)'
        elif func_name == '_catdsbylicensefreq':
            return 'Frequency of distributions by license type'
        elif func_name == '_catdsbylicenseprop':
            return 'Proportion of distributions by license type (excluding and including distributions with missing licenses)'
        elif func_name == '_catmedsinceupdatedays':
            return 'Median days since latest dataset update'
        elif func_name == '_catmedsincenewdays':
            return 'Median days since latest new dataset'
        elif func_name == '_catlastupdatebyyearfreq':
            return 'Frequency of dataset last update by yea'
        elif func_name == '_catupdatefreqfreq':
            return 'Frequency of datasets with stated update frequency'
        elif func_name == '_catupdatefreqprop':
            return 'Proportion of datasets with stated update frequency'
        elif func_name == '_cattau':
            return 'Tau of the catalogue'
        elif func_name == '_catsitepagerank':
            return 'PageRank of the catalogue site'
        elif func_name == '_catuniqpublishersfreq':
            return 'Frequency of unique publishers contributing to the catalogue'
        elif func_name == '_catuniqpublishersprop':
            return 'Frequency of unique publishers relative to catalogue size'
        elif func_name == '_catapisdumpsfreq':
            return 'Frequency of datasets available via APIs and/or data dumps'
        elif func_name == '_catapisdumpsprop':
            return 'Proportion of datasets available via APIs and/or data dumps'
        elif func_name == '_catapisdumpsratio':
            return 'Ratio of datasets with APIs to those with data dumps'
        elif func_name == '_catdspreviewsfreq':
            return 'Frequency of distributions with previews'
        elif func_name == '_catdspreviewsprop':
            return 'Proportion of distributions with previews'
        elif func_name == '_catlangs':
            return 'Frequency of different languages'
        elif func_name == '_dssize':
            return 'Dataset size'
        elif func_name == '_dspopulatedmdfields':
            return 'Number of fields in the metadata record that are populated'
        elif func_name == '_dsvocabsusedfreq':
            return 'Frequency of unique vocabularies used in metadata record'
        elif func_name == '_dsvocabtermsusedfreq':
            return 'Frequency of terms used from each vocabulary present in metadata record'
        elif func_name == '_dsvocabtermsusedprop':
            return 'Proportion of terms used from each vocabulary present in metadata record'
        elif func_name == '_dsodcertlevel':
            return 'Open Data Certificate level of the dataset'
        elif func_name == '_dscsvvalidationfreq':
            return 'Frequency of Errors and Warnings generated by CSVlint'
        elif func_name == '_dstimeliness':
            return 'Timeliness of the dataset'
        else:
            return ''


    def __reject_outliers(self, data, m = 2.):
        return data[abs(data - numpy.mean(data)) < m * numpy.std(data)]
        # d = numpy.abs(data - numpy.median(data))
        # mdev = numpy.median(d)
        # s = d/mdev if mdev else 0.
        # return data[s<m]


    def __pearson(self, prefs, p1, p2):
        si={}
        for item in prefs[p1]:
            if item in prefs[p2]: si[item]=1

        n=len(si)

        if n==0: return 0

        #regular sums
        sum1=sum([prefs[p1][it] for it in si])
        sum2=sum([prefs[p2][it] for it in si])

        #sum of the squares
        sum1Sq=sum([pow(prefs[p1][it],2) for it in si])
        sum2Sq=sum([pow(prefs[p2][it],2) for it in si])

        #sum of the products
        pSum=sum([prefs[p1][it]*prefs[p2][it] for it in si])

        #do pearson score
        num=pSum-(sum1*sum2/n)
        den=sqrt((sum1Sq-pow(sum1,2)/n)*(sum2Sq-pow(sum2,2)/n))
        if den==0: return 0

        r=num/den

        return r


    def __normalize_values(self,value,oldMin,oldMax,newMin,newMax):
        # old_min = min(values)
        # old_range = max(values) - old_min
        old_min = oldMin
        old_range = oldMax - old_min

        new_min = newMin
        new_range = newMax + 0.9999999999 - new_min
        # output = [int((n - old_min) / old_range * new_range + new_min) for n in values]
        output = int((value- old_min)*1.00 / old_range * new_range + new_min)

        return output


class MongoFakeStream:
    def __init__(self):
        self.str = ""

    def ostream(self, content):
        self.str = self.str + content

    def get_ostream(self):
        return self.str

class MongoFakeFieldStorage:
    def __init__(self, args):
        self.args = args

    def getvalue(self, key):
        return self.args[key]

    def __contains__(self, key):
        return key in self.args
