#encoding=utf8

import socketserver, json, threading
import os, sqlite3, codecs, re, traceback, cx_Oracle
import PageGenerator

FILE_DICT_DBNAME = 'pageDict.db'
DB_CONNECT_STRING_ORACLE = 'foragCollecter_1/foragCollecter@10.18.50.229/orcl'
# DB_CONNECT_STRING_ORACLE = 'foragCollecter_1/foragCollecter@192.168.43.16/orcl'
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

class ForagInterfaceHandler(socketserver.StreamRequestHandler):
    
    def setup(self):
        super(ForagInterfaceHandler, self).setup()
         
        self.serviceDict = {}
        self.serviceDict['getUserInteres'] = GetUserInterestPageService()
        self.serviceDict['generatePage'] = GeneratePageService()
        self.serviceDict['uploadPageTemplate'] = UploadPageTemplateService()
        self.serviceDict['uploadPageSchedule'] = UploadPageTemplateService()
        self.serviceDict['uploadSpider'] = UploadSpiderService()
        self.serviceDict['getHotArticle'] = GetHotArticleService()
        self.serviceDict['getTagArticle'] = GetTagArticleService()
    
    def registerService(self, name, serviceObj):
        self.serviceDict[name] = serviceObj
        
    def unregisterService(self, name):
        self.serviceDict.pop(name, "specified service don't existed")
    
    def handle(self):
        print('client ip:%s port:%s' % self.client_address)
        
        response = {}
        
        data = self.rfile.readline().decode()
        print('recv:%s' % data)
        requestParams = json.loads(data)
        print(requestParams)
        self.serviceDict[requestParams['name']].service(requestParams, response)
#         try:
#             data = self.rfile.readline().decode()
#             print('recv:%s' % data)
#             requestParams = json.loads(data)
#             print(requestParams)
#             self.serviceDict[requestParams['name']].service(requestParams, response)
#         except Exception:
#             response['state'] = 'failed'
#             response['reason'] = traceback.format_exc()
         
        self.wfile.write(json.dumps(response).encode())
            
class ForagInterfaceServer:
    def __init__(self, serve_addr, maxConnCnt=2):
        self.maxConnCnt = maxConnCnt
        self.server = socketserver.TCPServer(serve_addr, ForagInterfaceHandler)
        
    def run(self):
        for i in range(0, self.maxConnCnt):
            t = threading.Thread(target=self.server.serve_forever)
            t.setDaemon(True)
            t.start()
            
        self.server.serve_forever()

# {"name":"getUserInterestPage","params":{"user":{"detail":{},"interest":["数据库","python"]},"log":"日志文件","len":"10"}}
class GetUserInterestPageService:
    def service(self, params, response):
        params['len']
        params['log']
        params['user']['detail']
        params['user']['interest']
        response['data'] = "GetUserInterestPageService"

# {"name":"getTagArticle","params":{"type":"channel","name":"数据库","len":"10", "offset":"0", "userid":"123"}}
class GetTagArticleService:
    SQL_GET_TAG_MSG_ID = 'select tMsg from foragOwner.TagMsg where tName=:1'
    SQL_GET_CHANNEL_MSG_ID = 'select cMsg from foragOwner.ChannelMsg where cName=:1'
    SQL_GET_MSG = 'select mId, mTitle, mIntro, mPic, mTags, mAuthor, \
        mPublishTime, mLikeCount, mDislikeCount, mCollectCount, mTransmitCount from foragOwner.MsgTable where in %s'
    
    def __init__(self):
        self.conn = cx_Oracle.connect(DB_CONNECT_STRING_ORACLE)
        
    def _getTagMsg(self, name):
        cursor = self.conn.cursor()
        tagMsg = cursor.execute(self.SQL_GET_TAG_MSG, name).fetchone()
        return tagMsg
    
    def _getChannelMsg(self, name):
        cursor = self.conn.cursor()
        channelMsg = cursor.execute(self.SQL_GET_CHANNEL_MSG, name).fetchone()
        return channelMsg
    
    def service(self, params, response):
        params = params['params']
        msgIds = self._getChannelMsg(params['name']) if params['type']=='channel' else self._getTagMsg(params['name'])    
        msgIds = tuple(json.loads(msgIds).keys())
        msgs = self.conn.execute(self.SQL_GET_MSG % (str(msgIds) if len(msgIds) > 1 else '(%d)' % msgIds[0])).fetchall()

        response['result'] = json.dumps(msgs[int(params['offset']):int(params['offset']) + int(params['len'])], ensure_ascii=False)
        response['state'] = 'success'
        print(response['result'])
        
# {"name":"getHotArticle","params":{"len":"10", "userid":"123"}}
class GetHotArticleService:
    flushFrequence = 1800
    flushTimes = 1000
    msgListSize = 50
    
    SQL_GET_NEWEST_MSG = 'select mId, mTitle, mIntro, mPic, mTags, mAuthor, \
        mPublishTime, mLikeCount, mDislikeCount, mCollectCount, mTransmitCount from \
        (select * from foragOwner.MsgTable order by mPublishTime desc) where rownum<=:1'
    SQL_GET_POPULAREST_MSG_ID = 'select pageId from FileDict order by requestCnt desc limit :1'
    SQL_GET_POPULAREST_MSG = 'select mId, mTitle, mIntro, mPic, mTags, mAuthor, \
        mPublishTime, mLikeCount, mDislikeCount, mCollectCount, mTransmitCount from foragOwner.MsgTable where mId in '
    
    def __init__(self):
        self.hotMsg = []
        self._getPopularestMsg()
    
    def _getMsgToList(self, sql, params):
        conn = cx_Oracle.connect(DB_CONNECT_STRING_ORACLE)
        try:
            cursor = conn.cursor()
            result = cursor.execute(sql, params).fetchall()
            colName = list(map(lambda x:x[0], cursor.description))
            for row in result:
                colDict = {}
                for index, col in enumerate(row):
                    colDict[colName[index]] = col
                self.hotMsg.append(colDict)
                
            for item in self.hotMsg:
                for key, value in item.items():
                    item[key] = str(value) if value!=None else ''
        finally:
            conn.close()
    
    def _getNewestMsg(self):
        self._getMsgToList(self.SQL_GET_NEWEST_MSG, (self.msgListSize,))
            
    def _getPopularestMsg(self):
        listFreeSize = self.msgListSize
        conn = sqlite3.connect(FILE_DICT_DBNAME)
        try:
            cursor = conn.cursor()
            pageIds = cursor.execute(self.SQL_GET_POPULAREST_MSG_ID, (self.msgListSize,)).fetchall()[0]
            listFreeSize -= len(pageIds)
            pageIds = str(pageIds) if len(pageIds) > 1 else '(%d)' % pageIds[0]
        finally:
            conn.close()
            
        self._getMsgToList(self.SQL_GET_POPULAREST_MSG + pageIds, ())
        self._getMsgToList(self.SQL_GET_NEWEST_MSG, (listFreeSize,))
        
        self.timer = threading.Timer(self.flushFrequence, self._getPopularestMsg)
        self.timer.start()
        
    def _getUserRequestOffset(self, userid, number):
        return (0, 10)
    
    def service(self, params, response):
        offset = self._getUserRequestOffset(params['params']['userid'], params['params']['len'])
        response['result'] = json.dumps(self.hotMsg[offset[0]:offset[1]], ensure_ascii=False)
        response['state'] = 'success'
        print(response['result'])

#userid可用作协同过滤
# {"name":"generatePage","params":{"pageid":"12687","template":"articleTemplate.json", "userid":"123"}}
class GeneratePageService:
    SQL_CREATE_DB = 'CREATE TABLE IF NOT EXISTS FileDict(pageId INTEGER primary key, \
                        storeUrl text not null, \
                        requestCnt integer not null)'
    PAGETEMPLATE_DIR = "pageTemplate//"
    
    def __init__(self):
        self.__createDB()
        
        self.orConn = cx_Oracle.connect(DB_CONNECT_STRING_ORACLE)
        self.slConn = sqlite3.connect(FILE_DICT_DBNAME)
        
        self.templateDict = {}
        self.generator = PageGenerator.PageGenerator(self.slConn, self.orConn, 'articles')
    
    def __createDB(self):
        db = sqlite3.connect(FILE_DICT_DBNAME)
        cursor = db.cursor()
        cursor.execute(self.SQL_CREATE_DB)
        db.commit()
        db.close()
    
    def __getTemplate(self, name):
        if name not in self.templateDict:
            file = codecs.open(self.PAGETEMPLATE_DIR + name, 'r', encoding='utf8')
            self.templateDict[name] = re.sub(r'\s+', " ", file.read())
            file.close()
    
        return self.templateDict[name]
        
    def service(self, params, response):
        template = self.__getTemplate(params['params']['template'])
        response['result'] = self.generator.getPage(params['params']['pageid'], template)
        response['state'] = "success"
        print(response['result'])

# {"name":"uploadPageTemplate","params":{"name":"模板一","file":"文件数据"}}
class UploadPageTemplateService:
    def service(self, params, response):
        response['data'] = codecs.open('pageTemplate/articleTemplate.css', 'r', 'utf-8').read()

# {"name":"uploadPageSchedule","params":{"name":"策略一","file":"文件数据"}}
class UploadPageScheduleService:
    def service(self, params, response):
        response['data'] = "UploadPageScheduleService"

# {"name":"uploadSpider","params":{"name":"爬虫一","file":"文件数据"}}
class UploadSpiderService:
    def service(self, params, response):
        response['data'] = "UploadSpiderService"

if __name__ == '__main__':
    server = ForagInterfaceServer(('', 9999))
    server.run()

#     conn = sqlite3.connect(FILE_DICT_DBNAME)
#     try:
#         cursor = conn.cursor()
#         pageIds = cursor.execute(GetHotArticleService.SQL_GET_POPULAREST_MSG_ID, (10,)).fetchall()[0]
#         pageIds = str(pageIds) if len(pageIds) > 1 else '(%d)' % pageIds[0]
#     finally:
#         conn.close()
#     hotMsg = []
#     conn = cx_Oracle.connect(DB_CONNECT_STRING_ORACLE)
#     try:
#         cursor = conn.cursor()
#         print(GetHotArticleService.SQL_GET_POPULAREST_MSG + pageIds)
#         result = cursor.execute(GetHotArticleService.SQL_GET_POPULAREST_MSG + pageIds, ())
#         colName = list(map(lambda x:x[0], cursor.description))
#         for row in result.fetchall():
#             colDict = {}
#             for index, col in enumerate(row):
#                 colDict[colName[index]] = col
#             hotMsg.append(colDict)
#     finally:
#         conn.close()
#     print(hotMsg)