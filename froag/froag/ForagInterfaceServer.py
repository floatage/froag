#encoding=utf8

import socketserver, json, threading
import os, sqlite3, codecs, re, cx_Oracle
import PageGenerator, PageRecommand

FILE_DICT_DBNAME = 'pageDict.db'
# DB_CONNECT_STRING_ORACLE = 'foragCollecter_1/foragCollecter@10.18.50.229/orcl'
# DB_CONNECT_STRING_ORACLE = 'foragCollecter_1/foragCollecter@192.168.43.16/orcl'
DB_CONNECT_STRING_ORACLE = 'foragCollecter_1/foragCollecter@192.168.1.181/orcl'
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

def createDB(name, sql):
    db = sqlite3.connect(name)
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()
    db.close()
    
def getValidOffset(offset, number, maxSize):
    begin, number = int(offset), int(number)
    if begin > maxSize or number <= 0: return (0, 0)
    end = begin + number
    end = maxSize if end > maxSize else end
    return (begin, end)

# {"name":"getUserInterestPage","params":{"user":{},"log":"日志文件","len":"10"}}
class GetUserInterestPageService:
    SQL_GET_MSG = 'select mId, mTitle, mIntro, mPic, mTags, mAuthor, \
        mPublishTime, mLikeCount, mDislikeCount, mCollectCount, mTransmitCount from foragOwner.MsgTable where mId in '
    
    def __init__(self):
        self.conn = cx_Oracle.connect(DB_CONNECT_STRING_ORACLE)
        self.cursor = self.conn.cursor()
        
        self.generator = PageRecommand.InterestArticleGenerator()
    
    def _getMsgs(self, msgIds):
        msgIds = str(msgIds) if len(msgIds) > 1 else '(%d)' % msgIds[0]
        result = self.cursor.execute(self.SQL_GET_MSG + msgIds).fetchall()
        result = result if result else []
        result = [[str(col) if col!=None else 'None' for col in row]for row in result]  
        return result
    
    def service(self, params, response):
        params = json.loads(codecs.open(r'config/requestSample.json', 'r', 'utf-8').read())
        msgIds = self.generator.generate(params['params'], self.conn)
        response['result'] = self._getMsgs(msgIds)
        response['state'] = 'success'
        self.conn.close()
        print(response['result'])

#{"name":"getHotTag","params":{"len":"10","offset":"0"}}
class GetHotTagService:
    updateFrequence = 1800
    updateTimes = 1000
    tagListSize = 50
    
    SQL_GET_HOTTAG = 'select * from (select tName from foragOwner.TagMsg order by tRequest desc) where rownum<=:1'
    
    def __init__(self):
        self.hotTag = []
        self._updateHotTag()
        
    def _updateHotTag(self):
        conn = cx_Oracle.connect(DB_CONNECT_STRING_ORACLE)
        try:
            cursor = conn.cursor()
            result = cursor.execute(self.SQL_GET_HOTTAG, (self.tagListSize, )).fetchall()
            self.hotTag = [x[0] for x in result] if result else []
        finally:
            conn.close()
        
        print('update hot tag')
        self.timer = threading.Timer(self.updateFrequence, self._updateHotTag)
        self.timer.start()
    
    def service(self, params, response):
        offset = getValidOffset(params['params']['offset'], params['params']['len'], self.tagListSize)
        response['result'] = [] if offset == (0,0) else self.hotTag[offset[0]:offset[1]]
        response['state'] = 'success'
        print(response['result'])

# {"name":"getTagArticle","params":{"type":"tag","name":"数据库","len":"10", "offset":"0", "userid":"123"}}
class GetTagArticleService:
    SQL_GET_TAG_MSG_ID = 'select tMsg from foragOwner.TagMsg where tName=:1'
    SQL_GET_CHANNEL_MSG_ID = 'select cMsg from foragOwner.ChannelMsg where cName=:1'
    SQL_GET_MSG = 'select mId, mTitle, mIntro, mPic, mTags, mAuthor, \
        mPublishTime, mLikeCount, mDislikeCount, mCollectCount, mTransmitCount from foragOwner.MsgTable where mId in '
    SQL_REQUEST_TAG = 'update foragOwner.TagMsg set tRequest=tRequest+1 where tName=:1'
    def __init__(self):
        self.conn = cx_Oracle.connect(DB_CONNECT_STRING_ORACLE)
        self.cursor = self.conn.cursor()
    
    def _recordTagRequest(self, name):
        self.cursor.execute(self.SQL_REQUEST_TAG, (name,))
        self.conn.commit()
    
    def _getTagMsg(self, name):
        self._recordTagRequest(name)
        tagMsg = self.cursor.execute(self.SQL_GET_TAG_MSG_ID, (name,)).fetchone()
        return tagMsg
    
    def _getChannelMsg(self, name):
        channelMsg = self.cursor.execute(self.SQL_GET_CHANNEL_MSG_ID, (name,)).fetchone()
        return channelMsg
    
    def service(self, params, response):
        params = params['params']
        
        msgIds = self._getChannelMsg(params['name']) if params['type']=='channel' else self._getTagMsg(params['name'])
        if msgIds: 
            msgIds = tuple(map(lambda x:int(x[0]), json.loads(str(msgIds[0]))))
            offset = getValidOffset(params['offset'], params['len'], len(msgIds))
            if offset != (0,0):
                msgIds = msgIds[offset[0]:offset[1]]
                msgIds = str(msgIds) if len(msgIds) > 1 else '(%d)' % msgIds[0]
                
                msgs = self.cursor.execute(self.SQL_GET_MSG + msgIds).fetchall()
                for index,msg in enumerate(msgs):
                    msgs[index] = [str(v) for v in msg]
                
                response['result'] = msgs
            else:
                response['result'] = '[]'
        else:
            response['result'] = '[]'
        
        response['state'] = 'success'
        self.conn.close()
        print(response['result'])
        
# {"name":"getHotArticle","params":{"len":"10", "userid":"123", "offset":"0"}}
class GetHotArticleService:
    updateFrequence = 1800
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
            self.hotMsg.clear()
            for row in result:
                value = [str(col) if col!=None else 'None' for col in row]
                self.hotMsg.append(value)
        finally:
            conn.close()
    
    def _getNewestMsg(self):
        self._getMsgToList(self.SQL_GET_NEWEST_MSG, (self.msgListSize,))
            
    def _getPopularestMsg(self):
        listFreeSize = self.msgListSize
        conn = sqlite3.connect(FILE_DICT_DBNAME)
        try:
            cursor = conn.cursor()
            pageIds = cursor.execute(self.SQL_GET_POPULAREST_MSG_ID, (self.msgListSize,)).fetchall()
            if pageIds:
                listFreeSize -= len(pageIds)
                pageIds = pageIds[0]
                pageIds = str(pageIds) if len(pageIds) > 1 else '(%d)' % pageIds[0]
                self._getMsgToList(self.SQL_GET_POPULAREST_MSG + pageIds, ())
        finally:
            conn.close()
            
        self._getMsgToList(self.SQL_GET_NEWEST_MSG, (listFreeSize,))
        
        print('update hot article')
        self.timer = threading.Timer(self.updateFrequence, self._getPopularestMsg)
        self.timer.start()
        
    def _getUserRequestOffset(self, userid, offset, number):
        return getValidOffset(int(offset), int(number), self.msgListSize)
    
    def service(self, params, response):
        offset = self._getUserRequestOffset(params['params']['userid'], params['params']['offset'], params['params']['len'])
        response['result'] = [] if offset == (0,0) else self.hotMsg[offset[0]:offset[1]]
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
        createDB(FILE_DICT_DBNAME, self.SQL_CREATE_DB)
        
        self.orConn = cx_Oracle.connect(DB_CONNECT_STRING_ORACLE)
        self.slConn = sqlite3.connect(FILE_DICT_DBNAME)
        
        self.templateDict = {}
        self.generator = PageGenerator.PageGenerator(self.slConn, self.orConn, 'articles')
    
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
        self.orConn.close()
        self.slConn.close()
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

class ServiceManager:
    def __init__(self):
        self.serviceSingletons = {}
        self.serviceSingletons['getHotArticle'] = GetHotArticleService()
        self.serviceSingletons['getHotTag'] = GetHotTagService()
    
    def getServiceObj(self, name):
        obj = None
        if name in self.serviceSingletons:
            obj = self.serviceSingletons[name]
        elif name == 'generatePage':
            obj = GeneratePageService()
        elif name == 'getTagArticle':
            obj = GetTagArticleService()
        elif name == 'getUserInterestPage':
            obj = GetUserInterestPageService()
        elif name == 'uploadPageTemplate':
            obj = UploadPageTemplateService()
        elif name == 'uploadPageSchedule':
            obj = UploadPageTemplateService()
        elif name == 'uploadSpider':
            obj = UploadSpiderService()
        return obj
    
    def registerService(self, name, serviceObj):
        self.serviceDict[name] = serviceObj
        
    def unregisterService(self, name):
        self.serviceDict.pop(name, "specified service don't existed")

serviceManager = ServiceManager()
print('service init finished')
class ForagInterfaceHandler(socketserver.StreamRequestHandler):
    def handle(self):
        print('client ip:%s port:%s' % self.client_address)
        
        response = {}
        
        data = self.rfile.readline().decode()
        print('recv:%s' % data)
        requestParams = json.loads(data)
        print(requestParams)
        serviceManager.getServiceObj(requestParams['name']).service(requestParams, response)
#         try:
#             data = self.rfile.readline().decode()
#             print('recv:%s' % data)
#             requestParams = json.loads(data)
#             print(requestParams)
#             self.serviceDict[requestParams['name']].service(requestParams, response)
#         except Exception:
#             response['state'] = 'failed'
#             response['reason'] = traceback.format_exc()
         
        self.wfile.write(json.dumps(response, ensure_ascii=False).encode())
   
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