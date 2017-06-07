#encoding=utf8

import socketserver, json, threading, multiprocessing
import os, sqlite3, codecs, re, cx_Oracle
import PageGenerator

class ForagInterfaceHandler(socketserver.StreamRequestHandler):
    
    def setup(self):
        super(ForagInterfaceHandler, self).setup()
         
        self.serviceDict = {}
        self.serviceDict['getUserInteres'] = GetUserInterestPageService()
        self.serviceDict['generatePage'] = GeneratePageService()
        self.serviceDict['uploadPageTemplate'] = UploadPageTemplateService()
        self.serviceDict['uploadPageSchedule'] = UploadPageTemplateService()
        self.serviceDict['uploadSpider'] = UploadSpiderService()
    
    def registerService(self, name, serviceObj):
        self.serviceDict[name] = serviceObj
        
    def unregisterService(self, name):
        self.serviceDict.pop(name, "specified service don't existed")
    
    def handle(self):
        print('client ip:%s port:%s' % self.client_address)
        
        response = {}
        
        try:
            data = self.rfile.readline().decode()
            print('recv:%s' % data)
            requestParams = json.loads(data)
            print(requestParams)
            self.serviceDict[requestParams['name']].service(requestParams, response)
        except Exception as e:
            response['state'] = 'failed'
            response['reason'] = str(e)
            print(e)
         
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

# {"name":"getTagArticle","params":{"tag":"数据库","len":"10"}}
class GetTagArticleService:
    def service(self, params, response):
        params['len']
        response['data'] = "GetTagArticleService"

# {"name":"getHotArticle","params":{"len":"10"}}
class GetHotArticleService:
    def service(self, params, response):
        params['len']
        response['data'] = "GetHotArticleService"

#userid可用作协同过滤
# {"name":"generatePage","params":{"pageid":"12687","template":"articleTemplate.json", "userid":"123"}}
class GeneratePageService:
    FILE_DICT_DBNAME = 'pageDict.db'
    SQL_CREATE_DB = 'CREATE TABLE IF NOT EXISTS FileDict(pageId INTEGER primary key, \
                        storeUrl text not null, \
                        requestCnt integer not null)'
    DB_CONNECT_STRING_ORACLE = 'foragCollecter_1/foragCollecter@10.18.50.229/orcl'
    PAGETEMPLATE_DIR = "pageTemplate//"
    
    def __init__(self):
        self.__createDB()
        
        os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
        self.orConn = cx_Oracle.connect(self.DB_CONNECT_STRING_ORACLE)
        self.slConn = sqlite3.connect(self.FILE_DICT_DBNAME)
        
        self.templateDict = {}
        self.generator = PageGenerator.PageGenerator(self.slConn, self.orConn, 'articles')
    
    def __createDB(self):
        db = sqlite3.connect(self.FILE_DICT_DBNAME)
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

# {"name":"uploadPageTemplate","params":{"name":"模板一","file":"文件数据"}}
class UploadPageTemplateService:
    def service(self, params, response):
        response['data'] = "UploadPageTemplateService"

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