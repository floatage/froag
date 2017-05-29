#encoding=utf-8

from bs4 import BeautifulSoup
import urllib.request as ur
import re, json, sqlite3, cx_oracle

class PageGenerator:
    FILE_DICT_DBNAME = 'pageDict.db'
    SQL_CREATE_DB = 'CREATE TABLE IF NOT EXISTS FileDict(pageId INTEGER priamry key, \
                        storeUrl text not null, \
                        requestCnt integer not null)'
    
    DEFAULT_PARSER = 'lxml'
    SQL_GET_PAGE_PATH = 'select storeUrl from FileDict where pageId=?'
    SQL_GET_PAGE = 'select * from MsgTable where mid=?'
    SQL_GET_SIMILARPAGE = 'select * from msgtable,(select similarurl from similarurl where sourceurl=?) s where msource=similarurl'
    SQL_INSERT_PAGEDICT = 'insert into FileDict values(?,?,?)'
    SQL_REQUEST_PAGE = 'update FileDict set requestCnt+=1 where pageId=?'
    
    def __init__(self, template, connSL, connOR, storeDir):
        self.template = template
        self.connSL = connSL
        self.connOR = connOR
        self.picStoreDir = storeDir + 'images//'
        self.pageStoreDir = storeDir + 'pages//'
        self.downloader = PicDownloader()
    
    def __createDB__(self):
        db = sqlite3.connect(self.FILE_DICT_DBNAME)
        cursor = db.cursor()
        cursor.execute(self.SQL_CREATE_DB)
        db.commit()
        db.close()
    
    def getPage(self, pageId):
        path = self.__pageSearch__(pageId)
        text = ''
        if path != '':
            text = self.__getPage__(path)
        else:
            text = self.__generatePage__(pageId)
            
        return text
    
    def __pageSearch__(self, pageId):
        cursor = self.connSL.cursor()
        rs = cursor.execute(self.SQL_GET_PAGE_PATH, pageId)
        result = rs.fetchone()
        
        path = ''
        if result != None: 
            path = result[0]
            cursor.execute(self.SQL_REQUEST_PAGE, {'pageid':pageId})
            self.connSL.commit()
        
        return path
    
    def __getPage__(self, path):
        text = ''
        try:
            file = open(path, 'r')
            text = file.read()
        except IOError:
            pass
        finally:
            file.close()
        return text
    
    def __generatePage__(self, params):
        template = json.loads(self.template)
        paramsDict = self.__templateDataFetch(self.connOR.cursor(), params, template['data'])
        self.__templateRuleApply__(paramsDict, template['rule'])
        page = self.__templateStyleApply__(paramsDict, template['style'])
            
#         filename = item[colDict['mSource']].split('//')[-1]
#         storePath = self.pageStoreDir + filename
#         pageFile = open(storePath, 'w')
#         pageFile.write(text)
#         pageFile.close()
#             
#         try:
#             self.connS.execute(self.SQL_INSERT_PAGEDICT, (param['pageid'], storePath, 1))
#             self.connSL.commit()
#         except sqlite3.IntegrityError:
#             print('page:%d path insert error' % param['pageid'])
#             pass
            
        return page
    
    def __templateDataFetch(self, cursor, params, templateDataStatements):
        valuePattern = re.compile(r"\[\[.*?\]\]")
        
        paramsDict = {}
        for key,value in params.items():
            paramsDict['params.' + key] = value
            
        dataStatemetNames, dataStatemetLen = list(templateDataStatements.keys()), len(templateDataStatements)
        statemetSeq, dataStatemetStack = ['params'], []
        while (len(statemetSeq) == dataStatemetLen):
            if (len(dataStatemetStack) == 0):
                dataStatemetStack.append((dataStatemetNames.pop(), 'params'))
                
            dataStatemet = dataStatemetStack[-1]
            depends = valuePattern.findall(templateDataStatements[dataStatemet[0]]['sql'])
            depends = set(map(lambda x:x[2:-2].split('.')[0], depends))
            
            isFinished = True
            for depend in depends:
                if depend == dataStatemet[1] and depend != 'params':
                    raise ValueError('statement cross reference')
                if depend not in statemetSeq:
                    isFinished = False
                    dataStatemetStack.append((depend, dataStatemet[0]))
            if isFinished:
                sta = dataStatemetStack.pop()
                if sta[0] not in statemetSeq:
                    statemetSeq.append(sta[0])
        
        for statement in statemetSeq:
            sql = templateDataStatements[statement]['sql']
            depends = valuePattern.findall(sql)
            
            rs = None
            if len(depends) > 0: 
                sqlValues = []
                sql = valuePattern.sub('?', sql)
                for depend in depends:
                    depend = depend[2:-2]
                    sqlValues.append(paramsDict[depend])
                rs = cursor.execute(sql, sqlValues).fetchall()
            else:
                rs = cursor.execute(sql).fetchall()
                
            if len(rs) > 0:
                for index, col in enumerate(cursor.description):
                    paramsDict['%s.%s' % (statement, col[0])] = list(map(lambda x:x[index], rs))
        
        return paramsDict
    
    def __templateRuleApply__(self, paramsDict, templateRules):
        if "imgHandler" in templateRules:
            cols = json.loads(templateRules["imgHandler"])
            handler = self.__imageHandleWeb__ if templateRules["imgHandler"]=='web' else self.__imageHandleWeb__
            for col in cols:
                colName = col[2:-2]
                for row in paramsDict[colName]:
                    handler(row)
    
    def __templateStyleApply__(self, paramsDict, templateStyle):
        page = ''
        return page
            
    def __imageHandleLocal__(self, imgTag, template):
        pass
    
    def __imageHandleWeb__(self, imgTag):
        pos = imgTag['src'].find(':')
        if pos != -1:
            imgTag['src'] = 'http' + imgTag['src'][pos:]
    
class PicDownloader:
    def __init__(self):
        pass
    
    def addTask(self, url):
        pass
    
    def __picHandle(self, pic):
        pass