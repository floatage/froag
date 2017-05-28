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
    
    def __generatePage__(self, param):
        cursor = self.connOR.cursor()
        template = json.loads(self.template)
        
        datas = list(template['data'].items())
        dataQueue = map(lambda x:x[0], datas) 
        while(len(dataQueue) > 0):
            dataName = dataQueue.pop(0)
            depends = re.match(r"\[.*\]", datas['sql'])
            if depends:
                pass
            else:
                cursor.execute()
        
        result = cursor.execute(self.SQL_GET_PAGE, param['pageid'])
        item = result.fetchone()
        if item != None:
            item = list(item)
            colDict, index = {}, 0
            for col in result.description:
                colDict[col[0]] = index
                index += 1
            result = cursor.execute(self.SQL_GET_SIMILARPAGE, item[colDict['mSource']])
            similarPages = result.fetchall()
        
            text = ''
            self.__pageImageGenete__(item, colDict, self.template)
            text = self.__pageInforGenerate__(item, similarPages, colDict, self.template)
            
            filename = item[colDict['mSource']].split('//')[-1]
            storePath = self.pageStoreDir + filename
            pageFile = open(storePath, 'w')
            pageFile.write(text)
            pageFile.close()
            
            try:
                self.connS.execute(self.SQL_INSERT_PAGEDICT, (pageId, storePath, 1))
                self.connSL.commit()
            except sqlite3.IntegrityError:
                print('page:%d path insert error' % pageId)
                pass
            
        return text
    
    def __pageImageGenete__(self, item, colDict, template, way='web'):
        contentTree = BeautifulSoup(item[colDict['mContent']], self.DEFAULT_PARSER) 
        handler = self.__imageHandleWeb__ if way=='web' else self.__imageHandleWeb__
        for image in contentTree.find_all():
            handler(image)
        item[colDict['mContent']] = contentTree.find('div').prettify()
            
    def __imageHandleLocal__(self, imgTag, template):
        pass
    
    def __imageHandleWeb__(self, imgTag, template):
        pos = imgTag['src'].find(':')
        if pos != -1:
            imgTag['src'] = 'http' + imgTag['src'][pos:]
            
    def __pageInforGenerate__(self, item, similarPages, colDict, template):
        template = json.loads(template)
        template['layout']
    
class PicDownloader:
    def __init__(self):
        pass
    
    def addTask(self, url):
        pass
    
    def __picHandle(self, pic):
        pass