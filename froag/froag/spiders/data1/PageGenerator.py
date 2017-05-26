from bs4 import BeautifulSoup
import urllib.request as ur
import re, os, sqlite3, cx_oracle

class PageGenerator:
    FILE_DICT_DBNAME = 'pageDict.db'
    SQL_CREATE_DB = 'CREATE TABLE IF NOT EXISTS FileDict(pageId INTEGER priamry key, \
                        storeUrl text not null, \
                        requestCnt integer not null)'
    
    DEFAULT_PARSER = 'lxml'
    SQL_GET_PAGE_PATH = 'select storeUrl from FileDict where pageId=?'
    SQL_GET_PAGE = 'select * from MsgTable where mid=?'
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
            text = self.__pageGetFromFile__(path)
        else:
            text = self.__pageGetFromDB__(pageId)
            
        return text
    
    def __pageSearch__(self, pageId):
        cursor = self.connSL.cursor()
        rs = cursor.execute(self.SQL_GET_PAGE_PATH, pageId)
        result = rs.fetchone()
        
        path = ''
        if result != None: 
            path = result[0]
            cursor.execute(self.SQL_REQUEST_PAGE, pageId)
            self.connSL.commit()
        
        return path
    
    def __pageGetFromFile__(self, path):
        text = ''
        try:
            file = open(path, 'r')
            text = file.read()
        except IOError:
            pass
        finally:
            file.close()
        return text
    
    def __pageGetFromDB__(self, pageId):
        cursor = self.connOR.cursor()
        result = cursor.execute(self.SQL_GET_PAGE, pageId)
        item = result.fetchone()
        text = ''
        if item != None:
            colDict, index = {}, 0
            for col in result.description:
                colDict[col[0]] = index
                index += 1
            
            contentTree = BeautifulSoup(item[colDict['mContent']], self.DEFAULT_PARSER) 
            self.__pageImageGenete__(contentTree)
            self.__pageInforGenete__(item, contentTree, colDict, self.template)
            text = contentTree.prettify()
            
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
    
    def __pageImageGenete__(self, contentTree):
        for image in contentTree.find_all():
            url = image.attr['']
            self.downloader.addTask(url)
    
    def __pageInforGenete__(self, contentTree, colDict, template):
        pass
    
class PicDownloader:
    def __init__(self):
        pass
    
    def addTask(self, url):
        pass
    
    def __picHandle(self, pic):
        pass
    