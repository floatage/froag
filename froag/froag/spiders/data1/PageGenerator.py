#encoding=utf-8

from bs4 import BeautifulSoup, Tag
import urllib.request as ur
import re, json, sqlite3, copy, random, cx_oracle

class PageGenerator:
    DEFAULT_PARSER = 'lxml'
    FILE_DICT_DBNAME = 'pageDict.db'
    SQL_CREATE_DB = 'CREATE TABLE IF NOT EXISTS FileDict(pageId INTEGER priamry key, \
                        storeUrl text not null, \
                        requestCnt integer not null)'
    
    SQL_GET_PAGE_PATH = 'select storeUrl from FileDict where pageId=?'
    SQL_INSERT_PAGEDICT = 'insert into FileDict values(?,?,?)'
    SQL_REQUEST_PAGE = 'update FileDict set requestCnt+=1 where pageId=?'
    
    def __init__(self, template, connSL, connOR, storeDir):
        self.template = template
        self.connSL = connSL
        self.connOR = connOR
        self.picStoreDir = storeDir + 'images//'
        self.pageStoreDir = storeDir + 'pages//'
        self.downloader = PicDownloader()
        
        self.paramsDict = {}
        self.paramVisitedDict = {}
    
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
        
        self.paramsDict = paramsDict
        self.paramVisitedDict = paramsDict.fromkeys(paramsDict.keys(), 0)
        
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
                
                for index, row in enumerate(col):
                    result = ''
                    contentTree = BeautifulSoup(row)
                    for imgTag in contentTree.find_all('img'):
                        handler(imgTag)
                    for child in contentTree.body.children:
                        if isinstance(child, Tag):
                            result += str(child)
                    col[index] = result
                
                paramsDict[colName] = col
    
    def __templateStyleApply__(self, paramsDict, templateStyle):
        page = BeautifulSoup(templateStyle['layout'], self.DEFAULT_PARSER)
    
        repeatTag = page.body.find_next(repeat="true")
        while repeatTag:
            del repeatTag['repeat']
            statement = json.loads(repeatTag.attrs.get('statement', '{}'), '{}')
            if statement != {}: del repeatTag['statement']
            
            lackValue = re.search(r"\[\[.*?\]\]", str(repeatTag))
            if lackValue:
                for counter in range(0, len(paramsDict[lackValue.group(0)[2:-2]])-1):
                    copyTag = copy.deepcopy(repeatTag)
                    for attr, attrRule in statement.items():
                        if attrRule['way'] == '+r':
                            copyTag[attr] = '%s %s' % (' '.join(copyTag[attr]), random.choice(attrRule['values']))
                    repeatTag.insert_after(copyTag)
                    
            for attr, attrRule in statement.items():
                if attrRule['way'] == '+r':
                    repeatTag[attr] = '%s %s' % (' '.join(repeatTag[attr]), random.choice(attrRule['values']))
                    
            repeatTag = repeatTag.find_next(repeat="true")
        
        for jsFile in templateStyle['js']:
            jsTag = page.new_tag('script', src=jsFile)
            page.body.insert(0, jsTag)
        for cssFile in templateStyle['css']:
            cssTag = page.new_tag('link', rel="stylesheet", href=cssFile)
            page.body.insert(0, cssTag)
        
        for tag in page.body.find_all(self.__handlerLackValue__):
            pass
        
        return page
    
    def __handlerLackValue__(self, tag):
        valuePattern = re.compile(r"\[\[.*?\]\]")
        isLackValue = False
          
        if tag.string:
            lackTexts = valuePattern.findall(tag.string)
            if len(lackTexts) != 0:
                isLackValue = True
                for lackText in lackTexts:
                    lackTextValueName = lackText[2:-2]
                    tag.string = tag.string.replace(lackText, self.paramsDict[lackTextValueName][self.paramVisitedDict[lackTextValueName]])
                    self.paramVisitedDict[lackTextValueName] += 1
          
        for key, value in tag.attrs.items():
            if isinstance(value, str):
                lackAttrValues = valuePattern.findall(value)
                if len(lackAttrValues) != 0:
                    isLackValue = True
                    for lackAttrValue in lackAttrValues:
                        lackAttrValueName = lackText[2:-2]
                        tag.attrs[key] = tag.attrs[key].replace(lackAttrValue, self.paramsDict[lackAttrValueName][self.paramVisitedDict[lackAttrValueName]]) 
                        self.paramVisitedDict[lackAttrValueName] += 1
                
        return isLackValue
          
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