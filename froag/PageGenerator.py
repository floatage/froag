#encoding=utf-8

from bs4 import BeautifulSoup
import re, json, sqlite3, copy, random, os, codecs, cx_Oracle

class PageGenerator:
    DEFAULT_PARSER = 'lxml'
    
    SQL_GET_PAGE_PATH = 'select storeUrl from FileDict where pageId=:1'
    SQL_INSERT_PAGEDICT = 'insert into FileDict values(:1,:2,:3)'
    SQL_REQUEST_PAGE = 'update FileDict set requestCnt=requestCnt+1 where pageId=:1'
    
    def __init__(self, connSL, connOR, storeDir):
        self.connSL = connSL
        self.connOR = connOR
        self.picStoreDir = storeDir + '//images//'
        self.pageStoreDir = storeDir + '//pages//'
        self.downloader = PicDownloader()
        
        self.paramsDict = {}
        self.paramVisitedDict = {}
    
    def getPage(self, pageId, template):
        pageId = int(pageId)
        path = self.__pageSearch(pageId)
        text = ''
        if path != '':
            text = self.__getPage(path)
        else:
            text = self.__generatePage({'pageid':[pageId]}, template)
            
        return text
    
    def __pageSearch(self, pageId):
        cursor = self.connSL.cursor()
        rs = cursor.execute(self.SQL_GET_PAGE_PATH, (pageId,))
        result = rs.fetchone()
        
        path = ''
        if result != None: 
            path = result[0]
            cursor.execute(self.SQL_REQUEST_PAGE, (pageId,))
            self.connSL.commit()
        
        return path
    
    def __getPage(self, path):
        text = ''
        try:
            file = codecs.open(path, 'r', encoding='utf-8')
            text = file.read()
        except IOError:
            pass
        finally:
            file.close()
        return text
    
    def __generatePage(self, params, template):
        template = json.loads(template)
        paramsDict = self.__templateDataFetch(self.connOR.cursor(), params, template['data'])
        
        self.paramsDict = paramsDict
        self.paramVisitedDict = paramsDict.fromkeys(paramsDict.keys(), 0)
        
        self.__templateRuleApply(paramsDict, template['rule'])
        page = self.__templateStyleApply(paramsDict, template['style'])
            
        storePath = self.pageStoreDir + str(params['pageid'][0]) + '.txt'
        pageFile = codecs.open(storePath, 'w', encoding='utf-8')
        pageFile.write(page)
        pageFile.close()
                
        try:
            self.connSL.execute(self.SQL_INSERT_PAGEDICT, (params['pageid'][0], storePath, 1))
            self.connSL.commit()
        except sqlite3.IntegrityError:
            print('page:%d path insert error' % params['pageid'][0])
            pass
            
        return page
    
    def __templateDataFetch(self, cursor, params, templateDataStatements):
        valuePattern = re.compile(r"\[\[.*?\]\]")
        
        paramsDict = {}
        for key,value in params.items():
            paramsDict['params.' + key] = value
            
        dataStatemetNames, dataStatemetLen = list(templateDataStatements.keys()), len(templateDataStatements)+1
        statemetSeq, dataStatemetStack = ['params'], []
        while (len(statemetSeq) != dataStatemetLen):
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
        
        for statement in statemetSeq[1:]:
            sql = templateDataStatements[statement]['sql']
            depends = valuePattern.findall(sql)
            
            rs = None
            if len(depends) > 0: 
                sqlValues = []
                
                for index, depend in enumerate(depends):
                    sql = sql.replace(depend, ':%d' % index)
                    sqlValues.append(paramsDict[depend[2:-2]][0])
                rs = cursor.execute(sql, sqlValues).fetchall()
            else:
                rs = cursor.execute(sql).fetchall()
                
            if len(rs) > 0:
                for index, col in enumerate(cursor.description):
                    paramsDict['%s.%s' % (statement, col[0])] = list(map(lambda x:x[index], rs))
        
        return paramsDict
    
    def __templateRuleApply(self, paramsDict, templateRules):
        if "imghandler" in templateRules:
            cols, way = templateRules["imghandler"]['col'], templateRules["imghandler"]['way']
            handler = self.__imageHandleWeb if way=='web' else self.__imageHandleWeb
            for col in cols:
                colName = col[2:-2]
                
                for index, row in enumerate(paramsDict[colName]):
                    contentTree = BeautifulSoup(row, self.DEFAULT_PARSER)
                    for imgTag in contentTree.find_all('img'):
                        handler(imgTag)
                    
                    paramsDict[colName][index] = contentTree.body.prettify()[7:-8]
    
    def __templateStyleApply(self, paramsDict, templateStyle):
        page = ''
        for (sectionKey, sectionValue) in templateStyle['layout']:
            for paramKey in paramsDict.keys():
                if paramKey.find(sectionKey) != -1:
                    page = page + sectionValue
                    break
        page = BeautifulSoup(page, self.DEFAULT_PARSER)
    
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
        
        for tag in page.body.find_all(self.__handlerLackValue):
            pass
        
        page = page.body.prettify()[7:-8]
        return page
    
    def __handlerLackValue(self, tag):
        valuePattern = re.compile(r"\[\[.*?\]\]")
        isLackValue = False
        
        if tag.string:
            lackTexts = valuePattern.findall(tag.string)
            if len(lackTexts) != 0:
                isLackValue = True
                replaceWay = json.loads(tag.attrs.get('replaceway', '{}')).get('bs', [])
                if len(replaceWay) != 0: del tag.attrs['replaceway'] 
                
                for index, lackText in enumerate(lackTexts):
                    lackTextValueName = lackText[2:-2]
                    lackTextValue = str(self.paramsDict[lackTextValueName][self.paramVisitedDict[lackTextValueName]])
                    if index in replaceWay:
                        tag.string = tag.string.replace(lackText, '')
                        lackTextValue = BeautifulSoup(lackTextValue, self.DEFAULT_PARSER).body
                        replaceTag = lackTextValue.contents[0]
                        if replaceTag.name != 'div':
                            replaceTag = lackTextValue.body.wrap(lackTextValue.new_tag('div'))
                            replaceTag.div.body.unwrap()
                            replaceTag = replaceTag.div
                            
                        tag.append(replaceTag)
                    else:
                        tag.string = tag.string.replace(lackText, lackTextValue)
                    self.paramVisitedDict[lackTextValueName] += 1
        
        for key, value in tag.attrs.items():
            if isinstance(value, str):
                lackAttrValues = valuePattern.findall(value)
                if len(lackAttrValues) != 0:
                    isLackValue = True
                    for lackAttrValue in lackAttrValues:
                        lackAttrValueName = lackAttrValue[2:-2]
                        tag.attrs[key] = tag.attrs[key].replace(lackAttrValue, str(self.paramsDict[lackAttrValueName][self.paramVisitedDict[lackAttrValueName]]))
                        self.paramVisitedDict[lackAttrValueName] += 1
                
        return isLackValue
          
    def __imageHandleLocal(self, imgTag, template):
        pass
    
    def __imageHandleWeb(self, imgTag):
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
    
if __name__ == "__main__":
    file = codecs.open(r'pageTemplate/articleTemplate.json', 'r', encoding='utf8')
    template = re.sub(r'\s+', " ", file.read())
    file.close()
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    DB_CONNECT_STRING_ORACLE = 'foragCollecter_1/foragCollecter@10.18.50.229/orcl'
    conn = cx_Oracle.connect(DB_CONNECT_STRING_ORACLE)
    slConn = sqlite3.connect('pageDict.db')
    pg = PageGenerator(slConn, conn, "articles")
    print(pg.getPage(12687, template))