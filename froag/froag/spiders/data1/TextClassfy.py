#encoding=utf-8

import sqlite3, re, json
from bs4 import BeautifulSoup
import jieba.analyse, snownlp

class TextClassfy:

    DEFAULT_PARSER = 'lxml'
    ITEM_DB_NAME = 'items.db'
    SIMILAR_URL_DB_NAME = 'similarUrls.db'
    
    ITEM_COLNAME_DICT = {'mSource':1, 'mTitle':2, 'mIntro':3, 'mPic':4, 'mTags':5,
        'mAuthor':6, 'mContent':7, 'mPublishYime':8, 'mCollectTime':9}
    SQL_GET_ITEM_COUNT = "select * from ItemTable limit ?"
    SQL_GET_ITEM_URL = "select * from ItemTable where mSource=?"
    SQL_GET_SIMILAR_URL = "select similarUrl from SimilarUrl where url=?"
    
    KEYWORD_NUMBER = 20
    MAX_TAG_NUMBER = 5
    DATA_STORE_NUMBER = 50
    DATA_COUNTER = 0
    
    ACTUAL_ITEM_TABLE_NAME = 'MSG_TAB'
    ACTUAL_TAG_TABLE_NAME = 'TAG_TAB'
    SQL_INSERT_ITEM = "INSERT INTO ItemTable(mSource,mTitle,mIntro,mPic,mTags,mAuthor,mContent,mPublishYime,mCollectTime) \
        VALUES(?,?,?,?,?,?,?,?,?)"
    SQL_INSERT_TAG = 'insert into TAG_TAB(type,tag) values(?,?)'
    
    item_list, tag_map = [], {}
    
    def getRecord(self, dbname, sql, *params):
        db = sqlite3.connect(dbname)
        userInfor = db.execute(sql, params)
        result =  userInfor.fetchall()
        db.close()
        return result
    
    def getItem(self, count=10):
        return self.getRecord(self.ITEM_DB_NAME, self.SQL_GET_ITEM_COUNT, count)
    
    def getItemByUrl(self, url):
        return self.getRecord(self.ITEM_DB_NAME, self.SQL_GET_ITEM_URL, url[0])[0]
    
    def getSimilarUrl(self ,url):
        return self.getRecord(self.SIMILAR_URL_DB_NAME, self.SQL_GET_SIMILAR_URL, url)
    
    def generateTextFeature(self, item):
        content = BeautifulSoup(item[self.ITEM_COLNAME_DICT['mContent']], self.DEFAULT_PARSER)
        content = re.sub("\s", "", content.get_text())
        
        if content and len(content) != 0:
            title_words = jieba.cut(item[self.ITEM_COLNAME_DICT['mTitle']])
            jb_tags = jieba.analyse.extract_tags(content, self.KEYWORD_NUMBER, withWeight=True)
            result = snownlp.SnowNLP(content)
            sn_tags = result.keywords(self.KEYWORD_NUMBER)
            
            print('\ '.join(title_words))
            print('\ '.join(jieba.cut(item[self.ITEM_COLNAME_DICT['mTitle']], True)))
            print(sn_tags)
            print(result.keywords(self.KEYWORD_NUMBER, True))
            print(jb_tags)
            
#             result_tag = {}
#             for index, (tag, weight) in enumerate(jb_tags):
#                 if tag in sn_tags:
#                     print('%s:%f jb:%d sn:%d' % (tag, sn_tags[tag][0] / weight, index, sn_tags[tag][1]))
#                     result_tag[tag] = (weight + sn_tags[tag][0]) / 2
#                     if tag in title_words: result_tag[tag] *= 2
#              
#             top_tags = list(result_tag.items())
#             top_tags = sorted(top_tags, key=lambda x: x[1], reverse=True)
#             tag_len = len(result_tag) % (self.MAX_TAG_NUMBER + 1)
#             result_tag_text = {'type':item[self.ITEM_COLNAME_DICT['mTags']], 'tag':top_tags[:tag_len+1]}
#         
#             item = list(item)
#             item[self.ITEM_COLNAME_DICT['mTags']] = json.dumps(result_tag_text, indent=4, separators=(',', ': '))
#             self.item_list.append(item)
#             self.tag_map[item[self.ITEM_COLNAME_DICT['mTags']]] = list(result_tag.keys())
#             self.DATA_COUNTER += 1
        
    def storeDataToDb(self, items, tags):
        '''连接数据库'''
        
        '''存储数据'''
        
        for item in items:
            '''插入item'''
            pass
        
        for tag in tags.keys():
            for item in tags[tag]:
                '''插入tag'''
                pass
        
        '''关闭数据库'''

if __name__ == '__main__':
    classfier = TextClassfy()
    items = classfier.getItem(5)
 
    for item in items[:1]:
        classfier.generateTextFeature(item)
        for url in classfier.getSimilarUrl(item[classfier.ITEM_COLNAME_DICT['mSource']]):
            sItem = classfier.getItemByUrl(url)
            if sItem and len(sItem) > 0:
                classfier.generateTextFeature(sItem)
                
#         if DATA_COUNTER % DATA_STORE_NUMBER == 0:
#             storeDataToDb(item_list, tag_map)
#         content = BeautifulSoup(item[4], DEFAULT_PARSER)
#         content = re.sub("\s", "", content.get_text())
# #         result = jieba.cut(content)
# #         print(','.join(result))
# #         print('/'.join(jieba.analyse.extract_tags(content)))
#         result = snownlp.SnowNLP(content)
#         print(result.tags)
#         print(result.keywords())
#         print(result.summary())