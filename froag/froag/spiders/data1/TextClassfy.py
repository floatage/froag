#encoding=utf-8

import sqlite3, re, json, os
from bs4 import BeautifulSoup
import jieba.analyse, snownlp
import cx_Oracle

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
    
    def clacWordWeight(self, jb_tags, sn_tags, title_words):
        result_tags, min_tag_number, tag_rate = {}, 0, 0.0
        for (tag, weight) in jb_tags:
            if tag in sn_tags:
                tag_rate += sn_tags[tag][0] / weight
                result_tags[tag] = [weight, sn_tags[tag][0]]
#                 print('%s:%f jb:%d sn:%d' % (tag, sn_tags[tag][0] / weight, index, sn_tags[tag][1]))
        tag_rate /= len(result_tags)
        for tag, [jbw, snw] in result_tags.items():
            result_tags[tag] = (jbw * tag_rate + snw) / 2
#             print('tag:%s rate:%f sn:%f jb:%f result:%f' % (tag, tag_rate, snw, jbw, result_tags[tag]))
            if tag in title_words:
                min_tag_number += 1
                result_tags[tag] *= 4
#                 print(result_tags[tag])
                
#             print('rate:%f sn:%f jb:%f result:%f' % (tag_rate, snw, jbw, result_tags[tag]))
        
        tag_len = max(min_tag_number, self.MAX_TAG_NUMBER)
        return result_tags, tag_len
    
    def generateTextFeature(self, item):
        content = BeautifulSoup(item[self.ITEM_COLNAME_DICT['mContent']], self.DEFAULT_PARSER)
        content = re.sub("\s", "", content.get_text())
        
        if content and len(content) != 0:
            title_words = list(jieba.cut(item[self.ITEM_COLNAME_DICT['mTitle']]))
            jb_tags = jieba.analyse.extract_tags(content, self.KEYWORD_NUMBER, withWeight=True)
            result = snownlp.SnowNLP(content)
            sn_tags = result.keywords(self.KEYWORD_NUMBER)
            
#             print('\ '.join(title_words))
#             print('\ '.join(jieba.cut(item[self.ITEM_COLNAME_DICT['mTitle']], True)))
#             print(jb_tags)
#             print(sn_tags)
#             print(result.keywords(self.KEYWORD_NUMBER, True))
            
            
            result_tag, tag_len = self.clacWordWeight(jb_tags, sn_tags, title_words)
            top_tags = list(result_tag.items())
            top_tags = sorted(top_tags, key=lambda x: x[1], reverse=True)
#             print(top_tags[:tag_len])
            top_tag_map = {}
            for index in range(0,tag_len):
                top_tag_map[top_tags[index][0]] = top_tags[index][1]
            result_tag_text = {'type':item[self.ITEM_COLNAME_DICT['mTags']], 'tag':top_tag_map}

#             print(item[self.ITEM_COLNAME_DICT['mSource']])
#             print(result_tag_text)

            item = list(item)
#             print(json.dumps(result_tag_text, indent=4, separators=(',', ': ')))
            item[self.ITEM_COLNAME_DICT['mTags']] = json.dumps(result_tag_text,  ensure_ascii=False, indent=4, separators=(',', ': '))
            self.item_list.append(item)
            self.tag_map[item[self.ITEM_COLNAME_DICT['mTags']]] = list(result_tag.keys())
            self.DATA_COUNTER += 1
        
    def storeDataToDb(self):
        '''连接数据库'''
        
        '''存储数据'''
        
        for item in self.item_list:
            '''插入item'''
            pass
        self.item_list.clear()
        
        for tag in self.tag_map.keys():
            for item in self.tag_map[tag]:
                '''插入tag'''
                pass
        self.tag_map.clear()
        '''关闭数据库'''

if __name__ == '__main__':
    DB_CONNECT_STRING_ORACLE = 'foragCollecter_1/foragCollecter@10.18.50.229/orcl'
    SQL_ORACLE_ADD_RELATION = 'INSERT INTO foragOwner.UrlRelation(sourceUrl,parentUrl) VALUES(:1,:2)'
    SQL_ORACLE_ADD_SIMILARURL = 'INSERT INTO foragOwner.SimilarUrl(sourceUrl,similarUrl) VALUES(?,?)'
    SQL_ADD_NEW_ITEM = '''INSERT INTO foragOwner.MsgTable(mId,mSource,mTitle,mIntro,mPic,mTags,mAuthor,mContent,mPublishTime, \
        mCollectTime,mLikeCount,mDislikeCount,mCollectCount,mTransmitCount) \
        VALUES(foragOwner.MID_SEQ.NEXTVAL,:1,:2,:3,:4,:5,:6,:7,to_date(:8,'yyyy-mm-dd hh24:mi:ss'),sysdate,0,0,0,0)
    '''
    
    item = {'mauthor': '环球网',
     'mcontent': '''<div>
                  <p>
                   <img 
                 src="tp://himg2.huanqiu.com/attachment2010/2017/0516/16/49/20170516044903833.jpg"/>\n
                  </p>
                  <p>【环球网综合报道】据英国《每日邮报》5月15日报道，英国女子苏曼自打出生起每日都要拍一张肖像照，如今21岁的她已集齐了7665张，这些照片着实记录着她一天天的成长。
                 </p>
                 <p>
                 <img
                 src="tp://himg2.huanqiu.com/attachment2010/2017/0516/16/49/20170516044919429.jpg"/>
                 </p>
                 <p>
                 苏曼的父亲是一位摄影师。他一开始每天给苏曼拍照是为了让苏曼远在印度的祖父母能看到她一天天长大的模样。那时，她的父亲每天都定好闹钟，以防忘记这件事。有的时候，小苏曼都睡着了，还得被爸爸叫醒，起来拍照。这似乎成了家中的一种仪式。这个传统本来会因为苏曼去上大学而无法继续下去，但是既然老爸拍不着了，那就自拍呗。于是，这个习惯一直坚持至今。
                  </p>
                  <p>
                  <img
                 src="tp://himg2.huanqiu.com/attachment2010/2017/0516/16/49/20170516044931547.jpg"/>
                  </p>
                 <p>
                 2017年5月16日是苏曼21岁生日，她已经拥有7665张肖像照，快赶上普通人家一家人的照片总量了!(实习编译：汪燕妮 审稿：朱盈库)
                 </p>
                 </div>''',
     'mintro': '',
     'mpic': 'http://himg2.huanqiu.com/attachment2010/2017/0516/16/49/20170516044903833.jpg ',
     'msource': 'http://look.huanqiu.com/article/2017-05/06811342.html',
     'mtags': '{"国际": "低洼", "打完": "滚刀肉"}',
     'mtime': '2017-05-17 07:40:00',
     'mtitle': '英女子自出生起每日拍一张肖像照 现已集齐7665张'}

    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    database = cx_Oracle.connect(DB_CONNECT_STRING_ORACLE)
    cursor = database.cursor()
    
    cursor.execute(SQL_ADD_NEW_ITEM, (item['msource'],item['mtitle'],item['mintro'],item['mpic'],
        item['mtags'],item['mauthor'],item['mcontent'],item['mtime']))
    database.commit()
    print('conmmit success')
    result = cursor.execute('select * from foragOwner.msgtable').fetchall()
 
    for item in result:
        for field in item:
            print(field)
                
    database.close()
#     classfier = TextClassfy()
#     items = classfier.getItem(5)
#  
#     for item in items[:1]:
#         classfier.generateTextFeature(item)
#         for url in classfier.getSimilarUrl(item[classfier.ITEM_COLNAME_DICT['mSource']]):
#             sItem = classfier.getItemByUrl(url)
#             if sItem and len(sItem) > 0:
#                 classfier.generateTextFeature(sItem)
                
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