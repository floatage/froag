# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exceptions import DropItem
import time, sqlite3, cx_Oracle
from bs4 import BeautifulSoup, Tag, Comment
import re, json
import jieba.analyse, snownlp

class EmptyItemDropPipeline(object):
    def process_item(self, item, spider):
        if item['msource']!='' and item['mcontent']!='' and item['mtitle']!='':
            if spider.name == 'eastday':
    #                print('EmptyItemDropPipeline start')
                item['mtime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) if item['mtime'] == '' else ' '.join(item['mtime'].split()[0:2])
                item['mauthor'] = spider.default_author_name if item['mauthor'] == '' else ' '.join(item['mauthor'].split()[0:1])
    #           print('EmptyItemDropPipeline success')
            elif spider.name == 'huanqiu':
                item['mtime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) if item['mtime'] == '' else item['mtime']
                item['mauthor'] = spider.default_author_name if item['mauthor'] == '' else item['mauthor']
                
            return item
        else:
#                print('EmptyItemDropPipeline error')
            raise DropItem("Missing source or content or title in %s" % item)
    
class ItemContentFormatPipeline(object):
    default_parser = 'lxml'
    deleteTag = ['script']
    replaceTag = ['a']
    
    def tagAttrClear(self, tag):
        deleteKeys = []
        for key in tag.attrs.keys():
            if key != "src":
                deleteKeys.append(key)
            else:
                tag.attrs[key] = tag.attrs[key][1:]
        for key in deleteKeys:
            del tag.attrs[key]
        
    def tagChildrenAttrClear(self, tag):
        self.tagAttrClear(tag)
        for child in tag.children:
            if  isinstance(child, Tag):
                if child.name in self.deleteTag:
                    child.decompose()
                elif child.name in self.replaceTag:
                    child.replace_with(child.string.strip())
                else:
                    self.tagAttrClear(child)
                    self.tagChildrenAttrClear(child)
            elif isinstance(child, Comment):
                child.extract()
    
    def process_item(self, item, spider):
#       print('ItemContentFormatPipeline in')
        content = BeautifulSoup(item['mcontent'], self.default_parser)
        contentTag = content.select(spider.item_content_tag_class, limit=1)[0]
        
        for deleteTag in spider.item_content_delete_tag:
            for node in contentTag.select(deleteTag):
                node.decompose()
        
#       print('ItemContentFormatPipeline start')
        if contentTag:
            self.tagChildrenAttrClear(contentTag)
            item['mcontent'] = contentTag.prettify()
#           print('ItemContentFormatPipeline success')
            return item
        else:
#           print('ItemContentFormatPipeline error')
            raise DropItem("item content class error in %s" % item)

class ItemTagExtractorPipleline(object):
    DEFAULT_PARSER = 'lxml'
    KEYWORD_NUMBER = 20
    MAX_TAG_NUMBER = 5
    DATA_STORE_NUMBER = 50
    DATA_COUNTER = 0
    
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
        content = BeautifulSoup(item['mContent'], self.DEFAULT_PARSER)
        content = re.sub("\s", "", content.get_text())
        
        if content and len(content) != 0:
            title_words = list(jieba.cut(item['mTitle']))
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
            result_tag_text = {'type':item['mTags'], 'tag':top_tag_map}

#             print(item[self.ITEM_COLNAME_DICT['mSource']])
#             print(result_tag_text)

#             print(json.dumps(result_tag_text, indent=4, separators=(',', ': ')))
            item['mTags'] = json.dumps(result_tag_text, indent=4, separators=(',', ': '))
            self.tag_map[item['mTags']] = list(result_tag.keys())
            self.DATA_COUNTER += 1
    
    def process_item(self, item, spider):
        self.generateTextFeature(item)
#         if DATA_COUNTER % DATA_STORE_NUMBER == 0:
#             storeDataToDb(item_list, tag_map)
    
class ItemStoreDBPipeline(object):
#     DB_NAME = "items.db"
#     SQL_CREATE_TABLE_ItemTable = '''CREATE TABLE IF NOT EXISTS ItemTable(mId INTEGER PRIMARY KEY AUTOINCREMENT, \
#                          mSource text NOT NULL UNIQUE, \
#                          mTitle text NOT NULL, \
#                          mIntro text NOT NULL, \
#                          mPic text NOT NULL, \
#                          mTags text NOT NULL, \
#                          mAuthor text NOT NULL,\
#                          mContent text NOT NULL, \
#                          mPublishYime datetime NOT NULL, \
#                          mCollectTime datetime NOT NULL)
#     '''
#     
#     SQL_ADD_NEW_ITEM = '''INSERT INTO ItemTable(mSource,mTitle,mIntro,mPic,mTags,mAuthor,mContent,mPublishYime,mCollectTime) \
#          VALUES(?,?,?,?,?,?,?,?,datetime('now'))
#     '''
    
    item_counter = 0
    item_store_number = 300
    item_min_number = 0
    
#     def __init__(self):
#         database = self.getDBConnection()
#         curs = database.cursor()
#         curs.execute(self.SQL_CREATE_TABLE_ItemTable)
#         database.commit()
#         database.close()
#         self.database = self.getDBConnection()
    DB_CONNECT_STRING = 'foragCollecter_1/foragCollecter@10.18.50.229/orcl'
    SQL_ADD_NEW_ITEM = '''INSERT INTO foragOwner.MsgTable(mId,mSource,mTitle,mIntro,mPic,mTags,mAuthor,mContent, \
                    mPublishYime,mCollectTime,mLikeCount,mDislikeCount,mCollectCount,mTransmitCount) \
         VALUES(foragOwner.MID_SEQ.NEXTVAL,?,?,?,?,?,?,?,?,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'),0,0,0,0)
    '''

    def __init__(self):
        self.database = cx_Oracle.connect(self.DB_CONNECT_STRING)
        self.cursor = self.database.cursor()

#     def getDBConnection(self):
#         database = sqlite3.connect(self.DB_NAME)
#         return database
    
    def close_spider(self, spider):
        self.database.commit()
        self.database.close()
    
    def process_item(self, item, spider):
        try:
#             print('insert start')
            self.cursor.execute(self.SQL_ADD_NEW_ITEM,
                    (item['msource'],item['mtitle'],item['mintro'],item['mpic'],item['mtags'],item['mauthor'],item['mcontent'],item['mtime']))
            self.item_counter = self.item_counter + 1
            if self.item_counter % self.item_store_number == 0:
                self.database.commit()
#             with self.database: 
#                 self.database.execute(self.SQL_ADD_NEW_ITEM,
#                     (item['msource'],item['mtitle'],item['mintro'],item['mpic'],item['mtags'],item['mauthor'],item['mcontent'],item['mtime']))
#                 print('insert success')
        except cx_Oracle.IntegrityError:
#         except sqlite3.IntegrityError:
#             print('insert error')
            raise DropItem("insert item error in %s" % item)
        
        return item
    
# def tagAttrClear(tag):
#     deleteKeys = []
#     for key in tag.attrs.keys():
#         if key != "src":
#             deleteKeys.append(key)
#         else:
#             tag.attrs[key] = tag.attrs[key][1:]
#     for key in deleteKeys:
#         del tag.attrs[key]
#         
# def tagChildrenAttrClear(tag):
#     deleteTag = ["script"]
#     tagAttrClear(tag)
#     for child in tag.children:
#         if  isinstance(child, Tag):
#             if child.name in deleteTag:
#                 child.extract()
#             else:
#                 tagAttrClear(child)
#                 tagChildrenAttrClear(child)
            
# if __name__ == "__main__":
#     content = '''<div class="J-contain_detail_cnt contain_detail_cnt" id="J-contain_detail_cnt">
#         <p style="text-indent: 2em; line-height: 2em; text-align: center;"><span style='font-family: 微软雅黑, "Microsoft YaHei";'>
#         <img src="//imgmini.eastday.com/pushimg/20170427/1493297888117362.jpeg" title="1493297888117362.jpeg" alt="1.JPEG"&gt;&lt;/span></p><p style="text-indent: 2em; line-height: 2em;"><span style='font-family: 微软雅黑, "Microsoft YaHei";'>众所周知，姜文的现任老婆是周韵，两人在《太阳照常升起》、《让子弹飞》、《一步之遥》中都有合作。此前，姜文曾经有过一段婚姻，与法国女人桑德琳。而姜文的第一段恋情却是给了大名鼎鼎的娱乐圈风云人物刘晓庆。</span></p><p style="text-indent: 2em; line-height: 2em; text-align: center;"><span style='font-family: 微软雅黑, "Microsoft YaHei";'><img src="//imgmini.eastday.com/pushimg/20170427/1493298092361104.jpeg" title="1493298092361104.jpeg" alt="2.JPEG"></span></p><p style="text-indent: 2em; line-height: 2em;"><span style='font-family: 微软雅黑, "Microsoft YaHei";'>说到刘晓庆和姜文的恋情，还不得不提到谢晋与《芙蓉镇》。1986年，电影《芙蓉镇》促成了刘晓庆和姜文的相识相恋。当时，姜文23岁，是刚毕业的话剧演员，谢晋导演眼中中央戏剧学院最优秀的学生;刘晓庆31岁，已是影后级明星。</span></p><script type="text/javascript">goBackHome(page_num);</script></div>
#     '''
    
#     content = """
#         &lt;div class="J-contain_detail_cnt contain_detail_cnt" id="J-contain_detail_cnt"&gt;
#                         &lt;p style="text-indent: 2em; line-height: 2em; text-align: center;"&gt;&lt;span style='font-family: 微软雅黑, "Microsoft YaHei";'&gt;&lt;img src="//imgmini.eastday.com/pushimg/20170427/1493297888117362.jpeg" title="1493297888117362.jpeg" alt="1.JPEG"&gt;&lt;/span&gt;&lt;/p&gt;&lt;p style="text-indent: 2em; line-height: 2em;"&gt;&lt;span style='font-family: 微软雅黑, "Microsoft YaHei";'&gt;众所周知，姜文的现任老婆是周韵，两人在《太阳照常升起》、《让子弹飞》、《一步之遥》中都有合作。此前，姜文曾经有过一段婚姻，与法国女人桑德琳。而姜文的第一段恋情却是给了大名鼎鼎的娱乐圈风云人物刘晓庆。&lt;/span&gt;&lt;/p&gt;&lt;p style="text-indent: 2em; line-height: 2em; text-align: center;"&gt;&lt;span style='font-family: 微软雅黑, "Microsoft YaHei";'&gt;&lt;img src="//imgmini.eastday.com/pushimg/20170427/1493298092361104.jpeg" title="1493298092361104.jpeg" alt="2.JPEG"&gt;&lt;/span&gt;&lt;/p&gt;&lt;p style="text-indent: 2em; line-height: 2em;"&gt;&lt;span style='font-family: 微软雅黑, "Microsoft YaHei";'&gt;说到刘晓庆和姜文的恋情，还不得不提到谢晋与《芙蓉镇》。1986年，电影《芙蓉镇》促成了刘晓庆和姜文的相识相恋。当时，姜文23岁，是刚毕业的话剧演员，谢晋导演眼中中央戏剧学院最优秀的学生;刘晓庆31岁，已是影后级明星。&lt;/span&gt;&lt;/p&gt;                        &lt;script type="text/javascript"&gt;goBackHome(page_num);&lt;/script&gt;
#                     &lt;/div&gt;
#     """
#     content = content.encode('utf8')
#     print(content)
#     content = BeautifulSoup(content, 'lxml')
#     contentTag = content.find('div', 'J-contain_detail_cnt')
#     if contentTag:
#         tagChildrenAttrClear(contentTag)
#     print(contentTag.prettify())