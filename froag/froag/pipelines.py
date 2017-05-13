# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exceptions import DropItem
import time, sqlite3
from bs4 import BeautifulSoup, Tag, Comment

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
    
class ItemStoreDBPipeline(object):
    DB_NAME = "items.db"
    SQL_CREATE_TABLE_ItemTable = '''CREATE TABLE IF NOT EXISTS ItemTable(mId INTEGER PRIMARY KEY AUTOINCREMENT, \
                         mSource text NOT NULL UNIQUE, \
                         mTitle text NOT NULL, \
                         mIntro text NOT NULL, \
                         mPic text NOT NULL, \
                         mTags text NOT NULL, \
                         mAuthor text NOT NULL,\
                         mContent text NOT NULL, \
                         mPublishYime datetime NOT NULL, \
                         mCollectTime datetime NOT NULL)
    '''
    
    SQL_ADD_NEW_ITEM = '''INSERT INTO ItemTable(mSource,mTitle,mIntro,mPic,mTags,mAuthor,mContent,mPublishYime,mCollectTime) \
         VALUES(?,?,?,?,?,?,?,?,datetime('now'))
    '''
    
    item_counter = 0
    item_store_number = 300
    item_min_number = 0
    
    def __init__(self):
        database = self.getDBConnection()
        curs = database.cursor()
        curs.execute(self.SQL_CREATE_TABLE_ItemTable)
        database.commit()
        database.close()
        self.database = self.getDBConnection()
    
    def getDBConnection(self):
        database = sqlite3.connect(self.DB_NAME)
        return database
    
    def close_spider(self, spider):
        self.database.commit()
        self.database.close()
    
    def process_item(self, item, spider):
        try:
#             print('insert start')
            self.database.execute(self.SQL_ADD_NEW_ITEM,
                    (item['msource'],item['mtitle'],item['mintro'],item['mpic'],item['mtags'],item['mauthor'],item['mcontent'],item['mtime']))
            self.item_counter = self.item_counter + 1
            if self.item_counter % self.item_store_number == 0:
                self.database.commit()
#             with self.database: 
#                 self.database.execute(self.SQL_ADD_NEW_ITEM,
#                     (item['msource'],item['mtitle'],item['mintro'],item['mpic'],item['mtags'],item['mauthor'],item['mcontent'],item['mtime']))
#                 print('insert success')
        except sqlite3.IntegrityError:
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