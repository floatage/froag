#encoding=utf-8
import cx_Oracle, os, re, json, copy, random, socket, time, sched, copy, datetime, functools
import urllib.request as ur
from bs4 import BeautifulSoup, Tag

paramsDict = {'item.MTAG':[1, 2], 'item-similar.MTITLE':[1, 2], 'item.MTITLE':[1]}
paramVisitedDict = paramsDict.fromkeys(paramsDict.keys(), 0)
 
#初始化sched模块的scheduler类
#第一个参数是一个可以返回时间戳的函数，第二个参数可以在定时未到达之前阻塞。
# s = sched.scheduler(time.time,time.sleep)
#  
# #enter四个参数分别为：间隔事件、优先级（用于同时间到达的两个事件同时执行时定序）、被调用触发的函数，给他的参数（注意：一定要以tuple给如，如果只有一个参数就(xx,)）
# def perform(inc):
#     s.enter(inc,0,perform,(inc,))
#     print ("Current Time:",time.time())
#     
# def mymain(inc):
#     s.enter(inc,0,perform,(inc,))
#     s.run()

class a:
    def __init__(self):
        print('2')
        self.l = []

if __name__ == '__main__':
#     a = a()
#     b = copy.deepcopy(a)
#     print(a, a.l, b, b.l)
#     mymain(2)
#     sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     sock.connect(('127.0.0.1', 9999))
#     print('connect')
#                    
#     while True:
#         line = input('> ')
#         sock.send(('%s\r\n' % line).encode())
#         print(sock.recv(10240))
#                        
#     sock.close()
    d = {1:3,2:1,3:4,4:2}
    print(sorted(d.items(), key=lambda x:x[1], reverse=True))

#     s = 'jAva'
#     print(s.capitalize())
#     l = [[1,1],[2,2],[1,3],[4,5]]
#     print(list({i[0]:i for i in l}.values()))
#     d = {1:1,2:2,3:3,4:4}
#     for key,value in d.items():
#         print(key, value)
#     print(functools.reduce(lambda x,y:float(x)+float(y), [1.1,1.2,1.3,1.4,1.5,1.6]))
#     l = [1,2,3,4,5,6,7]
#     print(random.shuffle(l))
#     print(l)
#     d = {1:1, 2:2, 3:3}
#     print(list(filter(lambda x:x[0] not in d, [[2],[4],[3]])))
#     d = {1:1, 2:1, 3:2, 5:3}
#     print(random.randint(1.2, 1.3))
#     d = {1:1,2:2}
#     for item, (key, value) in enumerate(d.items()):
#         print(item,key, value)
#     d = {1:1,2:2,3:3}
#     print(d[1:2])
#     dateFormat = '%Y-%m-%d %H:%M:%S'
#     interval = datetime.datetime.strptime('2017-06-19 21:47:47', dateFormat) - datetime.datetime.strptime('2017-06-18 22:38:47', dateFormat)
#     print(interval.seconds)

#     now = datetime.datetime.now()
#     print(now.strftime('%m-%d'))

#     c = [7,6,1]
#     insertSortInsert(c, 10, lambda x,y:x>=y)
#     insertSortInsert(c, 8, lambda x,y:x>=y)
#     insertSortInsert(c, 0, lambda x,y:x>=y)
#     insertSortInsert(c, 11, lambda x,y:x>=y)
#     insertSortInsert(c, -1, lambda x,y:x>=y)
#     print(c)

#     file = open('articles//pages//test.txt', 'w')
#     file.close()
#     b1, b2 = BeautifulSoup('daw', 'lxml'), BeautifulSoup('<p>da</p><p>dwa</p>', 'lxml')
#     b2.body.wrap(b2.new_tag('div'))
#     b2.div.body.unwrap()
#     print(b2.div)
#     paramsDict = [1, 2]
#     for index, row in enumerate(paramsDict):
#         print(row)
#         paramsDict[index] = handler(row)
#         print(row)
#     print(paramsDict)

#     page = '''<link rel="stylesheet" href="bootstrap/css/bootstrap.min.css">'''
#     contentTree = BeautifulSoup(page, 'lxml')
#     page += '''<link rel="stylesheet" href="%s">''' % 'bootstrap/css/bootstrap.min.css'
#     print(page)
     
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
#     DB_CONNECT_STRING_ORACLE = 'foragCollecter_1/foragCollecter@10.18.50.229/orcl'
    DB_CONNECT_STRING_ORACLE = 'foragCollecter_1/foragCollecter@192.168.1.181/orcl'
# # #     SQL_ADD_NEW_ITEM = '''INSERT INTO foragOwner.MsgTable(mId,mSource,mTitle,mIntro,mPic,mTags,mAuthor,mContent,mPublishTime, \
# # #         mCollectTime,mLikeCount,mDislikeCount,mCollectCount,mTransmitCount) \
# # #         VALUES(foragOwner.MID_SEQ.NEXTVAL,'%s','%s','%s','%s','%s','%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),sysdate,0,0,0,0)
# # #     '''
# #     SQL_ADD_NEW_ITEM = '''INSERT INTO foragOwner.MsgTable(mId,mSource,mTitle,mIntro,mPic,mTags,mAuthor,mContent,mPublishTime,\
# # mCollectTime,mLikeCount,mDislikeCount,mCollectCount,mTransmitCount)\
# # VALUES(foragOwner.MID_SEQ.NEXTVAL,'dawfdsefadawwda','daw','daw','daw','daw','daw','daw',to_date('2017-05-17 09:55:00','yyyy-mm-dd hh24:mi:ss'),sysdate,0,0,0,0)'''
#     
#     connection = cx_Oracle.connect(DB_CONNECT_STRING_ORACLE)
#     print('get connection')
#     cursor = connection.cursor()
#     print('get cursor')
#     print(cursor.execute("select msource from foragOwner.msgtable where rownum<=1"))
#     print(cursor.fetchall())
#     SQL_ADD_NEW_ITEM = "INSERT INTO foragOwner.SimilarUrl(sourceUrl,similarUrl) VALUES('中dcdaw文','中文')"
#     print(SQL_ADD_NEW_ITEM)
#     cursor.execute(SQL_ADD_NEW_ITEM)
#     print('sql execute')
#     connection.commit()
#     print('conmmit success')
#     result = cursor.execute('select * from foragOwner.MsgTable where rownum<=:1', [2]).fetchall()
#     paramsDict = {}
#     for index, col in enumerate(cursor.description):
#         paramsDict['%s.%s' % ('item', col[0])] = list(map(lambda x:x[index] if x[index]!=None else '', result))
#     print(paramsDict)
#     for item in result:
#         for field in item:
#             print(field)
#     connection.close()
# #     data = ur.urlopen('http://himg2.huanqiu.com/attachment2010/2017/0224/14/27/20170224022730835.jpg')
# #     data = data.read()
# #     pic = open('20170224022730835.jpg', 'wb+')
# #     pic.write(data)
# #     pic.close()
#     print('db close')
#     text = '''<article class="item">
#                       <header class="item-head">
#                         <h3 class="item-title">[[MTITLE]]</h3>
#                         <section class="item-meta">
#                           <span class="item-author">作者：<a href="#">[[MAUTHOR]]</a></span> •
#                           <time class="item-date">[[MTIME]]</time>
#                         </section>
#                       </header>
#                       <section class="item-content">[[MCONTENT]]</section>
#                       <footer class="item-foot">
#                         <section class="item-souce"> 
#                           <span class="item-source">
#                             <a  href="[[MSOURCE]]">原网站</a>
#                           </span>
#                         </section>
#                         <section class="item-tag">
#                           <span class="label" repeat="true" statement="class:['label-danger','label-success','label-info','label-warning','label-primary']">[[MTAG]]
#                           </span>
#                         </section>
#                       </footer>
#                     </article>'''
#     for value in re.finditer(r'\[\[.*?\]\]', text):
#         print('%s %s' % (value, value.span()))
#         text.replace(text[value.start():value.end()], '')
#           
#     text = r'''
#         {
#                     "data": {
#                             "item": {
#                                 "sql": "select * from msgtable where mid=[[params.pageid]]"
#                             }, 
#                             "item-similar": {
#                                 "sql": "select * from msgtable,(select similarurl from similarurl where sourceurl=[[item.MSOURCE]]) s where msource=similarurl"
#                             }
#                     },
#                     "rule": {
#                         "imghandler": {
#                             "col": ["[[item.MCONTENT]]"],
#                             "way": "web"
#                         }
#                     },
#                     "style": {
#                         "css": ["xx.css"],
#                         "js": ["xx.js"],
#                         "layout": 
#                             "<article class='item'>
#                               <header class='item-head'>
#                                 <h3 class='item-title'>[[item.MTITLE]]</h3>
#                                 <section class='item-meta'>
#                                   <span class='item-author'>作者：<a href='#'>[[item.MAUTHOR]]</a></span> •
#                                   <time class='item-date'>[[item.MTIME]]</time>
#                                 </section>
#                               </header>
#                               <section class='item-content'>[[item.MCONTENT]]</section>
#                               <footer class='item-foot'>
#                                 <section class='item-souce'> 
#                                   <span class='item-source'>
#                                     <a  href='[[item.MSOURCE]]'>原网站</a>
#                                   </span>
#                                 </section>
#                                 <section class='item-tag'>
#                                   <span class='label' repeat='true' statement='{\"class\":{\"way\":\"+r\",\"values\":[\"label-danger\",\"label-success\",\"label-info\",\"label-warning\",\"label-primary\"]}}'>[[item.MTAG]]
#                                   </span>
#                                 </section>
#                               </footer>
#                             </article>
#                             <section class='item-similar'>
#                                 <div class='table-responsive'>
#                                <table class='table table-hover'>
#                                  <tr repeat='true'>
#                                    <td class='item-text' style='width:40%;'>[[item-similar.MTITLE]]</td>
#                                    <td style='width:25%;'>
#                                       <div class='progress progress-xs'>
#                                         <div class='progress-bar progress-bar-danger' style='width:[[item-similar.MHOT]]'></div>
#                                       </div>
#                                     </td>
#                                     <td class='item-tag hidden-xs' style='width:35%;'>
#                                       <span class='label' repeat='true' statement='{\"class\":{\"way\":\"+r\",\"values\":[\"label-danger\",\"label-success\",\"label-info\",\"label-warning\",\"label-primary\"]}}'>[[item.MTAG]]
#                                       </span>
#                                     </td>
#                                  </tr>
#                                 </table>
#                              </div>
#                             </section>"
#                     }
#                 
#                 }
#     '''
#     text = re.sub('\s+', " ", text)
#     text = json.loads(text)
# #     cols = json.loads(r'["[[item.MCONTENT]]", "[[item.MCONTENT]]"]')
# #     for col in cols:
# #         colName = col[2:-2]
# #         print(colName)
# # #     testDict = {'img':5}
# # #     print(testDict['imgs'])
# #     content = BeautifulSoup(text['style']['layout'], 'lxml')
# #     for child in content.body.children:
# #         if isinstance(child, Tag):
# #             print(str(child))
#     templateStyle= text['style']
#     page = BeautifulSoup(templateStyle['layout'], 'lxml')
#     
#     repeatTag = page.body.find_next(repeat="true")
#     while repeatTag:
#         del repeatTag['repeat']
#         statement = json.loads(repeatTag.attrs.get('statement', '{}'), '{}')
#         if statement != {}: del repeatTag['statement']
#         
#         lackValue = re.search(r"\[\[.*?\]\]", str(repeatTag))
#         if lackValue:
#             for counter in range(0, len(paramsDict[lackValue.group(0)[2:-2]])-1):
#                 copyTag = copy.deepcopy(repeatTag)
#                 for attr, attrRule in statement.items():
#                     if attrRule['way'] == '+r':
#                         copyTag[attr] = '%s %s' % (' '.join(copyTag[attr]), random.choice(attrRule['values']))
#                 repeatTag.insert_after(copyTag)
#                 
#         for attr, attrRule in statement.items():
#             if attrRule['way'] == '+r':
#                 repeatTag[attr] = '%s %s' % (' '.join(repeatTag[attr]), random.choice(attrRule['values']))
#                 
#         repeatTag = repeatTag.find_next(repeat="true")
#     
#     for jsFile in templateStyle['js']:
#         jsTag = page.new_tag('script', src=jsFile)
#         page.body.insert(0, jsTag)
#     for cssFile in templateStyle['css']:
#         cssTag = page.new_tag('link', rel="stylesheet", href=cssFile)
#         page.body.insert(0, cssTag)
#      
#     def tagContainValues(tag):
#         global paramsDict, paramVisitedDict
#         valuePattern = re.compile(r"\[\[.*?\]\]")
#         isLackValue = False
#           
#         if tag.string:
#             lackTexts = valuePattern.findall(tag.string)
#             if len(lackTexts) != 0:
#                 isLackValue = True
#                 for lackText in lackTexts:
#                     lackTextValueName = lackText[2:-2]
#                     tag.string = tag.string.replace(lackText, paramsDict[lackTextValueName][paramVisitedDict[lackTextValueName]])
#                     paramVisitedDict[lackTextValueName] += 1
#           
#         for key, value in tag.attrs.items():
#             if isinstance(value, str):
#                 lackAttrValues = valuePattern.findall(value)
#                 if len(lackAttrValues) != 0:
#                     isLackValue = True
#                     for lackAttrValue in lackAttrValues:
#                         lackAttrValueName = lackText[2:-2]
#                         tag.attrs[key] = tag.attrs[key].replace(lackAttrValue, paramsDict[lackAttrValueName][paramVisitedDict[lackAttrValueName]]) 
#                         paramVisitedDict[lackAttrValueName] += 1
#                 
#         return isLackValue
#     
#     for tag in page.body.find_all(tagContainValues):
#         pass

#     valuePattern = re.compile(r'<.*?>')
#     for i in valuePattern.findall(templateStyle['layout']):
#         print(i)