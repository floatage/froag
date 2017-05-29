#encoding=utf-8
import cx_Oracle, os, re, json
import urllib.request as ur

if __name__ == '__main__':
#     os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
#     DB_CONNECT_STRING_ORACLE = 'foragCollecter_1/foragCollecter@10.18.50.229/orcl'
# #     SQL_ADD_NEW_ITEM = '''INSERT INTO foragOwner.MsgTable(mId,mSource,mTitle,mIntro,mPic,mTags,mAuthor,mContent,mPublishTime, \
# #         mCollectTime,mLikeCount,mDislikeCount,mCollectCount,mTransmitCount) \
# #         VALUES(foragOwner.MID_SEQ.NEXTVAL,'%s','%s','%s','%s','%s','%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),sysdate,0,0,0,0)
# #     '''
#     SQL_ADD_NEW_ITEM = '''INSERT INTO foragOwner.MsgTable(mId,mSource,mTitle,mIntro,mPic,mTags,mAuthor,mContent,mPublishTime,\
# mCollectTime,mLikeCount,mDislikeCount,mCollectCount,mTransmitCount)\
# VALUES(foragOwner.MID_SEQ.NEXTVAL,'dawfdsefadawwda','daw','daw','daw','daw','daw','daw',to_date('2017-05-17 09:55:00','yyyy-mm-dd hh24:mi:ss'),sysdate,0,0,0,0)'''
#   
#     connection = cx_Oracle.connect(DB_CONNECT_STRING_ORACLE)
#     print('get connection')
#     cursor = connection.cursor()
#     print('get cursor')
#     SQL_ADD_NEW_ITEM = "INSERT INTO foragOwner.SimilarUrl(sourceUrl,similarUrl) VALUES('中dcdaw文','中文')"
# #     print(SQL_ADD_NEW_ITEM)
# #     cursor.execute(SQL_ADD_NEW_ITEM)
# #     print('sql execute')
# #     connection.commit()
# #     print('conmmit success')
#     result = cursor.execute('select * from foragOwner.MsgTable where rownum<=2').fetchall()
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
    text = '''<article class="item">
                      <header class="item-head">
                        <h3 class="item-title">[[MTITLE]]</h3>
                        <section class="item-meta">
                          <span class="item-author">作者：<a href="#">[[MAUTHOR]]</a></span> •
                          <time class="item-date">[[MTIME]]</time>
                        </section>
                      </header>
                      <section class="item-content">[[MCONTENT]]</section>
                      <footer class="item-foot">
                        <section class="item-souce"> 
                          <span class="item-source">
                            <a  href="[[MSOURCE]]">原网站</a>
                          </span>
                        </section>
                        <section class="item-tag">
                          <span class="label" repeat="true" statement="class:['label-danger','label-success','label-info','label-warning','label-primary']">[[MTAG]]
                          </span>
                        </section>
                      </footer>
                    </article>'''
    for value in re.finditer(r'\[\[.*?\]\]', text):
        print('%s %s' % (value, value.span()))
        text.replace(text[value.start():value.end()], '')
          
    print(text)
    text = r'''
        {
                    "data": {
                            "item": {
                                "sql": "select * from msgtable where mid=[[params.pageid]]"
                            }, 
                            "item-similar": {
                                "sql": "select * from msgtable,(select similarurl from similarurl where sourceurl=[[item.MSOURCE]]) s where msource=similarurl"
                            }
                    },
                    "rule": {
                        "imgHandler": {
                            "col": ["[[item.MCONTENT]]"],
                            "way": "web"
                        }
                    },
                    "style": {
                        "css":"xx.css",
                        "layout": 
                            "<article class='item'>
                              <header class='item-head'>
                                <h3 class='item-title'>[[MTITLE]]</h3>
                                <section class='item-meta'>
                                  <span class='item-author'>作者：<a href='#'>[[MAUTHOR]]</a></span> •
                                  <time class='item-date'>[[MTIME]]</time>
                                </section>
                              </header>
                              <section class='item-content'>[[MCONTENT]]</section>
                              <footer class='item-foot'>
                                <section class='item-souce'> 
                                  <span class='item-source'>
                                    <a  href='[[MSOURCE]]'>原网站</a>
                                  </span>
                                </section>
                                <section class='item-tag'>
                                  <span class='label' repeat='true' statement='class:[\"label-danger\",\"label-success\",\"label-info\",\"label-warning\",\"label-primary\"]'>[[MTAG]]
                                  </span>
                                </section>
                              </footer>
                            </article>'
                            <section class='item-similar'>
                                <div class='table-responsive'>
                               <table class='table table-hover'>
                                 <tr repeat='true'>
                                   <td class='item-text' style='width:40%;'>[[MTITLE]]</td>
                                   <td style='width:25%;'>
                                      <div class='progress progress-xs'>
                                        <div class='progress-bar progress-bar-danger' style='width:[[MHOT]]'></div>
                                      </div>
                                    </td>
                                    <td class='item-tag hidden-xs' style='width:35%;'>
                                      <span class='label' repeat='true' statement='class:[\"label-danger\",\"label-success\",\"label-info\",\"label-warning\",\"label-primary\"]'>[[MTAG]]
                                        </span>
                                    </td>
                                 </tr>
                                </table>
                             </div>
                            </section>"
                    }
                 
                }
    '''
    text = re.sub('\s', "", text)
    print(text)
    print(json.loads(text))
    cols = json.loads(r'["[[item.MCONTENT]]", "[[item.MCONTENT]]"]')
    for col in cols:
        colName = col[2:-2]
        print(colName)
    testDict = {'img':5}
    print(testDict['imgs'])