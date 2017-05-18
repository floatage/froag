#encoding=utf-8
import cx_Oracle, os


if __name__ == '__main__':
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    DB_CONNECT_STRING_ORACLE = 'foragCollecter_1/foragCollecter@10.18.50.229/orcl'
#     SQL_ADD_NEW_ITEM = '''INSERT INTO foragOwner.MsgTable(mId,mSource,mTitle,mIntro,mPic,mTags,mAuthor,mContent,mPublishTime, \
#         mCollectTime,mLikeCount,mDislikeCount,mCollectCount,mTransmitCount) \
#         VALUES(foragOwner.MID_SEQ.NEXTVAL,'%s','%s','%s','%s','%s','%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),sysdate,0,0,0,0)
#     '''
    SQL_ADD_NEW_ITEM = '''INSERT INTO foragOwner.MsgTable(mId,mSource,mTitle,mIntro,mPic,mTags,mAuthor,mContent,mPublishTime,\
mCollectTime,mLikeCount,mDislikeCount,mCollectCount,mTransmitCount)\
VALUES(foragOwner.MID_SEQ.NEXTVAL,'dawfdsefadawwda','daw','daw','daw','daw','daw','daw',to_date('2017-05-17 09:55:00','yyyy-mm-dd hh24:mi:ss'),sysdate,0,0,0,0)'''

    connection = cx_Oracle.connect(DB_CONNECT_STRING_ORACLE)
    print('get connection')
    cursor = connection.cursor()
    print('get cursor')
    SQL_ADD_NEW_ITEM = "INSERT INTO foragOwner.SimilarUrl(sourceUrl,similarUrl) VALUES('中dcdaw文','中文')"
    print(SQL_ADD_NEW_ITEM)
    cursor.execute(SQL_ADD_NEW_ITEM)
    print('sql execute')
    connection.commit()
    print('conmmit success')
    result = cursor.execute('select * from foragOwner.similarurl').fetchall()
 
    for item in result:
        for field in item:
            if isinstance(field, str):
                print(field)
    connection.close()
    print('db close')