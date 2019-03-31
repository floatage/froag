#encoding=utf8

import json, datetime, codecs, math, random, functools, copy
import ForagInterfaceService

dateDict = json.loads(codecs.open('config/date.json','r', 'utf-8').read(), 'utf-8')

# 浏览、赞、踩、评论、收藏、转发，对应操作分数1,2,-2,3,4,5
class UserProfileGenerator:
    opGradeDict = {'browse':3.0, 'like':3.5, 'dislike':-3.5, 'comment':4.0, 'collect':4.5, 'transmit':5.0}
    dateFormat = '%Y-%m-%d %H:%M:%S'
    dateWeight = 0.9
    defaultGrade = 10.0
    daySeconds = 86400
    
    interest_limit = {"optional":50,"tag":100,"channel":100}
    
    SQL_UPDATE_USER_INTEREST = 'update foragOwner.UserTable set UTINTEREST=:1 where utId=:2'
    
    def __init__(self, params, conn):
        self.conn = conn
        self.params = params
        self.todayDate = datetime.datetime.now()
        self.interest = params['user']['utInterest']
        self.optionalTags = self.interest['optional']
        self.recommandHistory = list(set(self.params['history']))
    
    def _getContextTags(self, context):
        specialDay = self.todayDate.strftime('%m-%d')
        if specialDay in dateDict:
            self.optionalTags[dateDict[specialDay]] = self.defaultGrade
        self.optionalTags[context['prinvice']] = self.defaultGrade
        self.optionalTags[context['city']] = self.defaultGrade
    
    def _getUserInforTags(self, userInfor):
        if userInfor['utEdu'] != 'null':self.optionalTags[userInfor['utEdu']] = self.defaultGrade
        if userInfor['utPro'] != 'null':self.optionalTags[userInfor['utPro']] = self.defaultGrade
        if userInfor['utSkill'] != 'null':
            for skill in userInfor['utSkill'].split(','):
                self.optionalTags[skill] = self.defaultGrade
    
    def _handleUserLog(self, log):
        historyUrls = []
        for item in log['data']:
            historyUrls.append(item['msgSource'])
            interval = self.todayDate - datetime.datetime.strptime(item['time'], self.dateFormat)
            msgGrade = self.opGradeDict[item['logType']] * math.pow(self.dateWeight, interval.seconds / self.daySeconds)
            
            msgTags = item['msgTags']
            tagAllWeight = functools.reduce(lambda x,y:float(x)+float(y), msgTags['tag'].values())
            for tag, weight in msgTags['tag'].items():
                tagGrade = msgGrade * weight / tagAllWeight
                if tag in self.interest['tag']:
                    self.interest['tag'][tag] = float(self.interest['tag'][tag]) + tagGrade
                else:
                    self.optionalTags[tag] = float(self.optionalTags.setdefault(tag, 0.0)) + tagGrade
            self.interest['channel'][msgTags['type']] = float(self.interest['channel'].setdefault(msgTags['type'], 0.0)) + msgGrade
        
        log['historyUrl'] = list(set(historyUrls))
        stopSet = list(set(log['history']))
        stopSet.extend(self.recommandHistory)
        log['history'] = stopSet
        
    def _recordProfile(self, interest, cursor, userId):
        record = {}
        for key, value in interest.items():
            record[key], maxSize = {}, self.interest_limit[key]
            for index, (tag, value) in enumerate(value.items()):
                if index >= maxSize: break
                record[key][tag] = value
        self.conn.cursor().execute(self.SQL_UPDATE_USER_INTEREST, (json.dumps(record, ensure_ascii=False), userId))
        self.conn.commit()
      
    def generate(self):
        self._getContextTags(self.params['user']['context'])
        self._getUserInforTags(self.params['user'])
        self._handleUserLog(self.params['log'])
        self._recordProfile(self.interest, self.conn, self.params['user']['utId'])
        return self.interest
    
class UserMsgMatcher_BaseFeature:
    SQL_GET_TAGS_MSGID = 'select tName, tMsg from foragOwner.TagMsg where tName in '
    SQL_GET_CHANNELS_MSGID = 'select cName, cMsg from foragOwner.ChannelMsg where cName in '
    
    interestOptionalRate = 0.7
    tagRate = 0.7
    channelRate = 1.0 - tagRate
    tagMaxNumbe = 10
    posReduceRate = 0.85
    chooseMaxSize = 2
    
    def _chooseMsgIds(self, keyResult, mLen, keySet, keyAllWeight, stopSet):
        result = []
        for (kName, kMsg) in keyResult:
            kMsg, keyMsgIds, counter = json.loads(str(kMsg)), [], 0
            msgIdLen = int(mLen * keySet[kName] / keyAllWeight)
            
            if msgIdLen > len(kMsg): 
                keyMsgIds = [int(m[0]) for m in kMsg]
            else:
                for index, mItem in enumerate(kMsg):
                    if counter >= msgIdLen: break
                    if mItem[0] not in stopSet and random.uniform(0.0, 1.0) <= math.pow(self.posReduceRate, index):
                        keyMsgIds.append(mItem[0])
                        counter += 1           
            result.extend(keyMsgIds)
        
        return result
    
    def _getKeysMsgIds(self, keySet, keyAllWeight, mLen, stopSet, sql, conn):
        queryKeys = list(keySet.keys())
        if len(queryKeys) > 1:
            queryKeys = str(tuple(queryKeys))
        else :
            if isinstance(queryKeys[0], str):
                queryKeys = "('%s')" % queryKeys[0]
            else:
                queryKeys = "(%d)" % queryKeys[0]
        keyResult = conn.cursor().execute(sql + queryKeys).fetchall()
        
        result = []
        if keyResult:
            for counter in range(0, self.chooseMaxSize):
                if len(result) >= mLen: break
                result.extend(self._chooseMsgIds(keyResult, mLen, keySet, keyAllWeight, stopSet))
                stopSet.extend(result)
            
            result = list(set(result))
            result = [int(i) for i in result]
                
        return result
    
    def _getInterestTagMsg(self, optionalTags, mLen, stopSet, conn):
        if len(optionalTags)==0: return []
        
        randomUpperBound, resultTags = max(optionalTags.values()), {}
        allTagWeight, counter = 0.0, 0
        for tag, weight in optionalTags.items():
            if random.uniform(0, randomUpperBound) <= weight:
                resultTags[tag] = weight
                allTagWeight += weight
                counter += 1
        
        return self._getKeysMsgIds(resultTags, allTagWeight, mLen, stopSet, self.SQL_GET_TAGS_MSGID, conn)
    
    def _getInterestChannelMsg(self, channel, mLen, stopSet, conn):
        if len(channel)==0: return []
        
        allChannelWeight = sum(channel.values())
        return self._getKeysMsgIds(channel, allChannelWeight, mLen, stopSet, self.SQL_GET_CHANNELS_MSGID, conn)
    
    def generate(self, params, interest, conn):
        optionalTags = interest['optional']
        for tag, weight in interest['tag'].items():
            optionalTags[tag] = float(weight) * self.interestOptionalRate
        
        requestMsgSize, history = int(params['len']), copy.deepcopy(params['log']['history'])
        result = self._getInterestTagMsg(optionalTags, int(requestMsgSize*self.tagRate), history, conn)
        result.extend(self._getInterestChannelMsg(interest['channel'], int(requestMsgSize*self.channelRate), history, conn))
        return 'id', result

class UserMsgMatcher_BaseCollaboration:
    historyLenRange = (5,10)
    resultRateRange = (0.1, 0.2)
    
    SQL_GET_SIMILAR_URL = 'select similarurl from foragOwner.similarurl where sourceurl in '
    
    def generate(self, params, interest, conn):
        resultUrlsLen = min(random.randint(self.historyLenRange[0],self.historyLenRange[1]), len(params['log']['historyUrl']))
        if resultUrlsLen == 0:
            return 'url', []
        
        resultUrls = random.sample(params['log']['historyUrl'], resultUrlsLen)
        resultUrls = str(tuple(resultUrls)) if len(resultUrls) > 1 else "('%s')" % resultUrls[0]
        similarUrls = conn.cursor().execute(self.SQL_GET_SIMILAR_URL + resultUrls).fetchall()
        similarUrls = [v[0] for v in similarUrls]
        similarUrlsLenRange = [int(int(params['len'])*r) for r in self.resultRateRange]
        similarUrls = random.sample(similarUrls, min(len(similarUrls), random.randint(similarUrlsLenRange[0], similarUrlsLenRange[1])))
        return 'url', similarUrls

class OptionalResultSetGenerator:
    generatorList = [UserMsgMatcher_BaseFeature(), UserMsgMatcher_BaseCollaboration()]
        
    def generate(self, params, interest, conn):
        resultSet = {}
        for g in self.generatorList:
            key, result = g.generate(params, interest, conn)
            resultSet[key] = result
        return resultSet

class ResultSetGenerator:
    SQL_GET_MSG_ID = 'select mId, mTitle, mIntro, mPic, mTags, mAuthor, \
        mPublishTime, mLikeCount, mDislikeCount, mCollectCount, mTransmitCount from foragOwner.MsgTable where mId in '
    SQL_GET_MSG_URL = 'select mId, mTitle, mIntro, mPic, mTags, mAuthor, \
        mPublishTime, mLikeCount, mDislikeCount, mCollectCount, mTransmitCount from foragOwner.MsgTable where mSource in '
    
    addtionMsgRange = (0.1, 0.12)
    
    def _getAddtionResultSet(self, mLen):
        service = ForagInterfaceService.serviceManager.getServiceObj('getHotArticle')
        return random.sample(service.hotMsg, mLen)
    
    def _resultSetFilter(self, params, resultSet):
        stopSet = params['log']['history']
        resultSet = list(filter(lambda x:x[0] not in stopSet, resultSet))
        return list({item[0]:item for item in resultSet}.values())
        
    def _resultSetRank(self, params, optionalResultSet):
        return optionalResultSet
    
    def _getMsgs(self, msgKeys, conn, keyType):
        if msgKeys==[]: return []
        
        msgKeys = str(tuple(msgKeys)) if len(msgKeys) > 1 else '(%d)' % msgKeys[0]
        sql = self.SQL_GET_MSG_ID if keyType=='id' else self.SQL_GET_MSG_URL
        result = conn.cursor().execute(sql + msgKeys).fetchall()
        result = result if result else []
        result = [[str(col) if col!=None else 'None' for col in row]for row in result]  
        return result
    
    def generate(self, params, optionalResultSet, conn):
        requestMsgLen = int(params['len'])
        self.addtionMsgRange = [int(requestMsgLen*r) for r in self.addtionMsgRange]
        resultSet = self._getAddtionResultSet(random.randint(self.addtionMsgRange[0],self.addtionMsgRange[1]))
        print('generate addtion msg len: %d' % len(resultSet))
        resultSet.extend(self._getMsgs(optionalResultSet['url'], conn, 'url'))
        print('generate url msg len: %d' % len(resultSet))
        self._resultSetRank(params, optionalResultSet)
        oddLen = requestMsgLen - len(resultSet)
        resultSet.extend(self._getMsgs(optionalResultSet['id'][0:max(0,oddLen)], conn, 'id'))
        print('generate id msg len: %d' % len(resultSet))
        resultSet = self._resultSetFilter(params, resultSet)
        print('recommand len: %d' % len(resultSet))
        return {'msg':resultSet, 'id':[m[0] for m in resultSet]}
    
class InterestArticleGenerator:
    def generate(self, params, conn):
        if int(params['len']) <= 0: return []
        interest = UserProfileGenerator(params, conn).generate()
        optionalResultSet = OptionalResultSetGenerator().generate(params, interest, conn)
        return ResultSetGenerator().generate(params, optionalResultSet, conn)

if __name__=='__main__':
    generator = ForagInterfaceService.GetUserInterestPageService()
    responce = {}
    generator.service('{"name":"getUserInterestPage","params":{"user":{},"log":"日志文件","len":"10"}}', responce)
    print(responce)