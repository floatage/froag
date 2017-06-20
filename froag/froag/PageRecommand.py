#encoding=utf8

import json, datetime, codecs, math

dateDict = json.loads(codecs.open('config/date.json','r', 'utf-8').read(), 'utf-8')

# 兴趣占比(结果筛选)，文章ID(协同过滤)，标签评价(新颖性)，加上用户信息，上下文(日期、地点)
class InterestArticleGenerator:
    def generate(self, params, conn):
        profile = UserProfileGenerator(params, conn).generate()
        print(profile)
        return (12345,12356)

# 浏览、赞、踩、评论、收藏、转发，对应操作分数1,2,-2,3,4,5
class UserProfileGenerator:
    opGradeDict = {'browse':1.0, 'like':2.0, 'dislike':-2.0, 'comment':3.0, 'collect':4.0, 'transmit':5.0}
    dateFormat = '%Y-%m-%d %H:%M:%S'
    dateWeight = 0.9
    defaultGrade = 20.0
    daySeconds = 86400
    
    interest_limit = {"optional":50,"tag":100,"channel":100}
    
    SQL_UPDATE_USER_INTEREST = 'update foragOwner.UserTable set UTINTEREST=:1 where utId=:2'
    
    def __init__(self, params, conn):
        self.conn = conn
        self.params = params
        self.todayDate = datetime.datetime.now()
        self.interest = params['user']['utInterest']
        self.optionalTags = self.interest['optional']
    
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
        for item in log['data']:
            interval = self.todayDate - datetime.datetime.strptime(item['time'], self.dateFormat)
            tagGrade = self.opGradeDict[item['logType']] * math.pow(self.dateWeight, interval.seconds / self.daySeconds)
            msgTags = item['msgTags']
            for tag in msgTags['tag'].keys():
                if tag in self.interest['tag']:
                    self.interest['tag'][tag] = float(self.interest['tag'][tag]) + tagGrade
                else:
                    self.optionalTags[tag] = float(self.optionalTags.setdefault(tag, 0.0)) + tagGrade
            self.interest['channel'][msgTags['type']] = float(self.interest['channel'].setdefault(msgTags['type'], 0.0)) + tagGrade
    
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
    pass

class UserMsgMatcher_BaseCollaboration:
    pass

class ResultSetGenerator:
    pass

class ResultSetFilter:
    pass

class ResultSetRanker:
    pass