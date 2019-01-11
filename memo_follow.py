#-*- coding:utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import json
import pymysql
import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId

# 필수 입력
url = 'kmooc.kr'
admin_id = 109077 

# 디비 커넥션 연결
f = open("connection_db", 'r')
line = f.readline()
f.close()
connection = json.loads(line)
conn = pymysql.connect(
    host=connection['host'],
    port=connection['port'],
    user=connection['id'],
    password=connection['pw'],
    db='edxapp',
    charset='utf8'
)

# 몽고 커넥션 연결
client = MongoClient(connection['host'], 27017)
db = client.cs_comments_service_development

# 변수 초기화
user_list = []

# 현재 진행중인 강좌에 수강신청 되어있는 유저 추출
curs = conn.cursor()
sql = '''
    select user_id
    from student_courseenrollment as a
    left join course_overviews_courseoverview as b
    on a.course_id = b.id
    where now() between b.start and b.end
    group by user_id
'''
curs.execute(sql)
rows = curs.fetchall()

if len(rows) != 0:
    for item in rows:
        user_list.append(str(item[0]))

    print "-------------------- DEBUG MODE --------------------"
    print "대상 유저 리스트 = ",user_list

    for user in user_list:
        source_list = []

        print "----------------------"
        print "현재 대상 유저 = ",user

        cursor = db.subscriptions.find({'subscriber_id':str(user)})
        for item in cursor:
            source_list.append(item['source_id'])

        print "구독하고 있는 글 = {}".format(source_list)

        for source in source_list:
            if db.contents.find_one({'comment_thread_id': ObjectId(source)}):
                cursor = db.contents.find_one({'comment_thread_id': ObjectId(source)})
                if cursor['created_at'].strftime('%Y-%m-%d') == (datetime.datetime.now() + datetime.timedelta(-1)).strftime('%Y-%m-%d'):
                #if cursor['created_at'].strftime('%Y-%m-%d') == (datetime.datetime.now()).strftime('%Y-%m-%d'):
                    course_id = cursor['course_id']
                    url = ""
                    url = "{}/courses/{}/discussion/forum/course/threads/{}".format(url ,course_id, source)
                    print "전송 할 url = {}".format(url)
                    curs = conn.cursor()
                    """
                    sql = '''
                        insert into memo(receive_id, title, contents, memo_gubun, regist_id)
                        values(%s, '구독하고 있는 게시판 신규 알림', '%s', 4, %s)
                    '''
                    """
                    sql = '''
                        insert into memo(receive_id, title, contents, memo_gubun, regist_id)
                        values({0}, '구독하고 있는 게시판 신규 알림', '{1}', 4, {2})
                    '''.format(user, url, admin_id)
                    print sql
                    print "----------------------"
                    #curs.execute(sql, [user, url, admin_id])
                    curs.execute(sql)
                    conn.commit()


