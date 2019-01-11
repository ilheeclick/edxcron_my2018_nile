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
db = client.edxapp

# 변수 초기화
definition = "lock"
#mongo_time = (datetime.datetime.now() + datetime.timedelta(-1)).strftime('%B %d, %Y')
#mongo_time = (datetime.datetime.now()).strftime('%B %d, %Y')
mongo_time = (datetime.datetime.now() + datetime.timedelta(-1)).strftime('%B %d, %Y')
mongo_time_a = (datetime.datetime.now() + datetime.timedelta(-1)).strftime('%B')
mongo_time_b = (datetime.datetime.now() + datetime.timedelta(-1)).strftime('%d')
mongo_time_c = (datetime.datetime.now() + datetime.timedelta(-1)).strftime('%Y')
if mongo_time_b[0] == '0':
    mongo_time_b = mongo_time_b[1]
mongo_time = mongo_time_a + " " + mongo_time_b + ", " + mongo_time_c
print "mongo_time = ", mongo_time

# 시간 가공
if mongo_time[9] == '0':
    mongo_time = mongo_time[:9] + mongo_time[10:]

# 강좌 코드를 얻어오는 쿼리
curs = conn.cursor()
sql = '''
    SELECT org,
           display_number_with_default,
           Substring_index(id, '+', -1)
    FROM   course_overviews_courseoverview
'''
curs.execute(sql)
rows = curs.fetchall()

if len(rows) != 0:
    for item in rows:
        org = item[0]
        course = item[1]
        run = item[2]

        # 변수 초기화
        cursor = None
        pb = None
        blocks = None
        definition = None
        fd = None
        it = None
        it_index = None
        user_list = []

        # DEBUG
        # course-v1:BUFSk+vi1+vi1
        print "course-v1:{}+{}+{}".format(org, course, run)

        if db.modulestore.active_versions.find_one({'org':org, 'course':course, 'run':run}):
            cursor = db.modulestore.active_versions.find_one({'org':org, 'course':course, 'run':run})
            if cursor.get('versions').get('published-branch'):
                pb = cursor.get('versions').get('published-branch')
                cursor = db.modulestore.structures.find_one({'_id': ObjectId(pb)})
                blocks = cursor.get('blocks')
                for item in blocks:
                    if item.get('block_type') == 'course_info' and item.get('block_id') == 'updates':
                        definition = item.get('definition')
                if definition != "lock":
                    if db.modulestore.definitions.find_one({'_id': ObjectId(definition)}):
                        cursor = db.modulestore.definitions.find_one({'_id': ObjectId(definition)})
                        fd = cursor.get('fields')
                        it = fd.get('items')
                        if it != None:
                            print "------------------------------------> it"
                            it_index = len(it)-1
                            date = it[it_index]['date']
                            status =  it[it_index]['status']
                            print "date ------------------------------------> ", date
                            print "mongo_time ------------------------------> ", mongo_time
                            print "type(date) ------------------------------------> ", type(date)
                            print "type(mongo_time) ------------------------------> ", type(mongo_time)
                            print "status ------------------------------------> ", status
                            if date == mongo_time and status == 'visible':
                                print "--------------------------------------> atari"
                                course_key = "course-v1:{}+{}+{}".format(org, course, run)
                                curs = conn.cursor()
                                sql = '''
                                    SELECT a.user_id
                                      FROM student_courseenrollment AS a
                                      JOIN auth_user AS b
                                      ON a.user_id = b.id
                                     WHERE course_id = '{}'
                                '''.format(course_key)
                                print sql
                                curs.execute(sql)
                                rows = curs.fetchall()
                                if len(rows) != 0:
                                    for item in rows:
                                        user_list.append(item)
                                    full = '{}/courses/{}/info'.format(url, course_key)
                                    print full
                                    for user in user_list:
                                        curs = conn.cursor()
                                        sql = '''
                                            insert into memo(receive_id, title, contents, memo_gubun, regist_id)
                                            values({0}, '등록된 강좌 공지사항 변경 안내', '{1}', 3, {2})
                                        '''.format(user[0], full, admin_id)
                                        # DEBUG
                                        print sql
                                        curs.execute(sql)
                                        conn.commit()

