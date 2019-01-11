#-*- coding:utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import json
import pymysql
import datetime

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

# 변수 초기화
user_list = []
course_list = []
#human_time = (datetime.datetime.now() + datetime.timedelta(+15)).strftime('%Y-%m-%d')
#human_html = (datetime.datetime.now() + datetime.timedelta(+15)).strftime('%Y년 %m월 %d일')

# 관심강좌 등록을 알리기 위한 강좌 추출
curs = conn.cursor()
sql = '''
    SELECT v1.interest_id,
           v3.email,
           v3.id,
           v2.id,
           v2.display_name,
           v2.start,
           v2.end,
           v2.enrollment_start,
           v2.enrollment_end,
           v2.created,
           v2.modified
      FROM interest_course AS v1
           JOIN
           (SELECT id,
                   org,
                   course,
                   start,
                   end,
                   enrollment_start,
                   enrollment_end,
                   created,
                   modified,
                   display_name
              FROM (  SELECT a.id,
                             CASE
                                WHEN     a.org = @org
                                     AND a.display_number_with_default = @course
                                THEN
                                   @rn := @rn + 1
                                ELSE
                                   @rn := 1
                             END
                                rn,
                             @org := a.org
                                org,
                             @course := a.display_number_with_default
                                `course`,
                             a.start,
                             a.end,
                             a.enrollment_start,
                             a.enrollment_end,
                             a.created,
                             a.modified,
                             a.display_name
                        FROM course_overviews_courseoverview a,
                             (SELECT @rn := 0, @org := 0, @course := 0) b
                       WHERE a.start < a.end
                       and date_format(a.enrollment_start ,'%Y%m%d') = date_format(now() ,'%Y%m%d')
                    ORDER BY a.org, a.display_number_with_default, a.enrollment_start DESC)
                   t1
             WHERE rn = 1) AS v2
              ON v1.org = v2.org AND v1.display_number_with_default = v2.course
           JOIN auth_user AS v3 ON v1.user_id = v3.id
     WHERE NOT EXISTS
              (SELECT 'x'
                 FROM student_courseenrollment d1
                WHERE     v1.user_id = d1.user_id
                      AND course_id LIKE
                             Concat('course-v1:',
                                    v1.org,
                                    '+',
                                    v1.display_number_with_default,
                                    '%'))
     and v1.use_yn='Y';
'''
print sql
curs.execute(sql)
rows = curs.fetchall()

if len(rows) != 0:
    for item in rows:
        user_list.append(item[2])
        course_list.append(item[3])

    cnt = 0
    for user in user_list:
        full = "{0}/courses/{1}/about".format(url, course_list[cnt])
        curs = conn.cursor()
        print full
        sql = '''
            insert into memo(receive_id, title, contents, memo_gubun, regist_id)
            values({0}, '등록된 관심강좌 신규 개설 알림', '{1}', 2, {2})
        '''.format(user, full, admin_id)
        print sql
        curs.execute(sql)
        #curs.execute(sql, [user, full, admin_id])
        conn.commit()
        cnt += 1
