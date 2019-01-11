# -*- coding: utf-8 -*-

import pymysql

conn = pymysql.connect(
    host='192.168.1.113',
    user='edxapp001',
    password='password',
    db='edxapp',
    charset='utf8'
)

curs = conn.cursor()

sql = '''
delete from memo
where date(regist_date) < now() - interval 6 month
'''

curs.execute(sql)
conn.commit()
conn.close()
