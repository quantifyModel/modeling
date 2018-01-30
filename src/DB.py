# -*- coding: utf-8 -*-

import MySQLdb
import pandas as pd
from sqlalchemy import create_engine
from src.File import File
class DB:
    db = None
    cursor = None
    connect_string = ""
    def __init__(self,db_type,host_name,user_name,password,db_name):
        if db_type == "mysql":
            # 打开数据库连接
            self.db = MySQLdb.connect(host_name,user_name,password,db_name)
            # 使用cursor()方法获取操作游标
            self.cursor = self.db.cursor()
            # 引擎字符串
            self.connect_string = "mysql+mysqldb://"+ user_name + ":"+\
                    password + "@"+ host_name + ":3306" +"/"+db_name+"?charset=utf8"

    def excut(self,query):
        try:
            self.cursor.execute(query)
            self.db.commit()
            return self.cursor.fetchall()
        except:
            self.db.rollback()
        return -1

    def get_connect(self):
        return create_engine(self.connect_string)

    def input_dateframe2sqldb(self,thedataframe,database_name,table_name):
        pd.io.sql.to_sql(thedataframe,table_name, self.get_connect(), schema=database_name, if_exists='append')

    def __del__(self):
        # 关闭数据库连接
        self.db.close()

# demo :

a = DB("mysql","localhost", "root", "123456", "modeling")
f = File("../data")
for file_name in f.get_file_names_with_path_in_current_dir():
    df = pd.read_csv(file_name)
    a.input_dateframe2sqldb(df,"modeling","000001")
    result = a.excut("UPDATE `000001` SET `index` = " +"1"+ file_name[8:14] + " where `index` < 80749")
pass