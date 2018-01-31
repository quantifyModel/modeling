# -*- coding: utf-8 -*-
import  MySQLdb
import pandas as pd
from sqlalchemy import create_engine
from src.File import File
class DB:
    db = None
    cursor = None
    connect_string = ""
    db_name = ""
    db_type = ""
    def __init__(self,db_type,host_name,user_name,password,db_name):
        self.db_name = db_name
        self.db_type = db_type
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
    # data_path ：数据路径，路径内必须是csv文件，符合dataframe的格式要求，不能有其他文件
    # numOfcsv_in_one_table：一张表中存多少张csv文件数据
    # 每张表都会自动添加index字段，默认（default_fixed=True）会用来替换为csv的名字用于识别哪张csv
    # 也可以自定义规则 fixed_function(current_table,file_name,start_line,end_line)
    def store_csv2db(self,data_path,data_type="csv",numOfcsv_in_one_table=1,
                                                    default_fixed=True,fixed_function=None):
        f = File(data_path);i = 0;current_table = ""
        for file_name in f.get_file_names_without_suffix_in_current_dir():
            if i % numOfcsv_in_one_table == 0: current_table = file_name
            df = pd.read_csv(data_path+"/"+file_name+"."+data_type)
            start_line = self.excut("select count(*) from "+ current_table)[0]
            self.input_dateframe2sqldb(df, self.db_name, current_table)
            end_line = self.excut("select count(*) from " + current_table)[0]
            if default_fixed:
                self.fix_function(current_table,file_name,start_line,end_line)
            else:
                fixed_function(current_table,file_name,start_line,end_line)
            i += 1

    def fix_function(self,current_table,file_name,start_line,end_line):
        self.excut("UPDATE `" + current_table
                        + "` SET `index` = " + file_name
                        + " where `index` >" + start_line
                        + " and `index` <= "+ end_line)

# demo :
a = DB("mysql","10.20.2.26", "root", "123456", "modeling")
f = File("../data");i = 0;current_table = ""
for file_name in f.get_file_names_with_path_in_current_dir():
    if i % 100 == 0: current_table = file_name[8:14]
    df = pd.read_csv(file_name)
    a.input_dateframe2sqldb(df, "modeling", current_table)
    result = a.excut("UPDATE `" + current_table + "` SET `index` = " + "1" + file_name[8:14] + " where `index` < 80749")
    print (file_name)
    i +=1
