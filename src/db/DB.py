# -*- coding: utf-8 -*-

import MySQLdb
import pandas as pd
from sqlalchemy import create_engine
from src.db.File import File
class DB:
    db = None
    cursor = None
    connect_string = ""
    db_name = ""
    db_type = ""
    engine  = None
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
            self.engine = self.get_connect()

    def excut(self,query):
        try:
            self.cursor.execute(query)
            self.db.commit()
            return self.cursor.fetchall()
        except:
            self.db.rollback()
            print(self.db.error())
        return -1

    def get_connect(self):
        return create_engine(self.connect_string)

    def input_dateframe2sqldb(self,thedataframe,database_name,table_name):
        pd.io.sql.to_sql(thedataframe,table_name, self.engine, schema=database_name, if_exists='append')

    def __del__(self):
        # 关闭数据库连接
        self.db.close()

    # data_path ：数据路径，路径内必须是csv文件，符合dataframe的格式要求，不能有其他文件
    # numOfcsv_in_one_table：一张表中存多少张csv文件数据
    # 每张表都会自动添加index字段，默认（default_fixed=True）会用来替换为csv的名字用于识别哪张csv
    # 也可以自定义规则 fixed_function(current_table,file_name,start_line,end_line)
    def store_csv2db(self,data_path,numOfcsv_in_one_table=1,data_type="csv",
                                                    default_fixed=True,fixed_function=None):
        f = File(data_path);i = 0;current_table = ""
        for file_name in f.get_file_names_without_suffix_in_current_dir():
            if file_name =='.DS_Store' or int(file_name) < 600851: continue
            if i % numOfcsv_in_one_table == 0:
                current_table = file_name
                start_line = 0
                df = pd.read_csv(data_path+"/"+file_name+"."+data_type)
                self.input_dateframe2sqldb(df, self.db_name, current_table)
                self.excut("alter table `" + current_table+"` add column `share_num` varchar(20)")
                self.excut("alter table `" + current_table+"` ADD COLUMN `id` int(20) ")
                self.excut("alter table `" + current_table+"` CHANGE COLUMN `id` `id` int(20) NOT NULL AUTO_INCREMENT, ADD PRIMARY KEY (`id`)")
            else:
                start_line = self.excut("select max(id) from `"+ current_table+ "`")[0][0]
                df = pd.read_csv(data_path+"/"+file_name+"."+data_type)
                self.input_dateframe2sqldb(df, self.db_name, current_table)

            end_line = start_line + len(df)
            if default_fixed:
                self.fix_function(current_table,file_name,start_line,end_line)
            else:
                fixed_function(current_table,file_name,start_line,end_line)
            i += 1
            print (file_name)

    def fix_function(self,current_table,file_name,start_line,end_line):
        self.excut("UPDATE `" + current_table
                        + "` SET `share_num` = '" + file_name
                        + "' where `id` >" + str(start_line)
                        + " and `id` <="+ str(end_line))

# demo :

# a = DB("mysql","localhost", "root", "123456", "modeling")
# a.store_csv2db("../../data",100)
