# -*- coding: utf-8 -*-
"""
Created on Sun Apr 25 13:45:39 2021

@author: jiuxin
"""

#-*- coding: utf-8 -*-
import pymysql
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import configparser
import sys
engine = create_engine('mysql+pymysql://root:Jiuxin2120@nm@124.70.142.226:3306/amdb')




class ConfigParser(configparser.RawConfigParser):
    def __init__(self, **kwargs):
        kwargs['allow_no_value'] = True
        configparser.RawConfigParser.__init__(self, **kwargs)

    def __remove_quotes(self, value):
        quotes = ["'", "\""]
        for quote in quotes:
            if len(value) >= 2 and value[0] == value[-1] == quote:
                return value[1:-1]
        return value

    def get(self, section, option):
        value = configparser.RawConfigParser.get(self, section, option)
        return self.__remove_quotes(value)

def get_config(read_default_group, key, arg=None):
    """
    获取配置值
    :param key:
    :return:
    """
    if arg:
        return arg

    try:
        cfg = ConfigParser()
        if sys.platform.startswith("win"):
            work_path = os.path.dirname(os.path.realpath(__file__))
            read_default_file = os.path.join(work_path, 'config.ini')
        else:
            work_path = os.path.dirname(os.path.realpath(__file__))
            read_default_file = os.path.join(work_path, 'config.cnf')
        
        cfg.read(os.path.expanduser(read_default_file))
        return cfg.get(read_default_group, key)
    except Exception:
        return arg

def get_mysql_engine(user ,password ,host ,port ,database):
    engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format(user ,password ,host ,port ,database))
    return engine

    

def get_mysql_client(user ,password ,host ,port ,database):
    """
    获取数据库连接
    :return:
    """
    db_config = {
        'host': host,
        'user': user,
        'password': password,
        'database': database,
        'port': port,
        'charset': get_config('mysql', 'charset'),
    }

    return pymysql.connect(**db_config)

def update_mysql_data(df, user ,password ,host ,port ,database, update_table , update_fields, update_condition_fields , file_fields , file_conditon_fields , extral_condition):
    conn=get_mysql_client(user ,password ,host ,port ,database)
    
    cursor =conn.cursor()
    for i in range(0,len(df)):
#    for i in range(0,5):
#        for m in range(0,len(update_condition_fields)):
        
        #如果本地文件条件字段文件中为空【第一个条件字段】，则退出，执行下一条更新语句
        if df[file_conditon_fields[0]].iloc[i]=='':
            continue
            
        #更新set语句段
        update_set_sements = ' , '.join([update_fields[a] + ' = ' + '''"{0}"'''.format(df[file_fields[a]].iloc[i]) for a in range(0,len(update_fields))])
        
        #更新条件语句段
        update_where_sements = ' and ' + ' and '.join([update_condition_fields[a] + ' = ' + '''"{0}"'''.format(df[file_conditon_fields[a]].iloc[i]) for a in range(0,len(update_condition_fields))]) + extral_condition
        sql_execute = '''UPDATE {0} SET {1} WHERE 1 = 1 {2}  ;'''.format(update_table, update_set_sements, update_where_sements)
        print(sql_execute)
        print(i)
        cursor.execute(sql_execute)
    conn.commit()
    print("================OK")
    conn.close()



host = get_config('mysql', 'host')
user = get_config('mysql', 'user')
password = get_config('mysql', 'password')
database = get_config('mysql', 'database')
port = int(get_config('mysql', 'port'))
charset = get_config('mysql', 'charset')

update_table = get_config('mysql', 'update_table')
update_fields = get_config('mysql', 'update_fields').split(",")
update_condition_fields = get_config('mysql', 'update_condition_fields').split(",")
file_fields = get_config('mysql', 'file_fields').split(",")
file_conditon_fields = get_config('mysql', 'file_conditon_fields').split(",")
extral_condition = get_config('mysql', 'extral_condition')
excel_combine_rows = get_config('mysql', 'excel_combine_rows').split(",")

print(host)
print(password)
print()

print(update_table)
print(update_fields)
print(update_condition_fields)
print(file_fields)
print(file_conditon_fields)
print(extral_condition)



if len(update_condition_fields) == 0 or len(file_conditon_fields) == 0 :
    print("条件字段为空")
else:

    #获取所有文件路径及文件夹
    for root, dirs, files in os.walk(r"./"):
        print('root_dir:', root)
    #    print('sub_dirs:', dirs)
        print('files:', files)
        file_list = files
        file_list = [ x for x in file_list if '.csv' in x or  '.xlsx' in x ]
        file_list.sort()
        print(file_list)
        for file in file_list:
            if '.csv' in file:
                df = pd.read_csv(r"{0}/{1}".format(root,file)).replace(np.nan, '', regex=True)
                for file_field in file_fields + file_conditon_fields:
                    df['{0}'.format(file_field)] = [x.replace(' ','') for x in df['{0}'.format(file_field)]]
                    df['{0}'.format(file_field)] = [x.replace('\r\n','') for x in df['{0}'.format(file_field)]]
                    df['{0}'.format(file_field)] = [x.replace('\r','') for x in df['{0}'.format(file_field)]]
                    df['{0}'.format(file_field)] = [x.replace('\n','') for x in df['{0}'.format(file_field)]]
                    df['{0}'.format(file_field)] = [x.replace('\\r\\n','') for x in df['{0}'.format(file_field)]]
                update_mysql_data(df, user ,password ,host ,port ,database, update_table , update_fields, update_condition_fields , file_fields , file_conditon_fields , extral_condition)
                print(file + '已更新：' + str(len(df)) + "条记录")
            
            elif '.xlsx' in file:   
                xls = pd.ExcelFile(r"{0}/{1}".format(root,file))
                sheet_names = xls.sheet_names   
                for sheet in sheet_names:
                    print("{0}/{1}".format(root,file) + "-------" + sheet)
                    df = pd.read_excel(r"{0}/{1}".format(root,file), sheet_name=sheet).replace(np.nan, '', regex=True)
                    
                    #定义空值上一值和定义新列表
                    up_one=''
                    list_new = []
                    
                    #对每一个excel列存在合并单元格的情况进行处理
                    for excel_combine_row in excel_combine_rows:
                        for value in df['{0}'.format(excel_combine_row)].tolist():
                            
                            if value == '':
                                list_new.append(up_one)
                            else:
                                up_one = value
                                list_new.append(value)
                        df['{0}'.format(excel_combine_row)] = list_new
                        
                    #对所有条件及更新字段进行去除空格和换行操作
                    for file_field in file_fields + file_conditon_fields + excel_combine_rows:
                        df['{0}'.format(file_field)] = [x.replace(' ','') for x in df['{0}'.format(file_field)]]
                        df['{0}'.format(file_field)] = [x.replace('\r\n','') for x in df['{0}'.format(file_field)]]
                        df['{0}'.format(file_field)] = [x.replace('\r','') for x in df['{0}'.format(file_field)]]
                        df['{0}'.format(file_field)] = [x.replace('\n','') for x in df['{0}'.format(file_field)]]
                        df['{0}'.format(file_field)] = [x.replace('\\r\\n','') for x in df['{0}'.format(file_field)]]
                    
                    #更新数据到数据库中
                    update_mysql_data(df, user ,password ,host ,port ,database, update_table , update_fields, update_condition_fields , file_fields , file_conditon_fields , extral_condition)
                    print(file + '已更新：' + str(len(df)) + "条记录")






















    



