from pyhive import hive
import pandas as pd
import os
import sys
# 设置当前路径为项目根目录
os.chdir(os.path.dirname(os.path.abspath(__file__)))
def export_hive_schema(host, port, database, output_file='schema.md',username='hive', password='hive', auth='LDAP'):
    """导出Hive库所有表结构"""
    conn = hive.Connection(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        auth=auth
    )
    cursor = conn.cursor()
    
    # 获取所有表名
    cursor.execute(f"SHOW TABLES IN {database}")
    tables = [row[0] for row in cursor.fetchall()]
    
    # 准备输出内容
    output = []
    
    for table in tables:
        # 获取表结构
        cursor.execute(f"DESCRIBE FORMATTED {database}.{table}")
        describe = cursor.fetchall()
        
        # 获取建表语句
        cursor.execute(f"SHOW CREATE TABLE {database}.{table}")
        ddl = '\n'.join([row[0] for row in cursor.fetchall()])
        
        # 格式化输出
        output.append(ddl+'\n\n')
    
    # 写入文件
    with open(output_file, 'w') as f:
        f.write('\n'.join(output))

if __name__ == "__main__":
    database=sys.argv[1]
    assert database ,'数据库名称错误'
    # 配置Hive连接信息
    export_hive_schema(
        host='10.8.15.240',
        port=10000,  # 默认端口
        database=database,
        output_file=f'{database}.sql',
        username='hive',
        password='hive'
    )