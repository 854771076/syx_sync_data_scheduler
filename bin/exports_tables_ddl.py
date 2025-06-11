# ... existing imports ...
from sqlalchemy import create_engine, inspect
import pymysql
import os
from pyhive import hive
import sys
# 设置当前路径为项目根目录
os.chdir(os.path.dirname(os.path.abspath(__file__)))
def export_hive_schema(host, port, database, output_file='schema.md',username='hive', password='hive', auth='LDAP',where=None):
    """导出Hive库表结构"""
    conn = hive.Connection(
        host=host,
        port=port,
        database='default',  # 修改默认连接库
        username=username,
        password=password,
        auth=auth
    )
    cursor = conn.cursor()
    
    # 获取数据库列表
    if database.lower() == 'all':
        cursor.execute("SHOW DATABASES")
        databases = [row[0] for row in cursor.fetchall()]
    else:
        databases = [database]

    for db in databases:
        # 获取当前数据库的所有表
        cursor.execute(f"SHOW TABLES IN {db}")
        tables = [row[0] for row in cursor.fetchall()]
        if where:
            tables = [tbl for tbl in tables if where in tbl]

        output = []
        for table in tables:
            # 获取建表语句
            cursor.execute(f"SHOW CREATE TABLE {db}.{table}")
            ddl = '\n'.join([row[0] for row in cursor.fetchall()])
            output.append(f"{ddl};\n\n")

        # 按库名生成文件
        output_file = f'hive_{db}.sql' if database.lower() == 'all' else output_file
        with open(output_file, 'w') as f:
            f.write('\n'.join(output))

def export_mysql_schema(host, port, database, user, password, output_file='schema.md', where=None):
    """导出MySQL库表结构"""
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            charset='utf8mb4'
        )
        cursor = conn.cursor()

        # 获取数据库列表
        if database.lower() == 'all':
            cursor.execute("SHOW DATABASES")
            databases = [row[0] for row in cursor.fetchall() 
                        if row[0] not in ['information_schema', 'mysql', 'performance_schema', 'sys']]
        else:
            databases = [database]

        for db in databases:
            cursor.execute(f"USE `{db}`")
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            if where:
                tables = [tbl for tbl in tables if where in tbl]

            output = []
            for table in tables:
                cursor.execute(f"SHOW CREATE TABLE `{db}`.`{table}`")
                create_table = cursor.fetchone()
                if create_table:
                    ddl = create_table[1]
                    output.append(f"{ddl};\n\n")

            # 按库名生成文件
            output_file = f'mysql_{db}.sql' if database.lower() == 'all' else output_file
            with open(output_file, 'w') as f:
                f.write('\n'.join(output))

    finally:
        cursor.close()
        conn.close()

# 修改参数检查部分
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='导出数据库表结构')
    parser.add_argument('database', help='数据库名称')
    parser.add_argument('--type', choices=['hive', 'mysql'], default='hive', 
                       help='数据库类型 (默认: hive)')
    parser.add_argument('--where', help='表名过滤条件')
    parser.add_argument('--host', help='数据库主机地址')
    parser.add_argument('--port', type=int, help='数据库端口')
    parser.add_argument('--user', help='数据库用户名')
    parser.add_argument('--password', help='数据库密码')
    
    args = parser.parse_args()
    print(args)
    
    # 处理database参数
    if args.database.lower() != 'all':
        assert args.database, "数据库名称不能为空"

    if args.type == 'hive':
        # 保持原有Hive导出逻辑
        export_hive_schema(
            host='10.255.79.164',
            port=10000,
            database=args.database,
            output_file=f'{args.database}.sql',
            username='hive',
            password='hive',
            auth='LDAP',
            where=args.where
        )
    elif args.type == 'mysql':
        # 新增MySQL导出逻辑
        assert args.host and args.port and args.user and args.password, "MySQL需要提供完整连接参数"
        export_mysql_schema(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password,
            output_file=f'{args.database}.sql'
        )

    #all --type mysql --host 10.255.79.191 --port 9030 --user root --password syx123!@#