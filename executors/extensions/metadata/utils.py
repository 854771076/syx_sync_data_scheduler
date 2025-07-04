
from pyhive import hive
import pyhdfs
import re
import pymysql
from loguru import logger
import json,os
from datetime import datetime
import subprocess
from threading import Thread,Lock
# from django.utils import timezone
from datetime import datetime,timedelta
from .base_util import BaseDBUtil



# 库表处理逻辑类
class DatabaseTableHandler:
    # 复制表
    @staticmethod
    def copy_create_table_by_task(task):
        '''
        复制表
        '''
        from ...models import ConfigItem
        tables=DatabaseTableHandler.split(task)
        config = dict(ConfigItem.objects.all().values_list("key", "value"))
        source_columns=get_table_schema_by_config(config,task.data_source,task.source_db,task.source_table,tables)
        target_columns=[]
        sync_time_column=task.sync_time_column or 'cdc_sync_date'
        # 排除不需要字段
        exclude_columns=task.exclude_columns or ''
        for f in source_columns:
            if f['name'] not in exclude_columns:
                target_columns.append({
                    'name': f['name'],
                    'type': DataxTypes.convert_type(task.data_source.type,task.data_target.type,f['type']),
                    'comment':f.get('comment',''),
                    'key':f.get('Key',''),
                    'default':f.get('Default',''),
                    'extra':f.get('Extra',''),
                    'null':f.get('Null','')

                })
        # 同步字段
        # 生成建表语句
        ddl = []
        if task.data_target.type in ['starrocks']:
            ddl.append(f"CREATE TABLE IF NOT EXISTS {task.target_db}.{task.target_table} (")
            column_defs = []
            for col in target_columns:
                if 'string' in col['type']:
                    col['type']='varchar(1048576)'
                col_def = f"  `{col['name']}` {col['type']}"
                if col['null'].upper() == 'NO':
                    col_def += " NOT NULL"
                if col['default']:
                    col_def += f" DEFAULT {col['default']}"
                # if col['extra']:
                #     col_def += f" {col['extra']}"
                if col['comment']:
                    col_def += f" COMMENT '{col['comment']}'"
                column_defs.append(col_def)
            ddl.append(',\n'.join(column_defs))
            ddl.append(f') DISTRIBUTED BY HASH(`{target_columns[0].get("name")}`) BUCKETS 256 PROPERTIES ("replication_num" = "1","in_memory" = "false","storage_format" = "DEFAULT","enable_persistent_index" = "true","compression" = "LZ4");')
        
        elif task.data_target.type in ['mysql']:
            ddl.append(f"CREATE TABLE IF NOT EXISTS {task.target_db}.{task.target_table} (")
            column_defs = []
            for col in target_columns:
                col_def = f"  `{col['name']}` {col['type']}"
                if col['null'].upper() == 'NO':
                    col_def += " NOT NULL"
                if col['default']:
                    col_def += f" DEFAULT {col['default']}"
                # if col['extra']:
                #     col_def += f" {col['extra']}"
                if col['comment']:
                    col_def += f" COMMENT '{col['comment']}'"
                column_defs.append(col_def)
            ddl.append(',\n'.join(column_defs))
            ddl.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")  
        elif task.data_target.type in  ['hive','hdfs']:
            if task.is_add_sync_time:
                target_columns.append({
                    'name':task.sync_time_column,
                    'type': DataxTypes.convert_type('hdfs',task.data_target.type,'timestamp'),
                    'comment':f.get('comment','')
                })
            # Hive建表语句生成
            ddl.append(f"CREATE TABLE IF NOT EXISTS {task.target_db}.{task.target_table} (")
            ddl.append(',\n'.join([f"  {col['name']} {col['type']} COMMENT '{col['comment']}'" for col in target_columns]))
            
            # 添加分区字段
            if task.is_partition and task.partition_column:
                ddl.append(f") PARTITIONED BY ({task.partition_column} STRING) ")
                ddl.append(f"STORED AS ORC LOCATION '/user/hive/warehouse/{task.target_db}.db/{task.target_table}'")
            else:
                ddl.append(f") STORED AS ORC LOCATION '/user/hive/warehouse/{task.target_db}.db/{task.target_table}'")
        else:
            raise ValueError(f"Unsupported database type: {task.data_target.type}")
        # 执行建表语句
        if ddl:
            conn=get_conn_by_config(config,task.data_target)
            cursor = conn.cursor()
            cursor.execute('\n'.join(ddl))
            conn.commit()
            logger.success(f"成功创建表 {task.target_db}.{task.target_table}")

    # 获取连接
    @staticmethod
    def get_connection(db_type, host, port, user, password, database=None):
        '''
        获取连接
        '''
        if db_type == 'hive':
            return hive.Connection(host=host, port=port, username=user, database=database)
        elif db_type == 'hdfs':
            return pyhdfs.HdfsClient(hosts=host, user_name=user)
        elif db_type == 'mysql' or db_type == 'starrocks':
            return pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    @staticmethod
    def get_time_list(start,end,format):
        '''
        生成时间列表
        '''

        date_list = []
        while start < end:
            date_list.append(start.strftime(format))
            start += timedelta(days=1)
        return date_list
    @staticmethod
    def split(task,execute_way='all'):
        # 单库单表
        tables=[]
        format_name=task.source_db + '.' + task.source_table
        if task.split_config.tb_time_suffix:
            # tb_time_suffix_format
            assert task.split_config.tb_time_suffix_format is not None, 'tb_time_suffix_format is None'
            # tb_time_suffix_start_time
            assert task.split_config.tb_time_suffix_start_time is not None, 'tb_time_suffix_start_time is None'
            # tb_time_suffix_end_time
            # assert task.split_config.tb_time_suffix_end_time is not None, 'tb_time_suffix_end_time is None'
            if task.split_config.tb_time_suffix_end_time is None:
                tb_time_suffix_end_time=datetime.now().date()-timedelta(days=task.split_config.tb_time_suffix_update_frequency-1)
            else:
                tb_time_suffix_end_time=task.split_config.tb_time_suffix_end_time
            if execute_way=='all':
                time_list=DatabaseTableHandler.get_time_list(task.split_config.tb_time_suffix_start_time,tb_time_suffix_end_time,task.split_config.tb_time_suffix_format)
            else:
                assert task.split_config.tb_time_suffix_update_frequency is not None, 'tb_time_suffix_update_frequency is None'
                time_list=DatabaseTableHandler.get_time_list(datetime.now().date()-timedelta(days=task.split_config.tb_time_suffix_update_frequency),datetime.now().date()-timedelta(days=task.split_config.tb_time_suffix_update_frequency-1),task.split_config.tb_time_suffix_format)
            for time in time_list:
                tables.append(format_name + '_' + time)
            return tables
        if not task.split_config.db_split and not task.split_config.tb_split and not task.split_config.tb_custom_split and not task.split_config.db_custom_split:
            tables.append(format_name)
            return tables
        # 单库多表
        elif not task.split_config.db_split and task.split_config.tb_split:
            '''
            程序自动添加表下标并且依次遍历分表
            '''
            
            # 检查tb_split_start_number，tb_split_end_number是否有值
            if (task.split_config.tb_split_start_number is  None or task.split_config.tb_split_end_number is  None) and not task.split_config.tb_other:
                raise ValueError('tb_split_start_number or tb_split_end_number is None')
            assert task.split_config.tb_split_start_number <= task.split_config.tb_split_end_number, 'tb_split_start_number must be less than tb_split_end_number'
            for i in range(task.split_config.tb_split_start_number, task.split_config.tb_split_end_number + 1):
                tables.append(format_name + f'_{i}')
            if task.split_config.tb_other:
                tables.append(format_name + '_other')
        # 多库多表
        elif task.split_config.db_split and task.split_config.tb_split:
            '''
            程序自动添加库和表下标并且依次遍历分表
            '''
            assert task.split_config.db_split_start_number <= task.split_config.db_split_end_number, 'db_split_start_number must be less than db_split_end_number'
            # 检查db_split_start_number，db_split_end_number是否有值
            if task.split_config.db_split_start_number is None or task.split_config.db_split_end_number is  None:
                raise ValueError('db_split_start_number or db_split_end_number is None')
            assert task.split_config.tb_split_start_number <= task.split_config.tb_split_end_number, 'tb_split_start_number must be less than tb_split_end_number'
            # 检查tb_split_start_number，tb_split_end_number是否有值
            if task.split_config.tb_split_start_number is  None or task.split_config.tb_split_end_number is  None:
                raise ValueError('tb_split_start_number or tb_split_end_number is None')
            for i in range(task.split_config.db_split_start_number,task.split_config.db_split_end_number + 1):
                for j in range(task.split_config.tb_split_start_number, task.split_config.tb_split_end_number + 1):
                    tables.append(f'{task.source_db}_{i}.{task.source_table}_{j}')
                    if task.split_config.tb_other:
                        tables.append(f'{task.source_db}_{i}.{task.source_table}_other')
            if task.split_config.db_other:
                for j in range(task.split_config.tb_split_start_number, task.split_config.tb_split_end_number + 1):
                    tables.append(f'{task.source_db}_other.{task.source_table}_{j}')
                    if task.split_config.tb_other:
                        tables.append(f'{task.source_db}_other.{task.source_table}_other')      
        # 自定义的分库分表
        if task.split_config.tb_custom_split or task.split_config.db_custom_split:
            if task.split_config.custom_split_db_list and task.split_config.custom_split_db_list:
                for db in task.split_config.custom_split_db_list:
                    for tb in task.split_config.custom_split_tb_list:
                        tables.append(task.source_db+db + '.' + task.source_table+tb)
            if task.split_config.custom_split_db_list and not task.split_config.custom_split_tb_list:
                for db in task.split_config.custom_split_db_list:
                    tables.append(task.source_db+db + '.' + task.source_table)
            if not task.split_config.custom_split_db_list and task.split_config.custom_split_tb_list:
                for tb in task.split_config.custom_split_tb_list:
                    tables.append(task.source_db + '.' + task.source_table+tb)
        return tables 

# Hive工具类
class HiveUtil(BaseDBUtil):    
    name = "hive|hdfs"
    @staticmethod
    def get_client(config,datasource):
        if config.get("ENV")=="dev":
            hive_host_defualt=config.get("HIVE_HOST_DEV")
            hive_port_defualt=int(config.get("HIVE_PORT_DEV",10000))
        else:
            hive_host_defualt=config.get("HIVE_HOST")
            hive_port_defualt=int(config.get("HIVE_PORT",10000))
        hive_host=datasource.connection.params.get('HIVE_HOST',hive_host_defualt)
        hive_port=int(datasource.connection.params.get('HIVE_PORT',hive_port_defualt))
        hive_user=datasource.connection.username
        hive_password=datasource.connection.password
        if hive_user and hive_password:
            return hive.connect(host=hive_host,port=hive_port,username=hive_user,password=hive_password,auth='LDAP')
        else:
            return hive.connect(host=hive_host,port=hive_port)
    
        
    # 通过传入的参数获取hive连接
    @staticmethod
    def get_hive_client_by_params(host,port,username=None,password=None,database=None):
        if username and password:
            return hive.connect(host=host,port=port,username=username,password=password,database=database,auth='LDAP')
        else:
            return hive.connect(host=host,port=port,database=database)
    
    # 获取表结构
    @staticmethod
    def get_table_schema( client, database_name, table_name):
        cursor = client.cursor()
        # 切换数据库
        cursor.execute(f"USE {database_name}")
        # 获取表结构
        cursor.execute(f"DESCRIBE {table_name}")
        results = cursor.fetchall()
        schema = []
        seen_fields = set()  # 用于记录已出现的字段名，避免重复
        for row in results:
            field_name = row[0].strip() if row[0] else None
            field_type = row[1].strip() if row[1] else None
            comment=row[2].strip() if row[2] else ''

            # 过滤无效字段
            if not field_name or '#' in field_name or not field_type:
                continue
            # 避免重复字段
            if field_name not in seen_fields:
                seen_fields.add(field_name)
                schema.append({
                    'name': field_name,
                    'type': field_type,
                    'comment':comment 
                })

        return schema
    
    # 添加某表分区
    @staticmethod
    def add_partition(hive_client, database_name, table_name, partition_columns, partition):
        cursor = hive_client.cursor()
        # 切换数据库
        cursor.execute(f"USE {database_name}")
        # 删除分区
        cursor.execute(f"ALTER TABLE {table_name} DROP IF EXISTS PARTITION ({partition_columns}='{partition}')")
        hive_client.commit()
        # 添加分区
        cursor.execute(f"ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION ({partition_columns}='{partition}')")
        hive_client.commit()
        logger.debug(f"Partition {partition_columns}='{partition}' added to table {table_name} in database {database_name}")
        return True
    
    # 删除某表分区
    @staticmethod
    def drop_partition(hive_client, database_name, table_name, partition_columns, partition):
        cursor = hive_client.cursor()
        # 切换数据库
        cursor.execute(f"USE {database_name}")
        # 删除分区
        cursor.execute(f"ALTER TABLE {table_name} DROP IF EXISTS PARTITION ({partition_columns}='{partition}')")
        hive_client.commit()
        logger.debug(f"Partition {partition_columns}='{partition}' dropped from table {table_name} in database {database_name}")
        return True
        
    # 检查某表是否存在
    @staticmethod
    def check_table_exists(hive_client, database_name, table_name):
        cursor = hive_client.cursor()
        # 切换数据库
        cursor.execute(f"USE {database_name}")
        # 检查表是否存在
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchone()
        if result:
            logger.debug(f"Table {table_name} exists in database {database_name}")
            return True
        else:
            logger.debug(f"Table {table_name} does not exist in database {database_name}")
            return False


def get_conn_by_config(config,data_source):
    DB_UTILS = { }
    for util in BaseDBUtil.__subclasses__():
        keys = util.name.split('|')
        for key in keys:
            DB_UTILS[key] = util
    if not DB_UTILS:
        raise ValueError("No database utility classes found.")
    db_type = data_source.type.lower()
    util_class = DB_UTILS.get(db_type)
    if util_class:
        return util_class.get_client(config, data_source)
    raise ValueError(f"Unsupported database type: {db_type}")
def get_table_schema_by_config(config,data_source,db_name,table_name,tables=[]):
    DB_UTILS = { }
    for util in BaseDBUtil.__subclasses__():
        keys = util.name.split('|')
        for key in keys:
            DB_UTILS[key] = util
    if not DB_UTILS:
        raise ValueError("No database utility classes found.")
    db_type = data_source.type.lower()
    util_class = DB_UTILS.get(db_type)
    if not util_class:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    conn = get_conn_by_config(config, data_source)
    if tables:
        source_db, source_table = tables[0].split(".")
    else:
        source_db, source_table = db_name, table_name
    
    return util_class.get_table_schema(conn, source_db, source_table)

def _sync_single_metadata(data_source, db_name:str, table_name:str,config,tables=[]):
    from executors.models import MetadataTable
    """缓存表字段"""
    # 根据不同类型获取元数据
    columns=get_table_schema_by_config(config,data_source,db_name,table_name,tables)
    # 保存到元数据表中，如果已存在则更新
    cls, created = MetadataTable.objects.update_or_create(
        data_source_id=data_source.id,
        db_name=db_name,
        name=table_name,
        defaults={
            'meta_data': columns,
            'description': f"自动同步表结构 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        }
    )

    logger.debug(f"Created metadata for {data_source}_{db_name}.{table_name}")
    return columns

# Hdfs工具类
class HdfsUtil:
    @staticmethod
    def get_hdfs_client_by_config(config,datasource):
        HadoopClient=datasource.connection.params.get('HadoopClient')
        username=datasource.connection.username
        if username:
            return pyhdfs.HdfsClient(hosts=HadoopClient,user_name=username)
        else:
            return pyhdfs.HdfsClient(hosts=HadoopClient)

    @staticmethod
    def drop_hive_table(hdfs_client, database_name, table_name,partition_columns=None,partition=None):
        if partition_columns:
            path=f'/user/hive/warehouse/{database_name}.db/{table_name}/{partition_columns}={partition}/*'
        else:
            path=f'/user/hive/warehouse/{database_name}.db/{table_name}/*'
        # 删除hdfs目录下的文件
        HdfsUtil.delete_file(hdfs_client, path,recursive=False)
        logger.debug(f"Deleted hive table: {database_name}.{table_name}, partition: {partition}")

        
    #删除hdfs目录下的文件
    @staticmethod
    def delete_file(hdfs_client, path,recursive=False):
        """
        删除 HDFS 上的文件或目录。

        :param hdfs_client: HDFS 客户端对象
        :param path: 文件或目录的路径
        :return: True 如果删除成功，False 否则
        """
        try:
            hdfs_client.delete(path, recursive=recursive)  # 递归删除目录及其内容
            logger.debug(f"Deleted file or directory: {path}")
            return True
        except Exception as e:
            logger.exception(f"Failed to delete file or directory: {path}. Error: {e}")
            return False

# Mysql工具类
class MysqlUtil(BaseDBUtil):
    name = "mysql|starrocks"
    @staticmethod
    def get_client(config,datasource):
        mysql_host=datasource.connection.host
        mysql_port=datasource.connection.port
        mysql_user=datasource.connection.username
        mysql_password=datasource.connection.password
        if mysql_user and mysql_password:
            return pymysql.connect(host=mysql_host,port=mysql_port,user=mysql_user,password=mysql_password)
        else:
            return pymysql.connect(host=mysql_host,port=mysql_port)

    # 获取表结构
    @staticmethod
    def get_table_schema(client, database_name, table_name):
        cursor = client.cursor(pymysql.cursors.DictCursor)
        # 切换数据库
        cursor.execute(f"USE {database_name}")
        # 获取表结构
        cursor.execute(f"DESCRIBE {table_name}")
        results = list(cursor.fetchall())
        for i in results:
            i['name']=i['Field']
            i['type']=i['Type']
        cursor.execute(f"SHOW CREATE TABLE {table_name}")
        ddl=cursor.fetchone().get('Create Table')
        if ddl:
            pattern = r"^\s*`(.+)`\s+([^\s,]+)\s*(?:[^,]*?COMMENT\s+\'(.*)\')?"
            matches = re.finditer(pattern,ddl, re.MULTILINE)
            for match in matches:
                field_name = match.group(1).strip()
                field_type = match.group(2).strip()
                field_comment = match.group(3).strip() if match.group(3) else ''
                for result in results:
                    if field_name  in result.get('Field'):
                        result['comment']=field_comment
                        break
        return results


        
# Datax工具类
class DataxUtil:
    # 解析执行日志
    @staticmethod
    def parse_log(content:str):
        """
        解析 DataX 执行日志文件，提取关键信息。

        :param log_path: 日志文件路径
        :return: 包含关键信息的字典
        """
        text=re.sub('\s','',content)
        try:
            read_num=int(re.findall('读出记录总数:(\d+)',text)[0])
        except:
            read_num=0
        try:
            error_num=int(re.findall('读写失败总数:(\d+)',text)[0])
        except:
            error_num=0
        try:
            is_None=bool(re.findall('您尝试读取的文件目录为空',text))
        except:
            is_None=False
        return {
            'read_num':read_num,
            'error_num':error_num,
            'is_None':is_None, 
        }
    
        
class DataxTypes:
    """
    该类用于实现数据源类型（如 MySQL、Hive）到 DataX 内部类型的转换。
    """
    # MySQL 数据类型到 DataX 内部类型的映射
    MYSQL_TO_DATAX = {
        'int': 'long',
        'tinyint': 'long',
        'smallint': 'long',
        'mediumint': 'long',
        'bigint': 'long',
        'float': 'double',
        'double': 'double',
        'decimal': 'double',
        'varchar': 'string',
        'char': 'string',
        'tinytext': 'string',
        'text': 'string',
        'mediumtext': 'string',
        'longtext': 'string',
        'year': 'string',
        'date': 'date',
        'datetime': 'date',
        'timestamp': 'date',
        'time': 'date',
        'bit': 'boolean',
        'bool': 'boolean',
        'tinyblob': 'bytes',
        'mediumblob': 'bytes',
        'blob': 'bytes',
        'longblob': 'bytes',
        'varbinary': 'bytes'
    }
    DATAX_TO_MYSQL={
        'long': 'int',
        'double': 'double',
        'string': 'longtext',
        'date': 'datetime',
        'boolean': 'bool',
        'bytes': 'longblob',
    }
    
    # STARROCKS
    STARROCKS_TO_DATAX = {
        'int': 'long',
        'tinyint': 'long',
        'smallint': 'long',
        'mediumint': 'long',
        'bigint': 'long',
        'largeint': 'long',
        'float': 'double',
        'double': 'double',
        'decimal': 'double',
        'varchar': 'string',
        'char': 'string',
        'tinytext': 'string',
        'text': 'string',
        'mediumtext': 'string',
        'longtext': 'string',
        'string': 'string',
        'year': 'string',
        'date': 'date',
        'datetime': 'date',
        'timestamp': 'date',
        'time': 'date',
        'bit': 'boolean',
        'bool': 'boolean',
        'boolean': 'boolean',
        'tinyblob': 'bytes',
        'mediumblob': 'bytes',
        'blob': 'bytes',
        'longblob': 'bytes',
        'binary': 'bytes',
        'varbinary': 'bytes'
    }
    DATAX_TO_STARROCKS={
        'long': 'largeint',
        'double': 'double',
        'string': 'string',
        'date': 'datetime',
        'boolean': 'boolean',
        'bytes': 'binary',
    }
    # Hive 数据类型到 DataX 内部类型的映射
    HIVE_TO_DATAX = {
        'tinyint': 'long',
        'smallint': 'long',
        'int': 'long',
        'bigint': 'long',
        'float': 'double',
        'double': 'double',
        'string': 'string',
        'varchar': 'string',
        'char': 'string',
        'boolean': 'boolean',
        'date': 'date',
        'timestamp': 'date'
    }
    DATAX_TO_HIVE={
        'long': 'bigint',
        'double': 'double',
        'string': 'string',
        'date': 'timestamp',
        'boolean': 'boolean',         
    }
    @staticmethod
    def format_reader_schema(schema:list):
        """
        格式化reader字段为sql语句
        """
        if not schema:
            return ''
        result=[]
        for item in schema:
            if item.get('value'):
                result.append(f"{item.get('value')} as `{item.get('name')}`")
            else:
                result.append(f'`{item.get("name")}`')
        return ','.join(result)

    @staticmethod
    def format_type(type:str):
        """
        格式化类型
        """
        return type.split('(')[0]
    @staticmethod
    def mysql_to_datax(mysql_type: str) -> str:
        """
        将 MySQL 数据类型转换为 DataX 内部类型。

        :param mysql_type: MySQL 数据类型，如 'int', 'varchar' 等
        :return: 对应的 DataX 内部类型
        :raises ValueError: 如果未找到对应的 DataX 内部类型
        """
        datax_type = DataxTypes.MYSQL_TO_DATAX.get(DataxTypes.format_type(mysql_type.lower()),"string")
        if datax_type is None:
            raise ValueError(f"Unsupported MySQL type: {mysql_type}")
        return datax_type
    @staticmethod
    def starrocks_to_datax(starrocks_type: str) -> str:
        """
        将 starrocks 数据类型转换为 DataX 内部类型。

        :param starrocks_type: starrocks 数据类型，如 'int', 'varchar' 等
        :return: 对应的 DataX 内部类型
        :raises ValueError: 如果未找到对应的 DataX 内部类型
        """
        datax_type = DataxTypes.STARROCKS_TO_DATAX.get(DataxTypes.format_type(starrocks_type.lower()),"string")
        if datax_type is None:
            raise ValueError(f"Unsupported STARROCKS type: {starrocks_type}")
        return datax_type
    @staticmethod
    def datax_to_starrocks( datax_type: str) -> str:
        """
        将 DataX 内部类型转换为 starrocks 数据类型。

        :param datax_type: DataX 内部类型，如 'long', 'string' 等
        :return: 对应的 starrocks 数据类型
        :raises ValueError: 如果未找到对应的 starrocks 数据类型
        """
        starrocks_type = DataxTypes.DATAX_TO_STARROCKS.get(DataxTypes.format_type(datax_type.lower()),"string")
        if starrocks_type is None:
            raise ValueError(f"Unsupported DataX type: {datax_type}")
        return starrocks_type
    @staticmethod
    def datax_to_mysql( datax_type: str) -> str:
        """
        将 DataX 内部类型转换为 MySQL 数据类型。

        :param datax_type: DataX 内部类型，如 'long', 'string' 等
        :return: 对应的 MySQL 数据类型
        :raises ValueError: 如果未找到对应的 MySQL 数据类型
        """
        mysql_type = DataxTypes.DATAX_TO_MYSQL.get(DataxTypes.format_type(datax_type.lower()),"longtext")
        if mysql_type is None:
            raise ValueError(f"Unsupported DataX type: {datax_type}")
        return mysql_type

    @staticmethod
    def hive_to_datax( hive_type: str) -> str:
        """
        将 Hive 数据类型转换为 DataX 内部类型。

        :param hive_type: Hive 数据类型，如 'INT', 'string' 等
        :return: 对应的 DataX 内部类型
        :raises ValueError: 如果未找到对应的 DataX 内部类型
        """
        datax_type = DataxTypes.HIVE_TO_DATAX.get(DataxTypes.format_type(hive_type.lower()),"string")
        if datax_type is None:
            raise ValueError(f"Unsupported Hive type: {hive_type}")
        return datax_type
    
    @staticmethod
    def datax_to_hive(datax_type: str) -> str:
        """
        将 DataX 内部类型转换为 Hive 数据类型。

        :param datax_type: DataX 内部类型，如 'long', 'string' 等
        :return: 对应的 Hive 数据类型
        :raises ValueError: 如果未找到对应的 Hive 数据类型
        """
        hive_type = DataxTypes.DATAX_TO_HIVE.get(DataxTypes.format_type(datax_type.lower()),"string")
        if hive_type is None:
            raise ValueError(f"Unsupported DataX type: {datax_type}")
        return hive_type
    @staticmethod
    def convert_to_datax_type(source_type: str, source: str) -> str:
        """
        通用类型转换方法，支持多种数据源类型。

        :param source_type: 源数据源类型，如 'mysql', 'hive' 等
        :param target_type: 目标数据源类型，如 'mysql', 'hive' 等
        :param value: 要转换的值
        :return: 转换后的值
        """
        conversion_map = {
            'mysql': lambda s: DataxTypes.mysql_to_datax(s),
            'starrocks': lambda s: DataxTypes.starrocks_to_datax(s),
            'hdfs': lambda s: DataxTypes.hive_to_datax(s),
        }

            
        key = source_type.lower()
        if key in conversion_map:
            return conversion_map[key](source)
            
        raise ValueError(f"Unsupported type conversion: {source_type} to datax type")
    # 通用统一转换方法，多数据源类型，根据源数据源类型转换到目标数据源类型
    @staticmethod
    def convert_type(source_type: str, target_type: str, source: str) -> str:
        """
        通用类型转换方法，支持多种数据源类型。

        :param source_type: 源数据源类型，如 'mysql', 'hive' 等
        :param target_type: 目标数据源类型，如 'mysql', 'hive' 等
        :param value: 要转换的值
        :return: 转换后的值
        """
        conversion_map = {
            ('mysql', 'hdfs'): lambda s: DataxTypes.datax_to_hive(DataxTypes.mysql_to_datax(s)),
            ('mysql', 'starrocks'): lambda s: DataxTypes.datax_to_starrocks(DataxTypes.mysql_to_datax(s)),
            ('starrocks', 'mysql'): lambda s: DataxTypes.datax_to_mysql(DataxTypes.starrocks_to_datax(s)),
            ('starrocks','hdfs'): lambda s: DataxTypes.datax_to_hive(DataxTypes.starrocks_to_datax(s)),
            ('hdfs', 'starrocks'): lambda s: DataxTypes.datax_to_starrocks(DataxTypes.hive_to_datax(s)),
            ('hdfs', 'mysql'): lambda s: DataxTypes.datax_to_mysql(DataxTypes.hive_to_datax(s)),
        }

        if source_type == target_type:
            return source
            
        key = (source_type.lower(), target_type.lower())
        if key in conversion_map:
            return conversion_map[key](source)
            
        raise ValueError(f"Unsupported type conversion: {source_type} to {target_type}")


