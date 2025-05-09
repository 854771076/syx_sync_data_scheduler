
from pyhive import hive
import pyhdfs
import re
import pymysql
from loguru import logger
import json,os
from datetime import datetime
import subprocess
from threading import Thread,Lock
from django.utils import timezone
# 库表处理逻辑类
class DatabaseTableHandler:

    @staticmethod
    def split(task):
        # 单库单表
        tables=[]
        format_name=task.source_db + '.' + task.source_table
        # 自定义的分库分表
        
        if not task.split_config.db_split and not task.split_config.tb_split:
            tables.append(format_name)
            return tables
        # 单库多表
        elif not task.split_config.db_split and task.split_config.tb_split:
            '''
            程序自动添加表下标并且依次遍历分表
            '''
            assert task.split_config.tb_split_start_number <= task.split_config.tb_split_end_number, 'tb_split_start_number must be less than tb_split_end_number'
            # 检查tb_split_start_number，tb_split_end_number是否有值
            if task.split_config.tb_split_start_number is  None or task.split_config.tb_split_end_number is  None:
                raise ValueError('tb_split_start_number or tb_split_end_number is None')
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
                        
        else:
            raise ValueError('db_split and tb_split must be True or False')
        if task.split_config.custom_split_db_list and task.split_config.custom_split_db_list:
            for db in task.split_config.custom_split_db_list:
                for tb in task.split_config.custom_split_tb_list:
                    tables.append(db + '.' + tb)
        if task.split_config.custom_split_db_list and not task.split_config.custom_split_tb_list:
            for db in task.split_config.custom_split_db_list:
                tables.append(db + '.' + task.source_table)
        if not task.split_config.custom_split_db_list and task.split_config.custom_split_tb_list:
            for tb in task.split_config.custom_split_tb_list:
                tables.append(task.source_db + '.' + tb)
        return tables 

# Hive工具类
class HiveUtil:    
    # 通过全局配置获取hive连接
    @staticmethod
    def get_hive_client_by_config(config,datasource):
        hive_host=config.get("HIVE_HOST")
        hive_port=int(config.get("HIVE_PORT",10000))
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
    def get_table_schema(hive_client, database_name, table_name):
        cursor = hive_client.cursor()
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
            # 过滤无效字段
            if not field_name or '#' in field_name or not field_type:
                continue
            # 避免重复字段
            if field_name not in seen_fields:
                schema.append({
                    'name': field_name,
                    'type': field_type
                })
                seen_fields.add(field_name)
        return schema
    
    # 添加某表分区
    @staticmethod
    def add_partition(hive_client, database_name, table_name, partition_columns, partition):
        cursor = hive_client.cursor()
        # 切换数据库
        cursor.execute(f"USE {database_name}")
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

    # @staticmethod
    # def extract_namenode_ips(hadoop_config):
    #     """
    #     从 Hadoop 配置 JSON 数据中提取所有 NameNode 的 IP 地址，并用逗号分割。

    #     :param hadoop_config: 包含 Hadoop 配置的字典
    #     :return: 用逗号分割的 IP 地址字符串
    #     """
    #     try:
    #         # 获取 nameservice 名称
    #         nameservice = hadoop_config["dfs.nameservices"]
    #         # 获取 namenode 列表
    #         namenodes = hadoop_config[f"dfs.ha.namenodes.{nameservice}"].split(',')
    #         # 提取所有 namenode 的 IP 地址
    #         ip_addresses = []
    #         for namenode in namenodes:
    #             key = f"dfs.namenode.rpc-address.{nameservice}.{namenode}"
    #             if key in hadoop_config:
    #                 ip = hadoop_config[key].split(':')[0]
    #                 ip_addresses.append(ip)
    #         # 用逗号分割 IP 地址
    #         return ','.join(ip_addresses)
    #     except KeyError:
    #         return ""
    # 删除hive目录下的文件
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
class MysqlUtil:
    @staticmethod
    def get_mysql_client_by_config(datasource):
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
    def get_table_schema(mysql_client, database_name, table_name):
        cursor = mysql_client.cursor()
        # 切换数据库
        cursor.execute(f"USE {database_name}")
        # 获取表结构
        cursor.execute(f"DESCRIBE {table_name}")
        results = cursor.fetchall()
        schema = []
        # 返回
        for row in results:
            field_name = row[0].strip() if row[0] else None
            field_type = row[1].strip() if row[1] else None
            schema.append({
                'name': field_name,
                'type': field_type 
            })
        return schema


        
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
    @staticmethod
    def execute_datax(cls,retry=False):
        from ...models import Task,ConfigItem,Log,DataSource
        """
        执行 DataX 任务。
        """
        try:
            if retry:
                config_path = cls.output_dir / f"{cls.task.id}_retry.json"
                log_path = cls.logs_dir / f"{cls.task.id}_retry.log"
            else:
                config_path = cls.output_dir / f"{cls.task.id}.json"
                log_path = cls.logs_dir / f"{cls.task.id}.log"
            start_time=timezone.now()
            # 判断系统
            if os.name == 'nt':
                command = f'set HADOOP_USER_NAME={cls.task.project.tenant.name}&&{cls.config["PYTHON_BIN_PATH"]} {cls.config["DATAX_BIN_PATH"]} --jvm="{cls.jvm_options}" {config_path} > {log_path}'
            else:
                command = f'HADOOP_USER_NAME={cls.task.project.tenant.name} {cls.config["PYTHON_BIN_PATH"]} {cls.config["DATAX_BIN_PATH"]} --jvm="{cls.jvm_options}" {config_path} > {log_path}'
            # 执行中日志
            logger.info(f"执行 DataX 任务 {cls.task.id}，命令：{command}")
            log_obj, created = Log.objects.get_or_create(
                task=cls.task,
                partition_date=cls.settings.get('partition_date'),
                execute_way=cls.settings.get('execute_way'),
                defaults={
                    'executed_state': 'process',
                    'complit_state': 2,
                    'start_time': start_time,
                    'end_time': None,
                    'local_row_update_time_start': start_time,
                    'local_row_update_time_end': None,
                    'numrows': None,
                    'remark': '执行中',
                    'datax_json': cls.task.datax_json,
                }
            )
            if not created:
                # 如果记录已存在，则更新
                log_obj.executed_state = 'process'
                log_obj.complit_state = 2
                log_obj.start_time = start_time
                log_obj.end_time = None
                log_obj.local_row_update_time_start = start_time
                log_obj.local_row_update_time_end = None
                log_obj.numrows = None
                log_obj.remark = '执行中'
                log_obj.datax_json = cls.task.datax_json
                log_obj.save()
            result = subprocess.run(
                    command,
                    shell=True, encoding='utf-8'
                )
            end_time=timezone.now()
            # 保存执行日志
            with open(cls.logs_dir / f"{cls.task.id}.log", "r", encoding="utf-8") as log_file:
                log_data=log_file.read()
            
            # 处理执行结果
            if result.returncode == 0:
                logger.info(f"DataX 任务 {cls.task.id} 执行成功")
                # 解析执行日志
                log=DataxUtil.parse_log(log_data)
                # 记录日志
                if log.get('error_num')>0:
                    logger.error(f"DataX 任务 {cls.task.id} 执行失败")
                    # 按唯一键查找或创建记录
                    log_obj, created = Log.objects.get_or_create(
                        task=cls.task,
                        partition_date=cls.settings.get('partition_date'),
                        execute_way=cls.settings.get('execute_way'),
                        defaults={
                            'executed_state': 'fail',
                            'complit_state': 0,
                            'start_time': start_time,
                            'end_time': end_time,
                            'local_row_update_time_start': start_time,
                            'local_row_update_time_end': end_time,
                            'numrows': log.get('read_num'),
                            'remark': "查看详细日志",
                            'datax_json': cls.task.datax_json,
                        }
                    )
                    if not created:
                        # 如果记录已存在，则更新
                        log_obj.executed_state = 'fail'
                        log_obj.complit_state = 0
                        log_obj.start_time = start_time
                        log_obj.end_time = end_time
                        log_obj.local_row_update_time_start = start_time
                        log_obj.local_row_update_time_end = end_time
                        log_obj.numrows = log.get('read_num')
                        log_obj.remark = "查看详细日志"
                        log_obj.datax_json = cls.task.datax_json
                        log_obj.save()

                elif log.get('is_None'):
                    logger.warning(f"DataX 任务 {cls.task.id} 执行完成，但是没有读取到数据")
                    # 按唯一键查找或创建记录
                    log_obj, created = Log.objects.get_or_create(
                        task=cls.task,
                        partition_date=cls.settings.get('partition_date'),
                        execute_way=cls.settings.get('execute_way'),
                        defaults={
                            'executed_state': 'success',
                            'complit_state': 1,
                            'start_time': start_time,
                            'end_time': end_time,
                            'local_row_update_time_start': start_time,
                            'local_row_update_time_end': end_time,
                            'numrows': log.get('read_num'),
                            'remark': f"DataX 任务 {cls.task.id} 执行完成，但是没有读取到数据",
                            'datax_json': cls.task.datax_json,
                        }
                    )
                    if not created:
                        # 如果记录已存在，则更新
                        log_obj.executed_state = 'success'
                        log_obj.complit_state = 1
                        log_obj.start_time = start_time
                        log_obj.end_time = end_time
                        log_obj.local_row_update_time_start = start_time
                        log_obj.local_row_update_time_end = end_time
                        log_obj.numrows = log.get('read_num')
                        log_obj.remark = f"DataX 任务 {cls.task.id} 执行完成，但是没有读取到数据"
                        log_obj.datax_json = cls.task.datax_json
                        log_obj.save()
                else:
                    logger.info(f"DataX 任务 {cls.task.id} 执行完成，读取到 {log.get('read_num')} 条数据")
                    # 按唯一键查找或创建记录
                    log_obj, created = Log.objects.get_or_create(
                        task=cls.task,
                        partition_date=cls.settings.get('partition_date'),
                        execute_way=cls.settings.get('execute_way'),
                        defaults={
                            'executed_state': 'success',
                            'complit_state': 1,
                            'start_time': start_time,
                            'end_time': end_time,
                            'local_row_update_time_start': start_time,
                            'local_row_update_time_end': end_time,
                            'numrows': log.get('read_num'),
                            'remark': f"DataX 任务 {cls.task.id} 执行完成，读取到 {log.get('read_num')} 条数据",
                            'datax_json': cls.task.datax_json,
                        }
                    )
                    if not created:
                        # 如果记录已存在，则更新
                        log_obj.executed_state = 'success'
                        log_obj.complit_state = 1
                        log_obj.start_time = start_time
                        log_obj.end_time = end_time
                        log_obj.local_row_update_time_start = start_time
                        log_obj.local_row_update_time_end = end_time
                        log_obj.numrows = log.get('read_num')
                        log_obj.remark = f"DataX 任务 {cls.task.id} 执行完成，读取到 {log.get('read_num')} 条数据"
                        log_obj.datax_json = cls.task.datax_json
                        log_obj.save()
                
                return True,result.stdout
            else:
                end_time=timezone.now()
                logger.exception(f"DataX 任务 {cls.task.id} 执行失败")
                # 按唯一键查找或创建记录
                log_obj, created = Log.objects.get_or_create(
                    task=cls.task,
                    partition_date=cls.settings.get('partition_date'),
                    execute_way=cls.settings.get('execute_way'),
                    defaults={
                        'executed_state': 'fail',
                        'complit_state': str(0),
                        'start_time': start_time,
                        'end_time': end_time,
                        'local_row_update_time_start': start_time,
                        'local_row_update_time_end': end_time,
                        'numrows': 0,
                        'remark': "查看详细日志",
                        'datax_json': cls.task.datax_json,
                    }
                )
                if not created:
                    # 如果记录已存在，则更新
                    log_obj.executed_state = 'fail'
                    log_obj.complit_state = str(0)
                    log_obj.start_time = start_time
                    log_obj.end_time = end_time
                    log_obj.local_row_update_time_start = start_time
                    log_obj.local_row_update_time_end = end_time
                    log_obj.numrows = 0
                    log_obj.remark = "查看详细日志"
                    log_obj.datax_json = cls.task.datax_json
                    log_obj.save()
                return False,result.stderr
        except Exception as e:
            logger.exception(f"DataX 任务 {cls.task.id} 执行异常: {e}")
            # 按唯一键查找或创建记录
            log_obj, created = Log.objects.get_or_create(
                task=cls.task,
                partition_date=cls.settings.get('partition_date'),
                execute_way=cls.settings.get('execute_way'),
                defaults={
                    'executed_state': 'fail',
                    'complit_state': str(0),
                    'start_time': start_time,
                    'end_time': end_time,
                    'local_row_update_time_start': start_time,
                    'local_row_update_time_end': end_time,
                    'numrows': 0,
                    'remark': str(e),
                    'datax_json': cls.task.datax_json,
                }
            )
            if not created:
                # 如果记录已存在，则更新
                log_obj.executed_state = 'fail'
                log_obj.complit_state = str(0)
                log_obj.start_time = start_time
                log_obj.end_time = end_time
                log_obj.local_row_update_time_start = start_time
                log_obj.local_row_update_time_end = end_time
                log_obj.numrows = 0
                log_obj.remark = str(e)
                log_obj.datax_json = cls.task.datax_json
                log_obj.save()
            return False,str(e)
        
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
        'long': 'bigint',
        'double': 'double',
        'string': 'longtext',
        'date': 'datetime',
        'boolean': 'bool',
        'bytes': 'longblob',
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