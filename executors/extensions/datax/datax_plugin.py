import json
import subprocess
from pathlib import Path

from typing import Union,List,Dict,Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime,timedelta,date
from .utils import DatabaseTableHandler,HiveUtil,HdfsUtil,DataxUtil,DataxTypes,MysqlUtil
from loguru import logger
from executors.alerts import AlertFactory

class settings:
    execute_way = None
    start_time = None
    end_time = None
    partition_date = None
    max_worker=None
class DataXPluginManager:
    def __init__(self,tasks,settings:settings={}):
        from ...models import Task,ConfigItem,Log
        logger.debug(f"[datax_plugin]:init DataXPluginManager with {len(tasks)} tasks,settings={settings}")
        
        self.settings=settings
        self.config=dict(ConfigItem.objects.all().values_list("key", "value"))
        assert self.settings.get("execute_way"),"[datax_plugin]:execute_way is required"
        assert self.settings.get("execute_way") in ["all","update","other","action"],f"[datax_plugin]:execute_way must be in ['all','update','other','action']"
        self.today=date.today()
        self.yesterday=date.today()-timedelta(days=1)
        self.partition_date=self.settings.get("partition_date") or self.yesterday.strftime("%Y%m%d")
        self.settings["today"]=self.today
        self.settings["yesterday"]=self.yesterday
        self.settings["partition_date"]=self.partition_date
        self.plugins = []
        self.tasks=tasks
        self.max_worker=int(settings.get("max_worker",self.config.get("DATAX_MAX_WORKER",10)))
        self.pool=ThreadPoolExecutor(max_workers=self.max_worker)
        self.register()
        logger.debug(f"[datax_plugin]:init DataXPluginManager with {len(self.tasks)} tasks,max_worker={self.max_worker},partition_date={self.partition_date},execute_way={self.settings.get('execute_way')}")
    def register(self):
        """注册DataX插件"""
        for task in self.tasks:
            self.plugins.append( DataXPlugin(task,self.config,self.settings))
            logger.debug(f"[datax_plugin]:register task {task.id}")

    def execute_tasks(self):
        """执行DataX任务"""
        futures = [self.pool.submit(task.execute) for task in self.plugins]
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                logger.exception(e)
                continue
    def execute_action(self):
        """执行DataX任务"""
        futures = [self.pool.submit(task.execute_action) for task in self.plugins]
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                logger.exception(e)
                continue
    def execute_retry(self,log):
        """执行DataX任务"""
        futures = [self.pool.submit(task.execute_retry,log) for task in self.plugins]
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                logger.exception(e)
                continue
    def generate_config(self):
        """生成DataX配置"""
        for task in self.plugins:
            task.generate_config()

        
class DataXPlugin:
    def __init__(self, task,config,settings):
        logger.debug(f"[datax_plugin]:init DataXPlugin with task {task.id},config={config},settings={settings}")
        self.task = task
        self.output_dir = Path(__file__).parent / "output"
        self.logs_dir = Path(__file__).parent / "logs"
        self.logs_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        self.config = config
        self.settings=settings
        self.jvm_options = self.task.config.get('jvm_options',settings.get("jvm_options", self.config.get("DATAX_JVM_OPTIONS", "")))
        self.source = self.task.data_source.first()
        self.target = self.task.data_target.first()
        self.hive:HiveUtil = None
        self.hdfs:HdfsUtil = None
        self.mysql:MysqlUtil = None
        self.alert=AlertFactory(self.task.project.notification)(project=self.task.project)
        self.source_columns=[]
        self.target_columns=[]
        self.mode='append'
        self._validate_config()
        try:
            self._init_clients()
        except Exception as e:
            logger.exception(e)
    def _validate_config(self):
        """验证配置"""
        assert self.task.data_source.count() == 1, f"[datax_plugin]:task {self.task.id} must have one data source"
        assert self.task.data_target.count() == 1, f"[datax_plugin]:task {self.task.id} must have one data target"
        if self.source.type == "mysql" and self.target.type == "hdfs":
            # settings必须有分区时间，执行方式
            assert self.settings.get("partition_date"), f"[datax_plugin]:execute_way is update,partition_date is required"
            assert self.settings.get("execute_way"), f"[datax_plugin]:execute_way is required"
        else:
            assert False, f"[datax_plugin]:unsupported data source or data target"
    def _init_clients(self):
        """初始化工具类"""
        
        if self.source.type == "hdfs" or self.target.type == "hdfs":
            self.hive = HiveUtil.get_hive_client_by_config(self.config,self.target)
            self.hdfs = HdfsUtil.get_hdfs_client_by_config(self.config,self.target)
        if self.source.type == "mysql" or self.target.type == "mysql":
            self.mysql = MysqlUtil.get_mysql_client_by_config(self.source)
    def _get_reader_config(self):
        """生成reader配置"""
        if self.source.type == "mysql" and self.target.type == "hdfs":
            return Reader.mysql2hive(self)
        elif self.source.type == "mysql" and self.target.type == "mysql":
            pass
        else:
            assert False, f"[datax_plugin]:不支持的数据源类型[{self.source.type}]"
    def _exclude_column(self,columns:list):
        """排除列"""
        exclude_column=list(filter(lambda x:x not in self.task.column_config.exclude_columns,columns))
        return exclude_column

    def _get_writer_config(self):
        """生成writer配置"""
        if self.target.type == "hdfs":
            return Writer.other2hive(self)
        else:
            assert False, f"[datax_plugin]:不支持的目标类型[{self.target.type}]"

    def generate_config(self):
        """生成DataX配置文件"""
        
        config = {
            "core":json.loads(self.config.get("DATAX_CORE_SETTINGS")),
            "job": {
                "setting": json.loads(self.config.get("DATAX_SETTINGS")),
                "content": [
                    {
                        "reader": self._get_reader_config(),
                        "writer": self._get_writer_config()
                    }
                ]
            }
        }

        self.task.datax_json=config
        self.task.save()
        # 读取任务中的json
        config = self.task.datax_json
        config_path = self.output_dir / f"{self.task.id}.json"
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=4)
        return True
    def execute_retry(self,log):
        """执行DataX任务"""
        config = log.datax_json
        config_path = self.output_dir / f"{self.task.id}_retry.json"
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=4)
        result = DataxUtil.execute_datax(self,retry=True)
    def is_completed(self):
        from ...models import Task,ConfigItem,Log
        return Log.objects.filter(
            task=self.task,
            execute_way=self.settings.get('execute_way'),
            partition_date=self.settings.get('partition_date'),
            complit_state=Log.complitStateChoices.success
        ).exists()
    def execute_action(self):
        """执行DataX任务"""
        self.pre_execute()
        logger.info(f"[datax_plugin]:task {self.task.id} 开始执行--{self.settings.get('execute_way')}--{self.settings.get('partition_date')}")
        # 读取任务中的json
        config = self.task.datax_json
        config_path = self.output_dir / f"{self.task.id}.json"
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=4)
        # 执行命令
        result = DataxUtil.execute_datax(self)
    # 预执行
    def pre_execute(self):
        """预执行"""
        if self.task.column_config.is_partition:
            HiveUtil.add_partition(self.hive,self.task.target_db,self.task.target_table,self.task.column_config.partition_column,self.settings.get('partition_date'))
        return True
    def execute(self):
        """执行DataX任务"""
        if self.is_completed():
            logger.info(f"[datax_plugin]:task {self.task.id} 当日已执行，跳过")
            return False
        self.pre_execute()
        config_path = self.output_dir / f"{self.task.id}.json"
        logger.info(f"[datax_plugin]:task {self.task.id} 开始执行--{self.settings.get('execute_way')}--{self.settings.get('partition_date')}")
        self.generate_config()
        result = DataxUtil.execute_datax(self)
    # 通过reader_transform_columns配置转换字段名字
    def _transform_columns(self,columns:List[Dict[str,Any]],mapping:Dict[str,str]):
        """转换字段名字"""
        new_columns=[]
        for column in columns:
            if column['name'] in mapping:
                column['name'] = mapping[column['name']]
            new_columns.append(column)
        return new_columns


        
   
class Reader:
    @staticmethod
    def mysql2hive(self:DataXPlugin):
        """mysql2hive 生成reader逻辑"""
        assert self.source.type == "mysql", f"[datax_plugin]:task {self.task.id} source type must be mysql"
        assert self.target.type == "hdfs", f"[datax_plugin]:task {self.task.id} target type must be hdfs"
        tables=DatabaseTableHandler.split(self.task,self.settings.get('execute_way'))
            
        # 排除配置不需要的字段
        if not self.task.column_config.columns:
            source_db,source_table=tables[0].split(".")
            columns_source=MysqlUtil.get_table_schema(self.mysql,source_db,source_table)
            columns_source=self._exclude_column(columns_source)
        else:
            columns_source=self._exclude_column(self.task.column_config.columns)

        # 映射配置的字段
        if self.task.column_config.reader_transform_columns:
            columns_source=self._transform_columns(columns_source,self.task.column_config.reader_transform_columns)
        
        source_not_exist=[]
        target_not_exist=[]
        columns_target=HiveUtil.get_table_schema(self.hive,self.task.target_db,self.task.target_table)
        # 遍历columns_source,如果columns_source中的列在columns_target中不存在，则将其添加到target_not_exist中
        for column in columns_source:
            if column['name'] not in [x['name'] for x in columns_target]:
                target_not_exist.append(column)

        for column in columns_target:
            if column['name'] not in [ x['name'] for x in columns_source] and column['name'] !=self.task.column_config.partition_column and column['name'] !=  self.task.column_config.sync_time_column:
                source_not_exist.append(column)
            elif column['name'] == self.task.column_config.sync_time_column and self.task.column_config.is_add_sync_time:
                # 更新字段
                self.source_columns.append({
                    "name": self.task.column_config.sync_time_column,
                    "type": 'datetime', 
                    "value": "now()"
                })
                self.target_columns.append({
                    "name": self.task.column_config.sync_time_column,
                    "type": 'timestamp',
                })
            elif column['name'] == self.task.column_config.partition_column and self.task.column_config.is_partition:
                # 分区字段
                pass
            else:
                # 共有字段
                format_column=DataxTypes.format_type(column['type'])
                datax_type=DataxTypes.hive_to_datax(column['type'])
                mysql_type=DataxTypes.datax_to_mysql(datax_type)
                self.target_columns.append({
                    "name": column['name'],
                    "type": format_column,
                })
                self.source_columns.append({
                    "name": column['name'],
                    "type": mysql_type, 
                })
        source_columns_format=DataxTypes.format_reader_schema(self.source_columns)
        # 如果source_not_exist和target_not_exist 不为空，则发送警告日志
        if source_not_exist:
            logger.warning(f"[datax_plugin]:task {self.task.id} source table {self.task.source_table} not exist columns {source_not_exist}")
            msg=f"[datax_plugin]:task {self.task.id} source table {self.task.source_table} not exist columns \n\n {source_not_exist}"
            self.alert.send_custom_message(msg)
        if target_not_exist:
            logger.warning(f"[datax_plugin]:task {self.task.id} target table {self.task.target_table} not exist columns {target_not_exist}")
            msg=f"[datax_plugin]:task {self.task.id} target table {self.task.target_table} not exist columns \n\n {target_not_exist}"
            self.alert.send_custom_message(msg)
        if not self.target_columns:
            assert False, f"[datax_plugin]:task {self.task.id} source table {self.task.source_table} and target table {self.task.target_table} columns not match"
        if self.settings.get('execute_way') == 'all':
            where = f"{self.task.update_column}<'{self.settings.get('today').strftime('%Y-%m-%d 00:00:00')}'"
        elif self.settings.get('execute_way') == 'update':
            where = f"{self.task.update_column} between '{self.settings.get('yesterday').strftime('%Y-%m-%d 00:00:00')}' and '{self.settings.get('today').strftime('%Y-%m-%d 00:00:00')}'"
        elif self.settings.get('execute_way') == 'other':
            where = f"{self.task.update_column} between '{self.settings.get('start_time')}' and '{self.settings.get('end_time')}'"
        else:
            assert False, f"[datax_plugin]:不支持的执行方式[{self.settings.get('execute_way')}]"
        if self.task.update_column and self.task.column_config.partition_column:
            querySql= [
                f"select {source_columns_format} from {i} where {where}" for i in tables]
        else:
            querySql = [f"select {source_columns_format}  from {i}" for i in tables]

        reader_config= {
            "name": "mysqlreader",
            "parameter": {
                "username": self.source.connection.username,
                "password": self.source.connection.password,
                "connection": [
                    {
                        "querySql":querySql,
                        "jdbcUrl": [
                            f"jdbc:mysql://{self.source.connection.host}:{self.source.connection.port}?useSSL=false&useUnicode=true&characterEncoding=utf8"
                        ]
                    }
                ]
            }
        }
        return reader_config
    def mysql2mysql(self:DataXPlugin):
        """mysql2mysql 生成reader逻辑"""
        assert self.source.type == "mysql", f"[datax_plugin]:task {self.task.id} source type must be mysql"
        assert self.target.type == "mysql", f"[datax_plugin]:task {self.task.id} target type must be mysql"
        tables=DatabaseTableHandler.split(self.task)
        # 排除配置不需要的字段
        if not self.task.column_config.columns:
            columns_source=MysqlUtil.get_table_schema(self.mysql,self.task.source_db,tables[0])
            columns_source=self._exclude_column(columns_source)
        else:
            columns_source=self._exclude_column(self.task.column_config.columns)

        # 映射配置的字段
        if self.task.column_config.reader_transform_columns:
            columns_source=self._transform_columns(columns_source,self.task.column_config.reader_transform_columns)
        
        source_not_exist=[]
        target_not_exist=[]
        # TODO 只支持MYSQL多到一
        columns_target=MysqlUtil.get_table_schema(self.mysql,self.task.target_db,self.task.target_table)
        # 遍历columns_source,如果columns_source中的列在columns_target中不存在，则将其添加到target_not_exist中
        for column in columns_source:
            if column['name'] not in [x['name'] for x in columns_target]:
                target_not_exist.append(column)

        for column in columns_target:
            if column['name'] not in [ x['name'] for x in columns_source] and column['name'] !=self.task.column_config.partition_column and column['name'] !=  self.task.column_config.sync_time_column:
                source_not_exist.append(column)
            elif column['name'] == self.task.column_config.sync_time_column:
                # 更新字段
                self.source_columns.append({
                    "name": self.task.column_config.sync_time_column,
                    "type": 'datetime', 
                    "value": "now()"
                })
                self.target_columns.append({
                    "name": self.task.column_config.sync_time_column,
                    "type": 'timestamp',
                })
            elif column['name'] == self.task.column_config.partition_column:
                # 分区字段
                pass
            else:
                # 共有字段
                format_column=DataxTypes.format_type(column['type'])
                datax_type=DataxTypes.hive_to_datax(column['type'])
                mysql_type=DataxTypes.datax_to_mysql(datax_type)
                self.target_columns.append({
                    "name": column['name'],
                    "type": format_column,
                })
                self.source_columns.append({
                    "name": column['name'],
                    "type": mysql_type, 
                })
        source_columns_format=DataxTypes.format_reader_schema(self.source_columns)
        # 如果source_not_exist和target_not_exist 不为空，则发送警告日志
        if source_not_exist:
            logger.warning(f"[datax_plugin]:task {self.task.id} source table {self.task.source_table} not exist columns {source_not_exist}")
            msg=f"[datax_plugin]:task {self.task.id} source table {self.task.source_table} not exist columns {source_not_exist}"
            self.alert.send_custom_message(msg)
        if target_not_exist:
            logger.warning(f"[datax_plugin]:task {self.task.id} target table {self.task.target_table} not exist columns {target_not_exist}")
            msg=f"[datax_plugin]:task {self.task.id} target table {self.task.target_table} not exist columns {target_not_exist}"
            self.alert.send_custom_message(msg)
        if not self.target_columns:
            assert False, f"[datax_plugin]:task {self.task.id} source table {self.task.source_table} and target table {self.task.target_table} columns not match"
        if self.settings.get('execute_way') == 'all':
            where = f"{self.task.update_column}<'{self.settings.get('today').strftime('%Y-%m-%d 00:00:00')}'"
        elif self.settings.get('execute_way') == 'update':
            where = f"{self.task.update_column} between '{self.settings.get('yesterday').strftime('%Y-%m-%d 00:00:00')}' and '{self.settings.get('today').strftime('%Y-%m-%d 00:00:00')}'"
        elif self.settings.get('execute_way') == 'other':
            where = f"{self.task.update_column} between '{self.settings.get('start_time')}' and '{self.settings.get('end_time')}'"
        else:
            assert False, f"[datax_plugin]:不支持的执行方式[{self.settings.get('execute_way')}]"
        if self.task.update_column and self.task.column_config.partition_column:
            querySql= [
                f"select {source_columns_format} from {i} where {where}" for i in tables]
        else:
            querySql = [f"select {source_columns_format}  from {i}" for i in tables]

        reader_config= {
            "name": "mysqlreader",
            "parameter": {
                "username": self.source.connection.username,
                "password": self.source.connection.password,
                "connection": [
                    {
                        "querySql":querySql,
                        "jdbcUrl": [
                            f"jdbc:mysql://{self.source.connection.host}:{self.source.connection.port}/{self.task.source_db}?useSSL=false&useUnicode=true&characterEncoding=utf8"
                        ]
                    }
                ]
            }
        }
        return reader_config
class Writer:
    @staticmethod
    def other2hive(self:DataXPlugin):
        """other2hive"""
        assert self.target.type == "hdfs", f"[datax_plugin]:task {self.task.id} target type must be hdfs"
        # TODO 代码删除hdfs数据有问题，删除成功但文件还在
        # if self.task.column_config.partition_column:
        #     # 创建分区
        #     HiveUtil.add_partition(self.hive,self.task.target_db,self.task.target_table,self.task.column_config.partition_column,self.settings.get('partition_date'))
        #     if self.task.is_delete:
        #         # 删除分区数据
        #         HdfsUtil.drop_hive_table(self.hdfs,self.task.target_db,self.task.target_table,self.task.column_config.partition_column,self.settings.get('partition_date'))
        # else:
        #     if self.task.is_delete:
        #         # 删除表数据
        #         HdfsUtil.drop_hive_table(self.hdfs,self.task.target_db,self.task.target_table)
        if self.task.is_delete:
            self.mode='truncate'
        else:
            self.mode='append'
        hadoopConfig=self.target.connection.params.get('hadoopConfig')
        writer_config={
            "name": "hdfswriter",
            "parameter": {
                "writeMode": self.mode,
                "column": self.target_columns,
                "defaultFS": self.target.connection.params.get("defaultFS"),
                "hadoopConfig":hadoopConfig,
                "path": "/user/hive/warehouse/%s.db/%s",
                "fileType": "orc",
                "fileName":f"{self.task.target_table}",
                "fieldDelimiter": "\u0001",
            }
        }
        if self.task.column_config.partition_column:
            writer_config["parameter"]["path"] = f"/user/hive/warehouse/{self.task.target_db}.db/{self.task.target_table}/{self.task.column_config.partition_column}={self.settings.get('partition_date')}"
        else:
            writer_config["parameter"]["path"] = f"/user/hive/warehouse/{self.task.target_db}.db/{self.task.target_table}"
        return writer_config