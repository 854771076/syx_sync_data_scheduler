import json
import subprocess
from pathlib import Path
from typing import Union, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date
from ..metadata.utils import (
    DatabaseTableHandler,
    HiveUtil,
    HdfsUtil,
    DataxUtil,
    DataxTypes,
    MysqlUtil,
)
from loguru import logger
from executors.alerts import AlertFactory


class settings:
    execute_way = None
    start_time = None
    end_time = None
    partition_date = None
    max_worker = None


class DataXPluginManager:
    def __init__(self, tasks, settings: settings = {}):
        from ...models import Task, ConfigItem, Log

        logger.debug(
            f"[datax_plugin]:init DataXPluginManager with {len(tasks)} tasks,settings={settings}"
        )

        self.settings = settings
        self.config = dict(ConfigItem.objects.all().values_list("key", "value"))
        assert self.settings.get(
            "execute_way"
        ), "[datax_plugin]:execute_way is required"
        assert self.settings.get("execute_way") in [
            "all",
            "update",
            "other",
            "action",
            "retry",
        ], f"[datax_plugin]:execute_way must be in ['all','update','other','action']"
        if self.settings.get("execute_way") == "other":
            assert self.settings.get("start_time"), "[datax_plugin]:start_time is required"
            assert self.settings.get("end_time"), "[datax_plugin]:end_time is required"
        self.today = date.today()
        self.yesterday = date.today() - timedelta(days=1)
        self.partition_date = self.settings.get(
            "partition_date"
        ) or self.yesterday.strftime("%Y%m%d")
        self.settings["today"] = self.today
        self.settings["yesterday"] = self.yesterday
        self.settings["partition_date"] = self.partition_date
        self.plugins = []
        self.tasks = tasks
        self.max_worker = int(
            settings.get("max_worker", self.config.get("DATAX_MAX_WORKER", 10))
        )
        self.pool = ThreadPoolExecutor(max_workers=self.max_worker)
        self.register()
        logger.debug(
            f"[datax_plugin]:init DataXPluginManager with {len(self.tasks)} tasks,max_worker={self.max_worker},partition_date={self.partition_date},execute_way={self.settings.get('execute_way')}"
        )

    def register(self):
        """注册DataX插件"""
        for task in self.tasks:
            self.plugins.append(DataXPlugin(task, self.config, self.settings))
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
    @staticmethod
    def execute_retry( logs):
        """执行DataX任务"""
        from ...models import ConfigItem
        config = dict(ConfigItem.objects.all().values_list("key", "value"))
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures=[]
            for log in logs:
                settings={
                    "execute_way": log.execute_way,
                    **log.task.project.config,
                    'partition_date':log.partition_date,
                    'start_time':log.start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'end_time':log.end_time.strftime('%Y-%m-%d %H:%M:%S'),
                    
                }
                task = DataXPlugin(log.task, config, settings)
                futures.append(pool.submit(task.execute_retry, log))
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
    def __init__(self, task, config, settings):
        logger.debug(
            f"[datax_plugin]:init DataXPlugin with task {task.id},config={config},settings={settings}"
        )
        self.task = task
        self.output_dir = Path(__file__).parent / "output"
        self.logs_dir = Path(__file__).parent / "logs"
        self.logs_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        self.config = config
        self.settings = settings
        self.jvm_options = self.task.config.get(
            "jvm_options",
            settings.get("jvm_options", self.config.get("DATAX_JVM_OPTIONS", "")),
        )
        self.source = self.task.data_source
        self.target = self.task.data_target
        self.hive: HiveUtil = None
        self.hdfs: HdfsUtil = None
        self.mysql: MysqlUtil = None
        self.alert = AlertFactory(self.task.project.notification)
        self.source_columns = []
        self.target_columns = []
        self.mode = "append"
        self._validate_config()
        try:
            self._init_clients()
        except Exception as e:
            logger.exception(e)

    def _validate_config(self):
        """验证配置"""

        assert self.settings.get(
            "execute_way"
        ), f"[datax_plugin]:execute_way is required"
        # if self.source.type == "mysql" and self.target.type == "hdfs":
        #     pass
        # elif self.source.type == "mysql" and self.target.type == "mysql":
        #     pass
        # elif self.source.type == "hdfs" and self.target.type == "hdfs":
        # else:
        #     assert False, f"[datax_plugin]:unsupported data source or data target"

    def _init_clients(self):
        """初始化工具类"""

        if self.source.type == "hdfs" or self.target.type == "hdfs":
            self.hive = HiveUtil.get_hive_client_by_config(self.config, self.target)
            self.hdfs = HdfsUtil.get_hdfs_client_by_config(self.config, self.target)
        if self.source.type == "mysql" or self.target.type == "mysql":
            self.mysql = MysqlUtil.get_mysql_client_by_config(self.source)

    def _get_reader_config(self):
        """生成reader配置"""
        if self.source.type in ["mysql","starrocks"] :
            return Reader.mysql2other(self)
        else:
            assert False, f"[datax_plugin]:不支持的数据源类型[{self.source.type}]"

    def _exclude_column(self, columns: list):
        """排除列"""
        exclude_column = list(
            filter(lambda x: x["name"] not in self.task.exclude_columns, columns)
        )
        return exclude_column

    def _get_writer_config(self):
        """生成writer配置"""
        if self.target.type == "hdfs":
            return Writer.other2hive(self)
        elif self.target.type == "mysql":
            return Writer.other2mysql(self)
        elif self.target.type == "starrocks":
            return Writer.other2starrocks(self)
        else:
            assert False, f"[datax_plugin]:不支持的目标类型[{self.target.type}]"

    def generate_config(self):
        """生成DataX配置文件"""

        config = {
            "core": json.loads(self.config.get("DATAX_CORE_SETTINGS")),
            "job": {
                "setting": json.loads(self.config.get("DATAX_SETTINGS")),
                "content": [
                    {
                        "reader": self._get_reader_config(),
                        "writer": self._get_writer_config(),
                    }
                ],
            },
        }

        self.task.datax_json = config
        self.task.save()
        # 读取任务中的json
        config = self.task.datax_json
        config_path = self.output_dir / f"{self.task.id}.json"
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=4)
        return True

    def execute_retry(self, log):
        """执行DataX任务"""
        config = log.datax_json
        config_path = self.output_dir / f"{self.task.id}_retry.json"
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=4)
        self.jvm_options="-Xms3g -Xmx10g"
        result = DataxUtil.execute_datax(self, retry=True)

    def is_completed(self):
        from ...models import Task, ConfigItem, Log

        return Log.objects.filter(
            task=self.task,
            execute_way=self.settings.get("execute_way"),
            partition_date=self.settings.get("partition_date"),
            complit_state=Log.complitStateChoices.success,
        ).exists()

    def execute_action(self):
        """执行DataX任务"""
        self.pre_execute()
        logger.info(
            f"[datax_plugin]:task {self.task.id} 开始执行--{self.settings.get('execute_way')}--{self.settings.get('partition_date')}"
        )
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
        if self.task.is_partition and self.target.type in ["hive","hdfs"] :
            HiveUtil.add_partition(
                self.hive,
                self.task.target_db,
                self.task.target_table,
                self.task.partition_column,
                self.settings.get("partition_date"),
            )
        return True

    def execute(self):
        """执行DataX任务"""
        if self.is_completed():
            logger.info(f"[datax_plugin]:task {self.task.id} 当日已执行，跳过")
            return False
        
        config_path = self.output_dir / f"{self.task.id}.json"
        logger.info(
            f"[datax_plugin]:task {self.task.id} 开始执行--{self.settings.get('execute_way')}--{self.settings.get('partition_date')}"
        )
        self.generate_config()
        result = DataxUtil.execute_datax(self)

    # 通过reader_transform_columns配置转换字段名字
    def _transform_columns(
        self, columns: List[Dict[str, Any]], mapping: Dict[str, str]
    ):
        """转换字段名字"""
        new_columns = []
        for column in columns:
            if column["name"] in mapping:
                column["name"] = mapping[column["name"]]
            new_columns.append(column)
        return new_columns


class Reader:
    @staticmethod
    def get_columns(self: DataXPlugin):
        from ...models import MetadataTable, _sync_single_metadata

        tables = DatabaseTableHandler.split(self.task, self.settings.get("execute_way"))
        # 排除配置不需要的字段
        if not self.task.columns:
            Metadata = MetadataTable.objects.filter(
                name=self.task.source_table, db_name=self.task.source_db
            ).first()
            if not Metadata:
                columns_source = _sync_single_metadata(
                    self.task.data_source,
                    self.task.source_db,
                    self.task.source_table,
                    self.config,
                    tables,
                )
            else:
                columns_source = Metadata.meta_data
            columns_source = self._exclude_column(columns_source)
        else:
            columns_source = self._exclude_column(self.task.columns)

        # 映射配置的字段
        if self.task.reader_transform_columns:
            columns_source = self._transform_columns(
                columns_source, self.task.reader_transform_columns
            )

        source_not_exist = []
        target_not_exist = []
        Metadata_target = MetadataTable.objects.filter(
            name=self.task.target_table, db_name=self.task.target_db
        ).first()
        if not Metadata_target:
            columns_target = _sync_single_metadata(
                self.task.data_target,
                self.task.target_db,
                self.task.target_table,
                self.config,
            )
        else:
            columns_target = Metadata_target.meta_data
        # 排除配置不需要的字段
        columns_target = self._exclude_column(columns_target)
        # 转小写
        columns_target = [
            {k.lower(): v.lower() for k, v in i.items()} for i in columns_target
        ]
        columns_source = [
            {k.lower(): v.lower() for k, v in i.items()} for i in columns_source
        ]
        # 遍历columns_source,如果columns_source中的列在columns_target中不存在，则将其添加到target_not_exist中
        for column in columns_source:
            if column["name"] not in [x["name"] for x in columns_target]:
                target_not_exist.append(column)

        for column in columns_target:
            if (
                column["name"] not in [x["name"] for x in columns_source]
                and column["name"] != self.task.partition_column
                and column["name"] != self.task.sync_time_column
            ):
                source_not_exist.append(column)
            elif (
                column["name"] == self.task.sync_time_column
                and self.task.is_add_sync_time
                and self.task.sync_time_column
            ):
                # 更新字段
                self.source_columns.append(
                    {
                        "name": self.task.sync_time_column,
                        "type": "datetime",
                        "value": "now()",
                    }
                )
                self.target_columns.append(
                    {
                        "name": self.task.sync_time_column,
                        "type": "timestamp",
                    }
                )
            elif (
                column["name"] == self.task.partition_column
                and self.task.is_partition
                and self.task.partition_column
            ):
                # 分区字段
                pass
            else:
                # 共有字段
                format_column = DataxTypes.format_type(column["type"])
                convert_type = DataxTypes.convert_type(
                    self.source.type, self.target.type, column["type"]
                )
                self.target_columns.append(
                    {
                        "name": column["name"],
                        "type": format_column,
                    }
                )
                self.source_columns.append(
                    {
                        "name": column["name"],
                        "type": convert_type,
                    }
                )
        source_columns_format = DataxTypes.format_reader_schema(self.source_columns)
        # 如果source_not_exist和target_not_exist 不为空，则发送警告日志
        if source_not_exist or target_not_exist:
            logger.warning(
                f"[字段监控通知]:task {self.task.id} source table {self.task.source_table} not exist columns {source_not_exist}"
            )
            # 优化通知，汉语，markdown排版
            source_not_exist_format = DataxTypes.format_reader_schema(source_not_exist)
            target_not_exist_format = DataxTypes.format_reader_schema(target_not_exist)
            msg = f"### 字段监控通知 \n"
            msg += f"#### 任务名称: {self.task.name} \n"
            msg += f"#### 任务ID: {self.task.id} \n"
            msg += f"#### 任务描述: {self.task.description} \n"
            msg += (
                f"#### 原始表： \n > {self.task.source_db}.{self.task.source_table} \n"
            )
            msg += (
                f"#### 目标表： \n > {self.task.target_db}.{self.task.target_table} \n"
            )
            msg += f"#### 通知内容: 原始表和目标表字段不一致 \n"
            msg += f"#### 原始表没有字段: \n > {source_not_exist_format} \n"
            msg += f"#### 目标表没有字段: \n > {target_not_exist_format} \n"
            self.alert.send_custom_message(msg)
        return source_columns_format, tables

    @staticmethod
    def mysql2other(self: DataXPlugin):
        """mysql2hive 生成reader逻辑"""
        assert (
            self.source.type in ["mysql","starrocks"]
        ), f"[datax_plugin]:task {self.task.id} source type must be mysql"
        source_columns_format, tables = Reader.get_columns(self)
        if not self.target_columns:
            assert (
                False
            ), f"[datax_plugin]:task {self.task.id} source table {self.task.source_table} and target table {self.task.target_table} columns not match"
        if self.settings.get("execute_way") == "all":
            where = f"{self.task.update_column}<'{self.settings.get('today').strftime('%Y-%m-%d 00:00:00')}'"
        elif self.settings.get("execute_way") == "update":
            where = f"{self.task.update_column} between '{self.settings.get('yesterday').strftime('%Y-%m-%d 00:00:00')}' and '{self.settings.get('today').strftime('%Y-%m-%d 00:00:00')}'"
        elif self.settings.get("execute_way") == "other":
            where = f"{self.task.update_column} between '{self.settings.get('start_time')}' and '{self.settings.get('end_time')}'"
        else:
            assert (
                False
            ), f"[datax_plugin]:不支持的执行方式[{self.settings.get('execute_way')}]"
        if self.task.update_column and not self.task.split_config.tb_time_suffix:
            querySql = [
                f"select {source_columns_format} from {i} where {where}" for i in tables
            ]
        elif self.task.split_config.tb_time_suffix:
            querySql = [f"select {source_columns_format}  from {i}" for i in tables]
        else:
            querySql = [f"select {source_columns_format}  from {i}" for i in tables]
        

        reader_config = {
            "name": "mysqlreader",
            "parameter": {
                "username": self.source.connection.username,
                "password": self.source.connection.password,
                "connection": [
                    {
                        "querySql": querySql,
                        "jdbcUrl": [
                            f"jdbc:mysql://{self.source.connection.host}:{self.source.connection.port}?useSSL=false&useUnicode=true&characterEncoding=utf8"
                        ],
                    }
                ],
            },
        }
        return reader_config
    @staticmethod
    def hive2other(self: DataXPlugin):
        """hive2other 生成reader逻辑"""
        assert (
            self.source.type in ["hive"]
        ), f"[datax_plugin]:task {self.task.id} source type must be hive"
        source_columns_format, tables = Reader.get_columns(self)
        if not self.target_columns:
            assert (
                False
            ), f"[datax_plugin]:task {self.task.id} source table {self.task.source_table} and target table {self.task.target_table} columns not match"
        if self.settings.get("execute_way") == "all":
            where = f"{self.task.update_column}<'{self.settings.get('today').strftime('%Y-%m-%d 00:00:00')}'"
        elif self.settings.get("execute_way") == "update":
            where = f"{self.task.update_column} between '{self.settings.get('yesterday').strftime('%Y-%m-%d 00:00:00')}' and '{self.settings.get('today').strftime('%Y-%m-%d 00:00:00')}'"
        elif self.settings.get("execute_way") == "other":
            where = f"{self.task.update_column} between '{self.settings.get('start_time')}' and '{self.settings.get('end_time')}'"
        else:
            assert (
                False
            ), f"[datax_plugin]:不支持的执行方式[{self.settings.get('execute_way')}]"
        if self.task.update_column and not self.task.split_config.tb_time_suffix:
            querySql = [
                f"select {source_columns_format} from {i} where {where}" for i in tables
            ]
        elif self.task.split_config.tb_time_suffix:
            querySql = [f"select {source_columns_format}  from {i}" for i in tables]
        else:
            querySql = [f"select {source_columns_format}  from {i}" for i in tables]

        reader_config = {
            "name": "mysqlreader",
            "parameter": {
                "username": self.source.connection.username,
                "password": self.source.connection.password,
                "connection": [
                    {
                        "querySql": querySql,
                        "jdbcUrl": [
                            f"jdbc:mysql://{self.source.connection.host}:{self.source.connection.port}?useSSL=false&useUnicode=true&characterEncoding=utf8"
                        ],
                    }
                ],
            },
        }
        return reader_config
    # hive2other
class Writer:
    @staticmethod
    def other2hive(self: DataXPlugin):
        """other2hive"""
        assert (
            self.target.type == "hdfs"
        ), f"[datax_plugin]:task {self.task.id} target type must be hdfs"
        # 清空规则
        if not self.task.is_delete:
            self.mode = "append"
        else:
            self.mode = "truncate"
        hadoopConfig = self.target.connection.params.get("hadoopConfig")
        writer_config = {
            "name": "hdfswriter",
            "parameter": {
                "writeMode": self.mode,
                "column": self.target_columns,
                "defaultFS": self.target.connection.params.get("defaultFS"),
                "hadoopConfig": hadoopConfig,
                "path": "/user/hive/warehouse/%s.db/%s",
                "fileType": "orc",
                "fileName": f"{self.task.target_table}",
                "fieldDelimiter": "\u0001",
            },
        }
        if self.task.partition_column and self.task.is_partition:
            writer_config["parameter"][
                "path"
            ] = f"/user/hive/warehouse/{self.task.target_db}.db/{self.task.target_table}/{self.task.partition_column}={self.settings.get('partition_date')}"
        else:
            writer_config["parameter"][
                "path"
            ] = f"/user/hive/warehouse/{self.task.target_db}.db/{self.task.target_table}"
        return writer_config

    @staticmethod
    def other2mysql(self: DataXPlugin):
        """other2mysql"""
        assert (
            self.target.type == "mysql"
        ), f"[datax_plugin]:task {self.task.id} target type must be mysql"
        self.mode = "replace"
        pre_execute_sql = []
        if self.settings.get("execute_way") == "all":
            where = f"{self.task.update_column}<'{self.settings.get('today').strftime('%Y-%m-%d 00:00:00')}'"
        elif self.settings.get("execute_way") == "update":
            where = f"{self.task.update_column} between '{self.settings.get('yesterday').strftime('%Y-%m-%d 00:00:00')}' and '{self.settings.get('today').strftime('%Y-%m-%d 00:00:00')}'"
        elif self.settings.get("execute_way") == "other":
            where = f"{self.task.update_column} between '{self.settings.get('start_time')}' and '{self.settings.get('end_time')}'"
        else:
            assert (
                False
            ), f"[datax_plugin]:不支持的执行方式[{self.settings.get('execute_way')}]"
        # 添加预执行sql
        if self.task.update_column:
            if self.task.is_delete:
                pre_execute_sql.append(
                    f"delete from {self.task.target_table} where {where}"
                )
        else:
            if self.task.is_delete:
                pre_execute_sql.append(f"delete from {self.task.target_table}")
        # 检查预执行sql
        if self.task.config.get('preSql'):
            pre_execute_sql.extend(self.task.config.get('preSql'))
        # 检查session变量
        session_variable = []
        if self.task.config.get('session'):
            session_variable.extend(self.task.config.get('session'))
        writer_config = {
            "name": "mysqlwriter",
            "parameter": {
                "username": self.target.connection.username,
                "password": self.target.connection.password,
                "column": [i["name"] for i in self.target_columns],
                "writeMode": self.mode,
                "preSql": pre_execute_sql,
                "session": session_variable,
                "connection": [
                    {
                        "jdbcUrl": f"jdbc:mysql://{self.target.connection.host}:{self.target.connection.port}/{self.task.target_db}?useSSL=false&useUnicode=true&characterEncoding=utf8",
                        "table": [self.task.target_table],
                    }
                ],
            },
        }
        return writer_config
    @staticmethod
    def other2starrocks(self: DataXPlugin):
        """other2starrocks"""
        assert (
            self.target.type == "starrocks"
        ), f"[datax_plugin]:task {self.task.id} target type must be starrocks"
        self.mode = "replace"
        pre_execute_sql = []
        if self.settings.get("execute_way") == "all":
            where = f"{self.task.update_column}<'{self.settings.get('today').strftime('%Y-%m-%d 00:00:00')}'"
        elif self.settings.get("execute_way") == "update":
            where = f"{self.task.update_column} between '{self.settings.get('yesterday').strftime('%Y-%m-%d 00:00:00')}' and '{self.settings.get('today').strftime('%Y-%m-%d 00:00:00')}'"
        elif self.settings.get("execute_way") == "other":
            where = f"{self.task.update_column} between '{self.settings.get('start_time')}' and '{self.settings.get('end_time')}'"
        else:
            assert (
                False
            ), f"[datax_plugin]:不支持的执行方式[{self.settings.get('execute_way')}]"
        # 添加预执行sql
        if self.task.update_column:
            if self.task.is_delete:
                pre_execute_sql.append(
                    f"delete from {self.task.target_table} where {where}"
                )
        else:
            if self.task.is_delete:
                pre_execute_sql.append(f"delete from {self.task.target_table}")
        if self.target.connection.params.get('loadUrl'):
            loadUrl = self.target.connection.params.get('loadUrl')
        else:
            loadUrl = [
                f"{self.target.connection.host}:8030"
            ]
        # 检查预执行sql
        if self.task.config.get('preSql'):
            pre_execute_sql.extend(self.task.config.get('preSql'))
        writer_config = {
            "name": "starrockswriter",
            "parameter": {
                "maxBatchRows": 5000000,
                "maxBatchSize":100000000,
                "flushInterval": 1000000,
                "nullFormat": "",
                "loadProps": {
                "format": "json",
                "strip_outer_array": True
                },
                "loadUrl": loadUrl,
                "username": self.target.connection.username,
                "password": self.target.connection.password,
                "column": [i["name"] for i in self.target_columns],
                "writeMode": self.mode,
                "preSql": pre_execute_sql,
                "connection": [
                    {
                        "jdbcUrl": f"jdbc:mysql://{self.target.connection.host}:{self.target.connection.port}?useSSL=false&useUnicode=true&characterEncoding=utf8",
                        "table": [self.task.target_table],
                        "selectedDatabase":self.task.target_db
                    }
                ],
            },
        }
        return writer_config