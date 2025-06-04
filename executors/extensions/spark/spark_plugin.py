import json
from pathlib import Path
from typing import Union, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date
import subprocess
import os,re
from loguru import logger
import threading
import time
from executors.alerts import AlertFactory
import urllib
class SparkReader:
    # 定义一个方法，选择对应的代码模板
    @staticmethod
    def get_reader(cls):
        if cls.source.type == 'mongo':
            return SparkReader.mongo2other(cls)
        else:
            raise Exception("Unsupported reader type: {}".format(type))
    # # 添加同步时间字段
    @staticmethod
    def cdc_sync_date(cls):
        if cls.task.is_add_sync_time:
            code_template = f'df = df.withColumn("{cls.task.sync_time_column}", F.current_timestamp())\n'
            return code_template
        else:
            return ''
    # 字段映射，如果字段映射存在，则替换字段名称
    @staticmethod  
    def transform_columns(cls):
        code_template=''
        if cls.task.reader_transform_columns:
            for source_column, target_column in cls.task.reader_transform_columns.items():
                source_column, target_column
                code_template = f'if "{source_column}" in df.columns:\n         df = df.withColumnRenamed("{source_column}", "{target_column}")\n'
            return code_template
        else:
            return ''
    # 排除字段，如果字段映射存在，则替换字段名称
    @staticmethod
    def exclude_columns(cls):
        code_template=''
        if cls.task.exclude_columns:
            for column in cls.task.exclude_columns:
                code_template = f'if "{column}" in df.columns:\n        df = df.drop("{column}")\n'
            return code_template
        else:
            return ''
    # 自定义字段，如果存在则只查询自定义字段
    @staticmethod
    def custom_columns(cls):
        code_template=''
        if cls.task.columns:
            code_template = f'df = df.select({", ".join(cls.task.columns)})\n'
            return code_template
        else:
            return ''
    def mongo2other(cls):
        # 处理无认证的情况
        username = urllib.parse.quote_plus(cls.source.connection.username) if cls.source.connection.username else ''
        password = urllib.parse.quote_plus(cls.source.connection.password) if cls.source.connection.password else ''
        
        # 带认证的URI格式
        auth_part = f"{username}:{password}@" if username and password else ""
        if not cls.source.connection.params.get('uri'):
            mongo_uri = f"mongodb://{auth_part}{cls.source.connection.host}:{cls.source.connection.port}/admin"
        else:
            mongo_uri = cls.source.connection.params.get('uri')

        # 处理日期格式
        today_str = f"datetime.strptime('{cls.settings.get('today')}', '%Y-%m-%d')"
        yesterday_str = f"datetime.strptime('{cls.settings.get('yesterday')}', '%Y-%m-%d')"
        # 读取数据逻辑
        if cls.settings.get("execute_way") == 'update' and cls.task.update_column:
            query= f'{{"{cls.task.update_column}": {{ "$gte": {yesterday_str}, "$lt": {today_str} }}}}'
        elif cls.settings.get("execute_way") == 'all' and cls.task.update_column:
            query= f'{{"{cls.task.update_column}": {{ "$lt": {today_str} }}}}'
        else:
            query= {}
        ### clean
        custom_columns=SparkReader.custom_columns(cls)
        cdc_sync_date=SparkReader.cdc_sync_date(cls)
        transform_columns=SparkReader.transform_columns(cls)
        exclude_columns=SparkReader.exclude_columns(cls)
        code_template = f"""
def reader(spark):
    query = {query}

    df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \\
        .option("uri", "{mongo_uri}") \\
        .option("database", "{cls.task.source_db}") \\
        .option("collection", "{cls.task.source_table}") \\
        .option("query", query) \\
        .load()
    # 处理_id
    if "_id" in df.columns:
        df = df.withColumn("_id", df["_id"].cast("string"))
        df = df.withColumn("_id", F.regexp_replace("_id", r"\[|\]", ""))
    # 处理嵌套字段
    complex_fields = [field.name for field in df.schema.fields 
                    if isinstance(field.dataType, (ArrayType, StructType))]
    for field in complex_fields:
        df = df.withColumn(field, to_json(col(field)))
    {custom_columns}
    {cdc_sync_date}
    {transform_columns}
    {exclude_columns}
    return df
"""
        return code_template

class SparkWriter:
    # 定义一个方法，选择对应的代码模板
    @staticmethod
    def get_writer(cls):
        if cls.target.type in ('hive','hdfs') :
            return SparkWriter.other2hive(cls)
        else:
            raise Exception("Unsupported writer type: {}".format(type))

    @staticmethod
    def other2hive(cls):
        if cls.task.is_partition:
            partition_sql=f'df = df.withColumn("partition_date", F.lit("{cls.settings.get("partition_date")}"))\n'
            if cls.settings.get("execute_way")=='all':
                insert_sql=f'df.write.mode("overwrite").partitionBy("partition_date").format("orc").saveAsTable("{cls.task.target_db}.{cls.task.target_table}")'
            elif cls.settings.get("execute_way")=='update':
                if cls.task.is_delete:
                    insert_sql=f'df.createOrReplaceTempView("temp_table_{cls.task.target_db}.{cls.task.target_table}")\n    spark.sql(\'INSERT OVERWRITE  INTO TABLE {cls.task.target_db}.{cls.task.target_table} PARTITION (partition_date = "{cls.settings.get("partition_date")}") SELECT * FROM temp_table_{cls.task.target_db}.{cls.task.target_table}\')'
                insert_sql=f'df.createOrReplaceTempView("temp_table_{cls.task.target_db}.{cls.task.target_table}")\n    spark.sql(\'INSERT INTO TABLE {cls.task.target_db}.{cls.task.target_table} PARTITION (partition_date = "{cls.settings.get("partition_date")}") SELECT * FROM temp_table_{cls.task.target_db}.{cls.task.target_table}\')'
            elif cls.settings.get("execute_way")=='other':
                if cls.task.is_delete:
                    insert_sql=f'df.createOrReplaceTempView("temp_table_{cls.task.target_db}.{cls.task.target_table}")\n    spark.sql(\'INSERT OVERWRITE  INTO TABLE {cls.task.target_db}.{cls.task.target_table} PARTITION (partition_date = "{cls.settings.get("partition_date")}") SELECT * FROM temp_table_{cls.task.target_db}.{cls.task.target_table}\')'
                insert_sql=f'df.createOrReplaceTempView("temp_table_{cls.task.target_db}.{cls.task.target_table}")\n    spark.sql(\'INSERT INTO TABLE {cls.task.target_db}.{cls.task.target_table} PARTITION (partition_date = "{cls.settings.get("partition_date")}") SELECT * FROM temp_table_{cls.task.target_db}.{cls.task.target_table}\')'
            else:
                raise Exception("Unsupported execute_way: {}".format(cls.settings.get("execute_way")))
        else:
            partition_sql=''
            insert_sql=f'df.write.mode("overwrite").format("orc").saveAsTable("{cls.task.target_db}.{cls.task.target_table}")'
        code_template = f"""
def writer(spark,df):
    logger.info("开始处理 {cls.task.target_db}.{cls.task.target_table}")
    # 保存到Hive表中
    read_num=df.count()
    if read_num==0:
        logger.info("读取数据为空，跳过写入")
        return read_num
    {partition_sql}
    {insert_sql}    
    logger.info("保存 {cls.task.target_db}.{cls.task.target_table} 成功")
    return read_num
"""
        return code_template

class settings:
    execute_way = None
    start_time = None
    end_time = None
    partition_date = None
    max_worker = None


class SparkPluginManager:
    def __init__(self, tasks, settings: settings = {}):
        from ...models import Task, ConfigItem, Log
        logger.debug(
            f"[spark_plugin]:init SparkPluginManager with {len(tasks)} tasks,settings={settings}"
        )

        self.settings = settings
        self.config = dict(ConfigItem.objects.all().values_list("key", "value"))
        assert self.settings.get(
            "execute_way"
        ), "[spark_plugin]:execute_way is required"
        assert self.settings.get("execute_way") in [
            "all",
            "update",
            "other",
            "action",
            "retry",
        ], f"[spark_plugin]:execute_way must be in ['all','update','other','action']"
        if self.settings.get("execute_way") == "other":
            assert self.settings.get("start_time"), "[spark_plugin]:start_time is required"
            assert self.settings.get("end_time"), "[spark_plugin]:end_time is required"
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
            settings.get("max_worker", self.config.get("Spark_MAX_WORKER", 5))
        )
        self.pool = ThreadPoolExecutor(max_workers=self.max_worker)
        self.register()
        logger.debug(
            f"[spark_plugin]:init SparkPluginManager with {len(self.tasks)} tasks,max_worker={self.max_worker},partition_date={self.partition_date},execute_way={self.settings.get('execute_way')}"
        )

    def register(self):
        """注册Spark插件"""
        for task in self.tasks:
            self.plugins.append(SparkPlugin(task, self.config, self.settings))
            logger.debug(f"[spark_plugin]:register task {task.id}")

    def execute_tasks(self):
        """执行Spark任务"""
        futures = [self.pool.submit(task.execute) for task in self.plugins]
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                logger.exception(e)
                continue

    def execute_action(self):
        """执行Spark任务"""
        futures = [self.pool.submit(task.execute_action) for task in self.plugins]
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                logger.exception(e)
                continue
    @staticmethod
    def execute_retry( logs):
        """执行Spark任务"""
        from ...models import ConfigItem
        config = dict(ConfigItem.objects.all().values_list("key", "value"))
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures=[]
            for log in logs:
                settings={
                    "execute_way": log.execute_way,
                    **log.task.project.config,
                    'partition_date':log.partition_date,
                    'start_time':log.start_time,
                    'end_time':log.end_time,
                    
                }
                task = SparkPlugin(log.task, config, settings)
                futures.append(pool.submit(task.execute_retry, log))
            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as e:
                    logger.exception(e)
                    continue
    @staticmethod
    def execute_retry_new( logs):
        """执行Spark任务"""
        from ...models import ConfigItem
        config = dict(ConfigItem.objects.all().values_list("key", "value"))
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures=[]
            for log in logs:
                settings={
                    "execute_way": log.execute_way,
                    **log.task.project.config,
                    'partition_date':log.partition_date,
                    'start_time':log.start_time,
                    'end_time':log.end_time,
                    'today':datetime.strptime(log.partition_date,'%Y%m%d')+timedelta(1) ,
                    'yesterday':datetime.strptime(log.partition_date,'%Y%m%d'),
                    
                }
                task = SparkPlugin(log.task, config, settings)
                futures.append(pool.submit(task.execute_retry_new, log))
            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as e:
                    logger.exception(e)
                    continue
    def generate_config(self):
        """生成Spark配置"""
        for task in self.plugins:
            task.generate_config()


class SparkPlugin:
    def __init__(self, task, config, settings):
        logger.debug(
            f"[spark_plugin]:init SparkPlugin with task {task.id},config={config},settings={settings}"
        )
        self.task = task
        self.output_dir = Path(__file__).parent.parent.parent / "static" / "spark_output"
        self.logs_dir = Path(__file__).parent.parent.parent / "static"/ "spark_logs"
        self.logs_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        self.config = config
        self.settings = settings
        self.spark_conf = self.task.config.get(
            "SPARK_CONF",
            task.project.config.get("SPARK_CONF", self.config.get("SPARK_CONF", "")),
        )
        self.master=self.task.config.get(
            "SPARK_MASTER",
            task.project.config.get("SPARK_MASTER", self.config.get("SPARK_MASTER", "local[*]")), 
        )
        self.jars_dir=Path(__file__).parent / "jars"
        self.source = self.task.data_source
        self.target = self.task.data_target
        self.alert = AlertFactory(self.task.project.notification)
        self._validate_config()
    def _validate_config(self):
        """验证配置"""

        assert self.settings.get(
            "execute_way"
        ), f"[spark_plugin]:execute_way is required"

    def _init_clients(self):
        """初始化工具类"""
        pass
    def generate_import(self):
        imports="from pyspark.sql import SparkSession\n"
        imports+="import sys,os\n"
        imports+="import pyspark.sql.functions as F\n"
        imports+="from loguru import logger\n"
        imports+="from datetime import datetime,timedelta\n"
        imports+="from pyspark.sql import SparkSession\n"
        imports+="import urllib.parse\n"
        imports+="from pyspark.sql.functions import to_json, col\n"
        imports+="from pyspark.sql.types import ArrayType, StructType\n"
        return imports
    def generate_main(self):
        code ='if __name__ == "__main__":\n'
        code +='    spark = SparkSession.builder \\\n'
        code +=f'        .appName("{self.task.name}") \\\n'
        code +=f'       .master("{self.master}") \\\n'
        code +='        .getOrCreate()\n'
        code +='    df=reader(spark)\n'
        code +='    num=writer(spark,df)\n'
        code +='    logger.info(f"读出记录总数:{num}|")\n'
        code +='    spark.stop()\n'
        return code

    def generate_code(self):
        reader=SparkReader.get_reader(self)
        writer=SparkWriter.get_writer(self)
        code = self.generate_import()
        code +=reader
        code +=writer
        code +=self.generate_main()

        return code
    def generate_config(self):
        if not self.task.is_custom_script:
            code = self.generate_code()
            self.task.spark_code = code
            self.task.save()
        else:
            code = (
                self.task.spark_code
                .replace("${source_db}", self.task.source_db)
                .replace("${source_table}", self.task.source_table)
                .replace("${target_db}", self.task.target_db)
                .replace("${target_table}", self.task.target_table)
                .replace("${partition_date}", self.settings.get("partition_date",""))
                .replace("${today}", self.settings.get("today").strftime("%Y-%m-%d"),"")
                .replace("${yesterday}", self.settings.get("yesterday").strftime("%Y-%m-%d"),"")
                .replace("${start_time}", self.settings.get("start_time"),"")
                .replace("${end_time}", self.settings.get("end_time"),"")
                .replace("${execute_way}", self.settings.get("execute_way"),"")
            )
        code_path = self.output_dir / f"{self.task.id}.py"
        with open(code_path, "w", encoding="utf-8") as f:
            f.write(code)
        
        return code_path
    
    def execute_retry(self, log):
        """执行Spark任务"""
        config = log.spark_code
        config_path = self.output_dir / f"{self.task.id}_retry.py"
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(config)
        result = self.execute_spark( retry=True)
    def execute_retry_new(self, log):
        """执行Spark任务"""
        config = log.spark_code
        config_path = self.output_dir / f"{self.task.id}_retry.py"
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(config)
        result = self.execute_spark()
    def is_completed(self):
        from ...models import Task, ConfigItem, Log

        return Log.objects.filter(
            task=self.task,
            execute_way=self.settings.get("execute_way"),
            partition_date=self.settings.get("partition_date"),
            complit_state=Log.complitStateChoices.success,
        ).exists()
    def is_depend(self):
        task_log_dependency=self.task.task_log_dependency
        if not task_log_dependency:
            return True
        else:
            status=task_log_dependency.is_executable(**{
                **self.settings,
                "source_db":self.task.source_db,
                "source_table":self.task.source_table,
                "target_db":self.task.target_db,
                "target_table":self.task.target_table,
                'id':self.task.id,

            })
            return status
    def execute_action(self):
        """执行Spark任务"""
        logger.info(
            f"[spark_plugin]:task {self.task.id} 开始执行--{self.settings.get('execute_way')}--{self.settings.get('partition_date')}"
        )
        # 读取任务中的json
        config = self.task.spark_code
        config_path = self.output_dir / f"{self.task.id}.py"
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(config)
        # 执行命令
        result = self.execute_spark()

    def execute(self):
        """执行Spark任务"""
        if self.is_completed():
            logger.info(f"[spark_plugin]:task {self.task.id} 当日已执行，跳过")
            return False
        
        config_path = self.output_dir / f"{self.task.id}.json"
        logger.info(
            f"[spark_plugin]:task {self.task.id} 开始执行--{self.settings.get('execute_way')}--{self.settings.get('partition_date')}"
        )
        result = self.execute_spark()
    def parse_log(self,log_data):
        text=re.sub('\s','',log_data)
        try:
            read_num=int(re.findall('读出记录总数:(\d+)',text)[0])
        except:
            read_num=0
        error_num=0
        try:
            is_None=bool(re.findall('读取数据为空',text))
        except:
            is_None=False
        return {
            'read_num':read_num,
            'error_num':error_num,
            'is_None':is_None, 
        }
    def execute_spark(cls,retry=False):
        """执行Spark任务"""
        from ...models import Task,ConfigItem,Log,DataSource
        """
        执行 Spark 任务。
        """
        try:
            pid = threading.get_ident()
            partition_path=cls.logs_dir/cls.settings.get('partition_date')
            partition_path.mkdir(exist_ok=True)
            if retry:
                config_path = cls.output_dir / f"{cls.task.id}_retry.py"
                log_path = partition_path / f"{cls.task.id}_retry.log"
            else:
                config_path = cls.output_dir / f"{cls.task.id}.py"
                log_path = partition_path / f"{cls.task.id}.log"
            start_time=datetime.now()
            # 判断系统
            if cls.master.find('yarn')!=-1:
                command = f'export HADOOP_USER_NAME={cls.task.project.tenant.name}&&export PYSPARK_PYTHON="/home/anaconda3/bin/python3"&&spark-submit    --master yarn  --queue {cls.task.project.tenant.queue} --jars $(echo {cls.jars_dir}/*.jar | tr " " ",") {cls.spark_conf} {config_path} > {log_path} 2>&1'
            else:
                command = f'export HADOOP_USER_NAME={cls.task.project.tenant.name}&&export PYSPARK_PYTHON="/home/anaconda3/bin/python3"&&spark-submit  --queue {cls.task.project.tenant.queue} --jars $(echo {cls.jars_dir}/*.jar | tr " " ",") {cls.spark_conf} {config_path} > {log_path} 2>&1'
            # 执行中日志
            logger.info(f"执行 Spark 任务 {cls.task.id}，命令：{command}")
            log_obj, created = Log.objects.get_or_create(
                task=cls.task,
                partition_date=cls.settings.get('partition_date'),
                execute_way=cls.settings.get('execute_way'),
                defaults={
                    'executed_state': 'process',
                    'complit_state': 2,
                    'start_time': cls.settings.get('start_time'),
                    'end_time': cls.settings.get('end_time'),
                    'local_row_update_time_start': start_time,
                    'local_row_update_time_end': None,
                    'numrows': None,
                    'remark': '执行中',
                    'spark_code': cls.task.spark_code,
                    'pid':''
                }
            )
            if not created:
                # 如果记录已存在，则更新
                log_obj.executed_state = 'process'
                log_obj.complit_state = 2
                log_obj.start_time = cls.settings.get('start_time')
                log_obj.end_time = cls.settings.get('end_time')
                log_obj.local_row_update_time_start = start_time
                log_obj.local_row_update_time_end = None
                log_obj.numrows = None
                log_obj.remark = '执行中'
                log_obj.spark_code = cls.task.spark_code
                log_obj.pid=''
                log_obj.save()
            if not retry and cls.settings.get('execute_way') not in ['retry','action']:
                while not cls.is_depend():
                    new_log=Log.objects.filter(
                        pk=log_obj.pk,
                    ).first()
                    if new_log:
                        if new_log.complit_state==4:
                            logger.warning(f"[datax_plugin]:task {cls.task.id} 手动停止，退出任务")
                            return False
                    logger.info(f"[datax_plugin]:task {cls.task.id} 依赖未执行，等待中")
                    time.sleep(60)
                cls.generate_config()
            cls.pre_execute()
            process = subprocess.Popen(
                    command,
                    shell=True, encoding='utf-8',preexec_fn=os.setsid
                )
            pid = process.pid
            log_obj, created = Log.objects.get_or_create(
                task=cls.task,
                partition_date=cls.settings.get('partition_date'),
                execute_way=cls.settings.get('execute_way'),
                defaults={
                    'executed_state': 'process',
                    'complit_state': 2,
                    'start_time': cls.settings.get('start_time'),
                    'end_time': cls.settings.get('end_time'),
                    'local_row_update_time_start': start_time,
                    'local_row_update_time_end': None,
                    'numrows': None,
                    'remark': '执行中',
                    'datax_json': cls.task.datax_json,
                    'pid':pid
                }
            )
            if not created:
                # 如果记录已存在，则更新
                log_obj.executed_state = 'process'
                log_obj.complit_state = 2
                log_obj.start_time = cls.settings.get('start_time')
                log_obj.end_time = cls.settings.get('end_time')
                log_obj.local_row_update_time_start = start_time
                log_obj.local_row_update_time_end = None
                log_obj.numrows = None
                log_obj.remark = '执行中'
                log_obj.datax_json = cls.task.datax_json
                log_obj.pid=pid
                log_obj.save()
            returncode = process.wait()
            end_time=datetime.now()
            stderr=process.stderr
            stdout=process.stdout
            # 保存执行日志
            with open(log_path, "r", encoding="utf-8") as log_file:
                log_data=log_file.read()
            
            # 处理执行结果
            if returncode == 0:
                # 解析执行日志
                log=cls.parse_log(log_data)
                # 记录日志
                if log.get('error_num')>0:
                    logger.error(f"Spark 任务 {cls.task.id} 执行失败")
                    # 按唯一键查找或创建记录
                    log_obj, created = Log.objects.get_or_create(
                        task=cls.task,
                        partition_date=cls.settings.get('partition_date'),
                        execute_way=cls.settings.get('execute_way'),
                        defaults={
                            'executed_state': 'fail',
                            'complit_state': 0,
                            'start_time': cls.settings.get('start_time'),
                            'end_time': cls.settings.get('end_time'),
                            'local_row_update_time_start': start_time,
                            'local_row_update_time_end': end_time,
                            'numrows': log.get('read_num'),
                            'remark': "查看详细日志",
                            'spark_code': cls.task.spark_code,
                            'pid':pid
                        }
                    )
                    if not created:
                        # 如果记录已存在，则更新
                        log_obj.executed_state = 'fail'
                        log_obj.complit_state = 0
                        log_obj.start_time = cls.settings.get('start_time')
                        log_obj.end_time = cls.settings.get('end_time')
                        log_obj.local_row_update_time_start = start_time
                        log_obj.local_row_update_time_end = end_time
                        log_obj.numrows = log.get('read_num')
                        log_obj.remark = "查看详细日志"
                        log_obj.spark_code = cls.task.spark_code
                        log_obj.pid=pid
                        log_obj.save()

                elif log.get('is_None'):
                    logger.warning(f"Spark 任务 {cls.task.id} 执行完成，但是没有读取到数据")
                    # 按唯一键查找或创建记录
                    log_obj, created = Log.objects.get_or_create(
                        task=cls.task,
                        partition_date=cls.settings.get('partition_date'),
                        execute_way=cls.settings.get('execute_way'),
                        defaults={
                            'executed_state': 'success',
                            'complit_state': 1,
                            'start_time': cls.settings.get('start_time'),
                            'end_time': cls.settings.get('end_time'),
                            'local_row_update_time_start': start_time,
                            'local_row_update_time_end': end_time,
                            'numrows': log.get('read_num'),
                            'remark': f"Spark 任务 {cls.task.id} 执行完成，但是没有读取到数据",
                            'spark_code': cls.task.spark_code,
                            'pid':pid
                        }
                    )
                    if not created:
                        # 如果记录已存在，则更新
                        log_obj.executed_state = 'success'
                        log_obj.complit_state = 1
                        log_obj.start_time = cls.settings.get('start_time')
                        log_obj.end_time = cls.settings.get('end_time')
                        log_obj.local_row_update_time_start = start_time
                        log_obj.local_row_update_time_end = end_time
                        log_obj.numrows = log.get('read_num')
                        log_obj.remark = f"Spark 任务 {cls.task.id} 执行完成，但是没有读取到数据"
                        log_obj.spark_code = cls.task.spark_code
                        log_obj.pid=pid
                        log_obj.save()
                else:
                    logger.info(f"Spark 任务 {cls.task.id} 执行完成，读取到 {log.get('read_num')} 条数据")
                    # 按唯一键查找或创建记录
                    log_obj, created = Log.objects.get_or_create(
                        task=cls.task,
                        partition_date=cls.settings.get('partition_date'),
                        execute_way=cls.settings.get('execute_way'),
                        defaults={
                            'executed_state': 'success',
                            'complit_state': 1,
                            'start_time': cls.settings.get('start_time'),
                            'end_time': cls.settings.get('end_time'),
                            'local_row_update_time_start': start_time,
                            'local_row_update_time_end': end_time,
                            'numrows': log.get('read_num'),
                            'remark': f"Spark 任务 {cls.task.id} 执行完成，读取到 {log.get('read_num')} 条数据",
                            'spark_code': cls.task.spark_code,
                            'pid':pid
                        }
                    )
                    if not created:
                        # 如果记录已存在，则更新
                        log_obj.executed_state = 'success'
                        log_obj.complit_state = 1
                        log_obj.start_time = cls.settings.get('start_time')
                        log_obj.end_time = cls.settings.get('end_time')
                        log_obj.local_row_update_time_start = start_time
                        log_obj.local_row_update_time_end = end_time
                        log_obj.numrows = log.get('read_num')
                        log_obj.remark = f"Spark 任务 {cls.task.id} 执行完成，读取到 {log.get('read_num')} 条数据"
                        log_obj.spark_code = cls.task.spark_code
                        log_obj.pid=pid
                        log_obj.save()
                
                return True,stdout
            else:
                end_time=datetime.now()
                logger.exception(f"Spark 任务 {cls.task.id} 执行失败")
                # 按唯一键查找或创建记录
                log_obj, created = Log.objects.get_or_create(
                    task=cls.task,
                    partition_date=cls.settings.get('partition_date'),
                    execute_way=cls.settings.get('execute_way'),
                    defaults={
                        'executed_state': 'fail',
                        'complit_state': str(0),
                        'start_time': cls.settings.get('start_time'),
                        'end_time': cls.settings.get('end_time'),
                        'local_row_update_time_start': start_time,
                        'local_row_update_time_end': end_time,
                        'numrows': 0,
                        'remark': "查看详细日志",
                        'spark_code': cls.task.spark_code,
                        'pid':pid
                    }
                )
                if not created:
                    # 如果记录已存在，则更新
                    log_obj.executed_state = 'fail'
                    log_obj.complit_state = str(0)
                    log_obj.start_time = cls.settings.get('start_time')
                    log_obj.end_time = cls.settings.get('end_time')
                    log_obj.local_row_update_time_start = start_time
                    log_obj.local_row_update_time_end = end_time
                    log_obj.numrows = 0
                    log_obj.remark = "查看详细日志"
                    log_obj.spark_code = cls.task.spark_code
                    log_obj.pid=pid
                    log_obj.save()
                return False,stderr
        except Exception as e:
            end_time=datetime.now()
            logger.exception(f"Spark 任务 {cls.task.id} 执行异常: {e}")
            # 按唯一键查找或创建记录
            log_obj, created = Log.objects.get_or_create(
                task=cls.task,
                partition_date=cls.settings.get('partition_date'),
                execute_way=cls.settings.get('execute_way'),
                defaults={
                    'executed_state': 'fail',
                    'complit_state': str(0),
                    'start_time': cls.settings.get('start_time'),
                    'end_time': cls.settings.get('end_time'),
                    'local_row_update_time_start': start_time,
                    'local_row_update_time_end': end_time,
                    'numrows': 0,
                    'remark': str(e),
                    'spark_code': cls.task.spark_code,
                    'pid':pid
                }
            )
            if not created:
                # 如果记录已存在，则更新
                log_obj.executed_state = 'fail'
                log_obj.complit_state = str(0)
                log_obj.start_time = cls.settings.get('start_time')
                log_obj.end_time = cls.settings.get('end_time')
                log_obj.local_row_update_time_start = start_time
                log_obj.local_row_update_time_end = end_time
                log_obj.numrows = 0
                log_obj.remark = str(e)
                log_obj.spark_code = cls.task.spark_code
                log_obj.pid=pid
                log_obj.save()
            return False,str(e)


