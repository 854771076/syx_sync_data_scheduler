from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, hex, lit, conv
import uuid
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc
import datetime
import yaml
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys
import os
import time
import argparse

# StorageLevel
from pyspark import StorageLevel
from py4j.java_gateway import java_import
import datetime
class SparkDataProcessor:
    def __init__(
        self,
        config,
        partition_date=None,
        export_type=None,
        start_date=None,
        end_date=None,
        task_config={}
    ):
        """初始化Spark数据处理器"""
        self.config = config
        self.partition_date = partition_date
        self.export_type = export_type
        self.start_date = start_date
        self.end_date = end_date
        self.task_config=task_config

        self.logger = self.setup_logging()
        self.spark = None
        self.session = None
        self.sc = None
        self.fs = None

    def setup_logging(self):
        """配置日志系统"""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler("spark_tasks.log"), logging.StreamHandler()],
        )
        return logging.getLogger(__name__)

    def initialize(self):
        """初始化SparkSession和数据库连接"""
        try:

            builder = SparkSession.builder.appName(
                self.config['spark'].get("app_name", "DataProcessingApp")
            )
            master = self.config['spark'].get("master")
            if master:
                builder.master(master)
            # 设置额外的Spark配置
            for key, value in self.config['spark'].items():
                if key not in ["app_name", "master"]:
                    builder.config(key, value)
                    self.logger.info(f"Spark配置: {key}={value}")

            self.spark = builder.getOrCreate()
            self.sc = self.spark.sparkContext
            hadoop_conf = self.sc._jsc.hadoopConfiguration()
            java_import(self.sc._jvm, "org.apache.hadoop.fs.Path")
            java_import(self.sc._jvm, "org.apache.hadoop.fs.FileSystem")
            FileSystem = self.sc._jvm.FileSystem
            self.fs = FileSystem.get(hadoop_conf)

            self.logger.info("初始化完成")
        except Exception as e:
            self.logger.error(f"初始化失败: {str(e)}")
            raise

    def close_resources(self):
        """关闭资源"""
        try:
            if self.session:
                self.session.close()
                self.logger.info("数据库会话已关闭")

            if self.spark:
                self.spark.stop()
                self.logger.info("SparkSession已关闭")
        except Exception as e:
            self.logger.error(f"关闭资源失败: {str(e)}")

    
    def process_task(self, task_config):
        """处理单个任务"""
        source_db = task_config["source_db"]
        source_table = task_config["source_table"]
        sink_db = task_config["sink_db"]
        sink_table = task_config["sink_table"]
        partition_field = task_config.get("partition_field")

        update_field = task_config.get("update_field")
        partition_date=datetime.datetime.strptime(self.partition_date, "%Y%m%d")
        start_date=(datetime.datetime.strptime(self.start_date , "%Y%m%d") if self.start_date else partition_date).strftime("%Y-%m-%dT00:00:00")
        end_date=(datetime.datetime.strptime(self.end_date , "%Y%m%d") if self.end_date else (partition_date+datetime.timedelta(days=1)) ).strftime("%Y-%m-%dT00:00:00") 
        export_type = task_config.get("export_type", self.export_type)
        if export_type == "all":
            query = """
            {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "%s": {
                                        "lte": "%s",
                                        "format": "strict_date_optional_time"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
            """%(update_field,end_date)
            method="overwrite"
        elif export_type == "update":
            query = """
            {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "%s": {
                                        "lte": "%s",
                                        "gt": "%s",
                                        "format": "strict_date_optional_time"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
            """%(update_field,end_date,start_date)

            method="append"
        elif export_type == "other":
            query = """
            {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "%s": {
                                        "lte": "%s",
                                        "gt": "%s",
                                        "format": "strict_date_optional_time"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
            """%(update_field,end_date,start_date)
            method="append"

        # 使用当前处理的分区日期或默认值
        current_partition_date = self.partition_date or (
            datetime.datetime.now() - datetime.timedelta(days=1)
        ).strftime("%Y%m%d")

        self.logger.info(
            f"开始处理任务: {source_db}.{source_table} -> {sink_db}.{sink_table}"
        )
        self.logger.info(f"分区日期: {current_partition_date}")
        self.logger.info(f"task_config: {task_config}")
        self.logger.info(f"export_type: {export_type}")
        self.logger.info(f"query: {query}")
        df = self.spark.read \
        .format("org.elasticsearch.spark.sql") \
        .option("es.query", query) \
        .option("es.resource", source_table) \
        .load()
        df.persist(StorageLevel.MEMORY_AND_DISK)

        total_rows = df.count()
        self.logger.info(f"读出记录总数:{total_rows}|")
        sink_db_name = sink_db
        sink_table_name = sink_table
        try:
            if df.rdd.isEmpty():
              
                self.logger.info(
                    f"任务数据为空，跳过: {source_db}.{source_table} -> {sink_db_name}.{sink_table_name}"
                )
                return
            df=df.withColumn('partition_date',lit(current_partition_date))
            df.write.mode(method).partitionBy("partition_date").format("orc").saveAsTable(f"{sink_db_name}.{sink_table_name}")
           
            self.logger.info(
                f"任务执行成功: {source_db}.{source_table} -> {sink_db_name}.{sink_table_name}"
            )
        except Exception as e:
            # 更新任务状态为失败
            
            self.logger.error(
                f"任务执行失败: {source_db}.{source_table} -> {sink_db_name}.{sink_table_name}, 错误: {str(e)}"
            )

    def run(self):
        """运行所有配置的任务"""
        try:
            self.initialize()
            self.process_task(self.task_config)

        except Exception as e:
            self.logger.error(f"执行任务时发生错误: {str(e)}")
            raise
        finally:
            self.close_resources()
if __name__ == "__main__":
    env='dev'
    if env=='dev':
        import findspark
        findspark.init()
        spark_config = {
            "spark": {
                "app_name": "DataExporter",
                "spark.sql.shuffle.partitions": 200,
                "spark.sql.sources.bucketing.enabled": "true",
                "master": "local[*]",
                "spark.sql.sources.partitionOverwriteMode": "dynamic",
                "spark.hive.exec.dynamic.partition": "true",
                "spark.hive.exec.dynamic.partition.mode": "nonstrict",
                "spark.sql.orc.impl": "native",
                "spark.sql.orc.enableVectorizedReader": "true",
                "spark.sql.hive.convertMetastoreOrc": "true",
                "spark.memory.dynamicAllocation.enabled":"false",
                "spark.hive.support.quoted.identifiers": "none",
                "spark.hive.metastore.uris": "thrift://10.8.15.240:9083",
                "spark.sql.warehouse.dir": "/user/hive/warehouse",
                "spark.sql.catalogImplementation": "hive",
                "spark.hadoop.yarn.resourcemanager.address": "10.8.15.240:8032",
                "spark.hadoop.fs.defaultFS": "hdfs://10.8.15.240:8020",
                "spark.sql.files.maxPartitionBytes": 268435456,
                'spark.sql.files.minPartitionBytes':67108864,
                "spark.es.nodes": "10.255.87.151",  # ES节点地址
                "spark.es.port": "9200" ,       # ES端口
                "spark.jars": "service/src/commons-httpclient-3.1_1.jar,service/src/elasticsearch-spark-20_2.11-7.12.1.jar"

            }
            

        }
        task_config={
            "name": "dwd_syx_es",
            "source_db": "dwd_syx",
            "source_table": "openapi_syx_basicdata_v3",
            "sink_db": "dwd_syx",
            "sink_table": "openapi_syx_basicdata_v3",
            "partition_field": "partition_date",
            "update_field": "updateTime",
        
        }
    else:
        
        spark_config = {
            "spark": {
                "app_name": "DataExporter",
                "spark.sql.shuffle.partitions": 200,
                "spark.sql.sources.bucketing.enabled": 'true',
                "spark.sql.sources.partitionOverwriteMode": "dynamic",
                "hive.exec.dynamic.partition": "true",
                "hive.exec.dynamic.partition.mode": "nonstrict",
                "spark.sql.orc.enableVectorizedReader": "true",
                "spark.sql.hive.convertMetastoreOrc": "true",
                "spark.memory.dynamicAllocation.enabled":"false",
                "spark.sql.files.maxPartitionBytes": 268435456,
                'spark.sql.files.minPartitionBytes':67108864,
                "spark.es.nodes": "${source_host}",  # ES节点地址
                "spark.es.port": "${source_port}" ,       # ES端口  
                # "spark.es.net.http.auth.user": "elastic",  # 用户名（可选）
                # "spark.es.net.http.auth.pass": "password"

            }
        }
    task_config={
            "name": "${source_table}_es",
            "source_db": "${source_db}",
            "source_table": "${source_table}",
            "sink_db": "${target_db}",
            "sink_table": "${target_table}",
            "partition_field": "partition_date",
            "update_field": "${update_column}",
        
        }
    parser = argparse.ArgumentParser(description="Spark数据处理工具(ES同步)")
    parser.add_argument("--partition_date", help="分区日期筛选条件", default="${partition_date}")
    parser.add_argument("--export_type", help="执行类型[all|update|other]", default="${execute_way}")
    start_date='${start_time}'.split(' ')[0].replace('-','')
    end_date='${end_time}'.split(' ')[0].replace('-','')
    parser.add_argument("--start_date", help="开始日期%%Y%%m%%d", default=start_date)
    parser.add_argument("--end_date", help="结束日期%%Y%%m%%d", default=end_date)

    args = parser.parse_args()


    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("spark_tasks.log"), logging.StreamHandler()],
    )
    logger = logging.getLogger(__name__)

    if args.partition_date:
        logger.info(f"分区日期参数: {args.partition_date}")

    start_time = time.time()

    try:
        processor = SparkDataProcessor(
            spark_config,
            partition_date=args.partition_date,
            export_type=args.export_type,
            start_date=args.start_date,
            end_date=args.end_date,
            task_config=task_config
        )
        results = processor.run()

        end_time = time.time()
        logger.info(f"任务执行完成，耗时: {end_time - start_time} 秒")

        logger.info("任务执行结果:")

    except Exception as e:
        logger.exception(f"程序执行失败: {str(e)}")
        raise e