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
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, Boolean,BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import UniqueConstraint
import datetime
Base = declarative_base()
class SparkTask(Base):
    """Spark分库分表任务记录"""
    __tablename__ = 'exporter_spark_tasks'
    __table_args__ = (
        # 添加联合唯一约束
        UniqueConstraint('partition_date', 'source_db', 'source_table', 'sink_db', 'sink_table', 
                        name='uq_spark_task'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment='自增主键ID')
    partition_date = Column(String(10), nullable=False, comment='数据分区日期，格式YYYYMMDD')
    source_db = Column(String(40), nullable=False, comment='源数据库名称')
    source_table = Column(String(70), nullable=False, comment='源数据表名称')
    sink_db = Column(String(40), nullable=False, comment='目标数据库名称')
    sink_table = Column(String(70), nullable=False, comment='目标数据表名称')
    status = Column(String(20), nullable=False, comment='任务状态: PENDING/RUNNING/SUCCESS/FAILED')
    start_time = Column(DateTime, comment='任务开始时间')
    end_time = Column(DateTime, comment='任务结束时间')
    total_rows = Column(Integer, default=0, comment='总行数(用于计算进度)')
    message = Column(Text, comment='任务消息或错误信息')
    export_type = Column(String(20), nullable=False, comment='导出类型: all/other/update')
    start_partition = Column(String(10), comment='开始分区')
    end_partition = Column(String(10), comment='结束分区')
    split_field = Column(String(20), comment='分表字段')
    shard_num = Column(Integer, default=16, comment='分库数')
    shard_table_num = Column(Integer, default=16, comment='分表数')
    created_at = Column(DateTime, default=datetime.datetime.utcnow, comment='记录创建时间')
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, 
                       onupdate=datetime.datetime.utcnow, comment='记录更新时间')
def get_db_session(uri):
    engine = create_engine(uri)
    Session = sessionmaker(bind=engine)
    return Session
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
            # 初始化数据库会话
            Session = get_db_session(self.config["database"]["connection_string"])
            self.session = Session()

            # 创建数据库表（如果不存在）
            Base.metadata.create_all(
                create_engine(self.config["database"]["connection_string"])
            )

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

    def check_task_status(
        self, source_db, source_table, sink_db, sink_table, partition_date
    ):
        """检查任务状态"""
        task = (
            self.session.query(SparkTask)
            .filter_by(
                source_db=source_db,
                source_table=source_table,
                sink_db=sink_db,
                sink_table=sink_table,
                partition_date=partition_date,
            )
            .first() 
        )

        if task:
            if task.status == "SUCCESS":
                return True, task

        return False, None

    def process_task(self, task_config):
        """处理单个任务"""
        source_db = task_config["source_db"]
        source_table = task_config["source_table"]
        sink_db = task_config["sink_db"]
        sink_table = task_config["sink_table"]
        partition_field = task_config.get("partition_field")
        id_field = task_config.get("id_field")
        
        update_field = task_config.get("update_field")
        export_type = task_config.get("export_type", self.export_type)
        is_sharded = task_config.get("is_sharded", False)
        shard_num = task_config.get("shard_num", 16)
        shard_table_num = task_config.get("shard_table_num", 16)
        if export_type == "all":
            condition = f'partition_date<="{self.partition_date}"'
            method="overwrite"
        elif export_type == "update":
            condition = f'partition_date="{self.partition_date}"'
            method="append"
        elif export_type == "other":
            condition = f'partition_date<="{self.end_date}" and partition_date>"{self.start_date}"'
            method="append"

        # 使用当前处理的分区日期或默认值
        current_partition_date = self.partition_date or (
            datetime.datetime.now() - datetime.timedelta(days=1)
        ).strftime("%Y%m%d")

        self.logger.info(
            f"开始处理任务: {source_db}.{source_table} -> {sink_db}.{sink_table}"
        )
        self.logger.info(f"分区日期: {current_partition_date}")
        # 构建源表完整名称
        full_source_table = f"{source_db}.{source_table}"
        df = self.spark.table(full_source_table).filter(condition)
        df.persist(StorageLevel.MEMORY_AND_DISK)
        if not id_field:
            if 'md5_id' in df.columns:
                id_field='md5_id'
            elif 'firm_eid' in df.columns:
                id_field='firm_eid'
            elif 'id' in df.columns:
                id_field='id'
            else:
                raise ValueError(f"id_field not found in {source_db}.{source_table}")
            self.logger.info(f"id_field: {id_field}")
        window = Window.partitionBy("md5_id").orderBy(desc("local_row_update_time"))
        df = (
            df.withColumn("rw", row_number().over(window))
            .filter(col("rw") == 1)
            .drop("rw")
        )
        df = df.withColumn(
            "db_id", conv(substring(id_field, -2, 1), 16, 10).cast("int")
        )  # 转换为16进制整数
        df = df.withColumn(
            "table_id", conv(substring(id_field, -1, 1), 16, 10).cast("int")
        )  # 转换为16进制整数
        total_rows = df.count()
        self.logger.info(f"读出记录总数:{total_rows}|")
        sink_db_name = sink_db
        sink_table_name = sink_table
        # 如果启用分库分表
        unique_params = {
                "partition_date": current_partition_date,
                "source_db": source_db,
                "source_table": source_table,
                "sink_db": sink_db_name,
                "sink_table": sink_table_name,
                'shard_num':shard_num,
                'shard_table_num':shard_table_num,
                'split_field':id_field,
            }
        if export_type == "other":
            unique_params["start_partition"] = self.start_date
            unique_params["end_partition"] = self.end_date

        # 分表任务的Upsert操作
        task = self.session.query(SparkTask).filter_by(**unique_params).first()
        if not task:
            task = SparkTask(**unique_params, status="RUNNING")
            task.start_time = datetime.datetime.now()
            task.export_type = export_type
            self.session.add(task)
        else:
            task.status = "RUNNING"
            task.export_type = export_type
            task.message = ""
            task.start_time = datetime.datetime.now()
        self.session.commit()
        try:
            
            # 检查任务状态
            is_complete, existing_task = self.check_task_status(
                source_db,
                source_table,
                sink_db_name,
                sink_table_name,
                current_partition_date,
            )
            if df.rdd.isEmpty():
                task.status = "SUCCESS"
                task.end_time = datetime.datetime.now()
                task.total_rows = 0
                self.session.commit()
                self.logger.info(
                    f"任务数据为空，跳过: {source_db}.{source_table} -> {sink_db_name}.{sink_table_name}"
                )
                return
            if is_complete:
                self.logger.info(
                    f"任务已完成，跳过执行: {source_db}.{source_table} -> {sink_db_name}.{sink_table_name}"
                )
                return
            df=df.withColumn('partition_date',lit(current_partition_date))
            if export_type == "update":
                df.repartition(5).write.mode(method).partitionBy("partition_date",'db_id','table_id').format("orc").saveAsTable(f"{sink_db}.{sink_table}")
            else:
                df.write.mode(method).partitionBy("partition_date",'db_id','table_id').format("orc").saveAsTable(f"{sink_db}.{sink_table}")
            # 更新进度
            task.end_time = datetime.datetime.now()
            task.total_rows = total_rows
            task.status = "SUCCESS"
            self.session.commit()
            self.logger.info(
                f"任务执行成功: {source_db}.{source_table} -> {sink_db_name}.{sink_table_name}"
            )
        except Exception as e:
            # 更新任务状态为失败
            task.status = "FAILED"
            task.end_time = datetime.datetime.now()
            task.message = str(e)[:200]
            self.session.commit()
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
    env='prod'
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
                'spark.sql.files.minPartitionBytes':67108864
            },
            "database": {
                "connection_string": "mysql+pymysql://root:123456@127.0.0.1:3306/test"
            },

        }
        task_config={
                "name": "${source_db}_split",
                "source_db": "${source_db}",
                "source_table": "${source_table}",
                "sink_db": "${target_db}",
                "sink_table": "${target_table}",
                "partition_field": "partition_date",
                "id_field": "firm_eid",
                "update_field": "local_row_update_time",
                "is_sharded": ${is_split},
                "shard_num": 16,
                "shard_table_num": 16,
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
                'spark.sql.files.minPartitionBytes':67108864

            },
            "database": {
                "connection_string": "mysql+pymysql://etl_sync_user_rw:LZYd5uG!@10.255.79.163:3306/db_ods_etl_dwd_ads_sync_mpp"
            },

        }
        task_config={
                "name": "${source_db}_split",
                "source_db": "${source_db}",
                "source_table": "${source_table}",
                "sink_db": "${target_db}",
                "sink_table": "${target_table}",
                "partition_field": "partition_date",
                "id_field": "firm_eid",
                "update_field": "local_row_update_time",
                # "is_sharded": ${is_split},
                # "shard_num": 16,
                # "shard_table_num": 16,
            }
    parser = argparse.ArgumentParser(description="Spark数据处理工具")
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
