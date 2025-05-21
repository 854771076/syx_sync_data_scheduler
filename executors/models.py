from django.db import models
from  pathlib import Path
from .extensions.metadata.utils import DatabaseTableHandler
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib import messages
from loguru import logger
from django.contrib.auth.models import User

# 租户
class Tenant(models.Model):
    name = models.CharField(max_length=255, verbose_name="租户名称", db_index=True)
    description = models.TextField(verbose_name="租户描述",default="",null=True,blank=True)
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")
    def __str__(self):
        return self.name
    class Meta:
        verbose_name = '租户'
        verbose_name_plural = verbose_name

# 通知配置
class Notification(models.Model):
    # 引擎
    class Engine(models.TextChoices):
        DINGTALK = "dingtalk", "钉钉"
        EMAIL = "email", "邮件"
    name = models.CharField(max_length=255, verbose_name="通知名称", db_index=True)
    # 模板markdown文本
    template = models.TextField(verbose_name="模板markdown文本",default="",null=True,blank=True)
    # 通知引擎
    engine = models.CharField(max_length=255, verbose_name="通知引擎",null=True,blank=True,choices=Engine.choices,default=Engine.DINGTALK)
    # 通知配置
    config=models.JSONField(verbose_name="通知配置",default=dict,null=True,blank=True)
    is_active=models.BooleanField(verbose_name="是否启用",default=True)

    description = models.TextField(verbose_name="通知描述",default="",null=True,blank=True)
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")

    def __str__(self):
        return self.name
    class Meta:
        verbose_name = '通知'
        verbose_name_plural = verbose_name

# Create your models here.
class Project(models.Model):
    class Engine(models.TextChoices):
        DATAX = "datax", "DataX"
        # SPARK = "spark", "Spark"
        # FLINK = "flink", "Flink"
    name = models.CharField(max_length=255, verbose_name="项目名称", db_index=True)
    description = models.TextField(verbose_name="项目描述")
    engine = models.CharField(max_length=255, verbose_name="项目引擎", choices=Engine.choices,default=Engine.DATAX)
    config=models.JSONField(verbose_name='项目配置,{"cron": "05 00 * * *", "max_worker": 10, "DATAX_JVM_OPTIONS": "-Xms1g -Xmx2g"}',default=dict,null=True,blank=True)
    # 租户
    tenant = models.ForeignKey(Tenant, on_delete=models.SET_DEFAULT, verbose_name="关联租户",default=1,null=True,blank=True)
    # 通知配置
    notification = models.ForeignKey(Notification, on_delete=models.SET_DEFAULT, verbose_name="关联通知",default=1,null=True,blank=True)
    is_active=models.BooleanField(verbose_name="是否启用",default=False)

    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")

    def __str__(self):
        return self.name
    class Meta:
        verbose_name = '项目'
        verbose_name_plural = verbose_name

class DataSource(models.Model):
    TYPE_CHOICES = [
        ("mysql", "MySQL"),
        ("starrocks", "StarRocks"),
        ("hive", "Hive"), 
        ("hdfs", "HDFS"),
        ("hbase", "HBase"),
        ("redis", "Redis"),
        ("kafka", "Kafka"),
        ("mongo", "Mongo"),
    ]
    name = models.CharField(max_length=255, verbose_name="数据源名称", db_index=True)
    type = models.CharField(max_length=50, verbose_name="数据源类型", choices=TYPE_CHOICES)
    description = models.TextField(verbose_name="数据源描述",default="",null=True,blank=True)
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")
    @property
    def username(self):
        return self.connection.username
    def __str__(self):
        return f"{self.name} ({self.type})"
    class Meta:
        verbose_name = '数据源'
        verbose_name_plural = verbose_name

class Connection(models.Model):
    data_source = models.OneToOneField(DataSource, on_delete=models.CASCADE, verbose_name="关联数据源")
    host = models.CharField(max_length=255, verbose_name="主机地址", db_index=True)
    port = models.IntegerField(verbose_name="端口", db_index=True)
    username = models.CharField(max_length=255, verbose_name="用户名")
    password = models.CharField(max_length=255, verbose_name="密码")
    charset = models.CharField(max_length=50, verbose_name="字符集",default="utf8mb4",null=True,blank=True)
    params=models.JSONField(verbose_name="参数",default=dict,null=True,blank=True)
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")

    def __str__(self):
        return f"{self.host}:{self.port}"
    class Meta:
        verbose_name = '连接'
        verbose_name_plural = verbose_name

# 全局配置
class Config(models.Model):
    name = models.CharField(max_length=255, verbose_name="配置名称", db_index=True)
    description = models.TextField(verbose_name="配置描述",default="",null=True,blank=True)
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")

    def __str__(self):
        return self.name
    class Meta:
        verbose_name = '全局配置'
        verbose_name_plural = verbose_name

class ConfigItem(models.Model):
    config = models.ForeignKey(Config, on_delete=models.CASCADE, verbose_name="关联配置")
    key = models.CharField(max_length=255, verbose_name="配置项键名", db_index=True)
    value = models.TextField(verbose_name="配置项值")
    description = models.TextField(verbose_name="配置项描述",default="",null=True,blank=True)
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")

    def __str__(self):
        return f"{self.key}: {self.value}"
    class Meta:
        verbose_name = '配置项'
        verbose_name_plural = verbose_name
class SplitConfig(models.Model):
    """分库分表配置模型"""
    name = models.CharField(max_length=255, verbose_name="配置名称", db_index=True)
    description = models.TextField(verbose_name="配置描述",default="",null=True,blank=True)
    db_split = models.BooleanField(default=False, verbose_name="是否分库")
    tb_split = models.BooleanField(default=False, verbose_name="是否分表")
    tb_other = models.BooleanField(default=False, verbose_name="是否有other分表")
    db_other = models.BooleanField(default=False, verbose_name="是否有other分库")
    # 是否时间后缀分表
    tb_time_suffix = models.BooleanField(default=False, verbose_name="是否时间后缀分表")
    # 时间后缀格式
    tb_time_suffix_format = models.CharField(max_length=255, verbose_name="时间分表后缀格式", default="%Y%m%d", null=True, blank=True)
    # 开始时间
    tb_time_suffix_start_time = models.DateField(verbose_name="开始分表时间", null=True, blank=True)
    # 结束时间
    tb_time_suffix_end_time = models.DateField(verbose_name="结束分表时间", null=True, blank=True)
    # 时间更新频率
    tb_time_suffix_update_frequency = models.IntegerField(verbose_name="时间分表更新频率(天)", default=1, null=True, blank=True)
    db_split_start_number = models.IntegerField(null=True, blank=True, verbose_name="分库起始编号")
    db_split_end_number = models.IntegerField(null=True, blank=True, verbose_name="分库结束编号")
    tb_split_start_number = models.IntegerField(null=True, blank=True, verbose_name="分表起始编号")
    tb_split_end_number = models.IntegerField(null=True, blank=True, verbose_name="分表结束编号")
    tb_custom_split=models.BooleanField(default=False, verbose_name="是否自定义分表")
    db_custom_split=models.BooleanField(default=False, verbose_name="是否自定义分库")
    custom_split_tb_list = models.JSONField(verbose_name="自定义分表列表", default=list, null=True, blank=True)
    custom_split_db_list = models.JSONField(verbose_name="自定义分库列表", default=list, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")
    class Meta:
        verbose_name = '分库分表配置'
        verbose_name_plural = verbose_name

    def __str__(self):
        return f"{self.name}"





class Task(models.Model):
    project = models.ForeignKey(Project, on_delete=models.CASCADE, verbose_name="关联项目")
    data_source =  models.ForeignKey(DataSource, related_name='source_tasks', on_delete=models.SET_NULL, verbose_name="源数据源", null=True, blank=True)
    data_target =  models.ForeignKey(DataSource, related_name='target_tasks', on_delete=models.SET_NULL, verbose_name="目标数据源", null=True, blank=True)
    is_active = models.BooleanField(default=True, verbose_name="是否启用")
    name = models.CharField(max_length=255, verbose_name="任务名称")
    description = models.TextField(verbose_name="任务描述", null=True, blank=True)
    source_db = models.CharField(max_length=80, verbose_name="源数据库名称")
    source_table = models.CharField(max_length=80, verbose_name="源表名称")
    target_db = models.CharField(max_length=80, verbose_name="目标数据库名称")
    target_table = models.CharField(max_length=80, verbose_name="目标表名称")
    is_delete = models.BooleanField(default=False, verbose_name="是否删除")
    split_config = models.ForeignKey(SplitConfig, on_delete=models.SET_NULL, verbose_name="分库分表配置", null=True, blank=True)
    # column_config = models.ForeignKey(ColumnConfig, on_delete=models.SET_NULL, verbose_name="字段配置", null=True, blank=True)
    partition_column = models.CharField(max_length=255, null=True, blank=True, verbose_name="分区字段", default="partition_date")
    exclude_columns = models.JSONField(verbose_name="排除字段列表", default=list, null=True, blank=True)
    columns=models.JSONField(verbose_name="自定义字段列表", default=list, null=True, blank=True)
    reader_transform_columns=models.JSONField(verbose_name='映射字段字典(如{"partition_date":"partition_date1"})', default=dict, null=True, blank=True)
    # 同步时间字段，如果有则会自动生成同步时间字段，默认为cdc_sync_date
    sync_time_column = models.CharField(max_length=255, null=True, blank=True, verbose_name="同步时间字段，如果有则会自动生成同步时间字段，例如cdc_sync_date", default=None)
    # 是否分区
    is_partition = models.BooleanField(default=False, verbose_name="是否分区")
    # 是否添加同步时间字段
    is_add_sync_time = models.BooleanField(default=False, verbose_name="是否添加同步时间字段")
    update_column = models.CharField(max_length=255, null=True, blank=True, verbose_name="更新字段", default="create_time")
    # 启动参数配置
    config = models.JSONField(verbose_name='启动参数配置,{"jvm_options": "-Xms5g -Xmx10g","session":[],"preSql":[],"compress":"NONE","fieldDelimiter":"\\u0001","fileType":"text"或"orc"}', default=dict, null=True, blank=True)
    datax_json=models.JSONField(verbose_name="最新DataX任务JSON",default=dict,null=True,blank=True)
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")
    @property
    def tables_update(self):
        return DatabaseTableHandler.split(self,execute_way="update")
    tables_update.fget.short_description = "分库分表列表（增量）"
    @property
    def tables_all(self):
        return DatabaseTableHandler.split(self)
    tables_all.fget.short_description = "分库分表列表（全量）"
    

    
    def __str__(self):
        return f"{self.name}"

    class Meta:
        verbose_name = '任务'
        verbose_name_plural = verbose_name
        # 添加唯一约束，确保每个项目的任务名称唯一
        # unique_together = ('project','source_db','source_table','target_db','target_table','split_config','is_delete')

class Log(models.Model):
    class executeWayChoices(models.TextChoices):
        full_sync = 'all', '全量同步'
        incr_sync = 'update', '增量同步'
        full_load = 'other', '其他'
        action='action','自定义执行'
        retry='retry','重试'
    class statusChoices(models.TextChoices):
        success = 'success', '成功'
        fail = 'fail', '失败'
        process = 'process', '处理中'
        bak = 'bak', '备份'
    class complitStateChoices(models.TextChoices):
        success = 1, '成功'
        fail = 0, '失败'
        process = 2, '处理中'
        bak = 3, '备份'
    task = models.ForeignKey(Task, on_delete=models.CASCADE, verbose_name="关联任务")
    partition_date = models.CharField(max_length=10,verbose_name="分区日期",null=True,blank=True)
    execute_way = models.CharField(max_length=10, verbose_name="执行方式",choices=executeWayChoices.choices,default=executeWayChoices.incr_sync)
    executed_state = models.CharField(max_length=255, verbose_name="状态",choices=statusChoices.choices,default=statusChoices.fail)
    complit_state = models.IntegerField(verbose_name="完成状态",default=complitStateChoices.fail)
    start_time = models.DateTimeField(verbose_name="同步脚本启动时间",null=True,blank=True)
    end_time = models.DateTimeField(verbose_name="同步脚本结束时间",null=True,blank=True)
    local_row_update_time_start = models.DateTimeField(verbose_name="同步数据最小时间",null=True,blank=True)
    local_row_update_time_end = models.DateTimeField(verbose_name="同步数据最大时间",null=True,blank=True)
    numrows = models.BigIntegerField(verbose_name="同步时间区间条数",null=True,blank=True)
    remark = models.TextField(verbose_name="备注",null=True,blank=True)
    db_num = models.IntegerField(verbose_name="完成分库数",null=True,blank=True)
    t_num = models.IntegerField(verbose_name="完成分表数",null=True,blank=True)
    t_num_0 = models.IntegerField(verbose_name="数据量为零的分表数",null=True,blank=True)
    map_input_nums = models.IntegerField(verbose_name=" 拉取数量统计",null=True,blank=True)
    datax_json=models.JSONField(verbose_name="DataX任务",default=dict,null=True,blank=True)
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")
    
    @property
    def project(self):
        return self.task.project
    # @property
    # def log_file(self):
    #     log_file_path = self.logs_dir / f"{self.task.id}.log"
    #     if log_file_path.exists():
    #         with open(log_file_path, 'r', encoding='utf-8') as file:
    #             # 取最后200行
    #             lines = file.readlines()[-10:]
    #             return '\n'.join(lines)
    #     return "日志文件不存在"
    @property
    def source_db(self):
        return self.task.source_db
    @property
    def source_table(self):
        return self.task.source_table
    @property
    def target_db(self):
        return self.task.target_db
    @property
    def target_table(self):
        return self.task.target_table
    @property
    def partition_column(self):
        return self.task.partition_column
    @property
    def update_column(self):
        return self.task.update_column
    # 检查当日（使用partition_date）是否已执行成功
    
    def __str__(self):
        return f"{self.task.name} - {self.created_at}"
    class Meta:
        verbose_name = '执行日志'
        verbose_name_plural = verbose_name
        unique_together = ('task', 'execute_way', 'complit_state','partition_date')

class MetadataTable(models.Model):
    """元数据-数据表"""
    data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE, verbose_name="关联数据源")
    name = models.CharField(max_length=255, verbose_name="表名称", db_index=True)
    db_name = models.CharField(max_length=255, verbose_name="数据库名称", db_index=True)
    description = models.TextField(verbose_name="表描述", null=True, blank=True)
    meta_data = models.JSONField(verbose_name="表元数据", null=True, blank=True)
    create_time = models.DateTimeField(verbose_name="创建时间", auto_now_add=True)
    update_time = models.DateTimeField(verbose_name="更新时间", auto_now=True)
    def __str__(self):
        return f"{self.db_name}.{self.name}"

    class Meta:
        verbose_name = '数据表元数据'
        unique_together = ('data_source', 'db_name', 'name')

class AsyncTaskStatus(models.Model):
    user_id=models.IntegerField(verbose_name="用户ID",null=True,blank=True)
    task_name = models.CharField(max_length=255)
    status = models.CharField(max_length=20, choices=[
        ('pending', '等待中'),
        ('success', '成功'),
        ('failed', '失败')
    ])
    message = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)


# @receiver(post_save, sender=Task)
# def sync_table_metadata(sender, instance:Task, created, **kwargs):
#     from executors.extensions.metadata.utils import DatabaseTableHandler
#     """监听任务创建/更新，同步表元数据"""
#     try:
#         tables=DatabaseTableHandler.split(instance)
        
#         config=dict(ConfigItem.objects.all().values_list("key", "value"))
#         # 同步源表元数据
#         _sync_single_metadata(
#             instance.data_source,
#             instance.source_db,
#             instance.source_table,
#             config,
#             tables

#         )
#         # 同步目标表元数据
#         _sync_single_metadata(
#             instance.data_target,
#             instance.target_db,
#             instance.target_table,
#             config
#         )
#         logger.info(f"任务 {instance.name} 的元数据已同步")
#     except Exception as e:
#         logger.error(e)
def _sync_single_metadata(data_source:DataSource, db_name:str, table_name:str,config,tables=[]):
    """同步单个表的元数据"""
    from executors.extensions.metadata.utils import DatabaseTableHandler,HiveUtil,MysqlUtil
    db_type = data_source.type
    if tables:
        source_db,source_table=tables[0].split(".")
    else:
        source_db,source_table=db_name,table_name
    # 根据不同类型获取元数据
    if db_type == 'mysql' or db_type == 'starrocks':
        mysql_conn=MysqlUtil.get_mysql_client_by_config(data_source)
        columns = MysqlUtil.get_table_schema(mysql_conn,source_db,source_table)
    elif db_type == 'hive' or db_type == 'hdfs':
        hive_conn=HiveUtil.get_hive_client_by_config(config,data_source)
        columns = HiveUtil.get_table_schema(hive_conn,db_name,table_name)
    else:
        raise ValueError(f"不支持的数据库类型: {db_type}")
    # 保存到元数据表中，如果已存在则更新
    cls, created = MetadataTable.objects.update_or_create(
        data_source=data_source,
        db_name=db_name,
        name=table_name,
        defaults={'meta_data': columns}
    )
    return columns


# 启动时更新所有任务的元数据
def update_all_tasks_metadata():
    """启动时更新所有任务的元数据"""
    for task in Task.objects.filter(is_active=True):
        try:
            sync_table_metadata(sender=Task, instance=task, created=False)
        except Exception as e:
            logger.error(e)
