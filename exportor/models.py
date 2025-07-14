from django.db import models

class ExportCustomer(models.Model):
    customer_name = models.CharField(max_length=255, verbose_name='客户名称', null=True)
    access_key = models.CharField(max_length=255, verbose_name='客户授权秘钥', null=True)
    create_time = models.DateTimeField(verbose_name='创建时间', null=True, db_index=True)

    class Meta:
        db_table = 'syx_export_customer'
        verbose_name = '推送客户信息'
        managed = False

    def __str__(self):
        return self.customer_name

class ExportHdfsTask(models.Model):
    from_task_id = models.BigIntegerField(verbose_name='上游任务ID')
    task_name = models.CharField(max_length=255, verbose_name='任务名称')
    db = models.CharField(max_length=64, verbose_name='库')
    tb = models.CharField(max_length=64, verbose_name='表')
    db_shard_num = models.IntegerField(verbose_name='库分库数量')
    tb_shard_num = models.IntegerField(verbose_name='表分库数量')
    partition_date = models.CharField(max_length=64, verbose_name='分区日期')
    task_type = models.CharField(max_length=64, verbose_name='任务类型')
    # hdfs_urls = models.TextField(verbose_name='HDFS路径列表')
    hdfs_file_num = models.IntegerField(verbose_name='文件数量')
    row_num = models.IntegerField(verbose_name='数据行数')
    is_valid = models.BooleanField(verbose_name='是否有效')
    create_time = models.DateTimeField(verbose_name='创建时间', null=True, db_index=True)

    class Meta:
        db_table = 'syx_export_hdfs_task'
        indexes = [
            models.Index(fields=['db', 'tb', 'partition_date'], name='db_tb_partition_date'),
        ]
        managed = False

    def __str__(self):
        return f"{self.db}.{self.tb} {self.partition_date}"

class ExportHdfsFile(models.Model):
    task = models.ForeignKey(ExportHdfsTask, on_delete=models.CASCADE, related_name='hdfs_files')
    hdfs_url = models.CharField(max_length=255, verbose_name='HDFS路径')
    file_info = models.TextField(verbose_name='fileInfo')
    is_cached = models.BooleanField(verbose_name='是否缓存')
    cached_path = models.CharField(max_length=255, verbose_name='缓存路径')
    is_valid = models.BooleanField(verbose_name='是否有效')
    create_time = models.DateTimeField(verbose_name='创建时间', null=True, db_index=True)

    class Meta:
        db_table = 'syx_export_hdfs_file'
        managed = False

    def __str__(self):
        return self.hdfs_url

class ExportCustomerSubscribe(models.Model):
    class SyncConfigChoice(models.TextChoices):
        TRUNCATE = 'truncate'
        UPSERT = 'upsert'
        APPEND = 'append'
    class BooleanChoice(models.TextChoices):
        开启 = '1'
        关闭 = '0'
    customer = models.ForeignKey(ExportCustomer,on_delete=models.SET_NULL, null=True, blank=True,
        db_constraint=False)
    topic = models.CharField(max_length=255, verbose_name='订阅主题', null=True)
    db = models.CharField(max_length=255, verbose_name='订阅库', null=True)
    tb = models.CharField(max_length=255, verbose_name='订阅表', null=True)
    full_partition = models.CharField(max_length=255, verbose_name='全量分区', null=True)
    start_partition_date = models.CharField(max_length=255, verbose_name='增量开始分区', null=True)
    is_full_syncing = models.CharField(max_length=255, verbose_name='全量同步状态', null=True,choices=BooleanChoice.choices)
    is_increment_syncing = models.CharField(max_length=255, verbose_name='增量同步状态', null=True,choices=BooleanChoice.choices)
    sync_config = models.CharField(max_length=255, verbose_name='同步配置', null=True,choices=SyncConfigChoice.choices)
    filter = models.CharField(max_length=255, verbose_name='数据筛选', null=True,blank=True)
    is_valid = models.BooleanField(verbose_name='是否有效')
    create_time = models.DateTimeField(verbose_name='创建时间', null=True, db_index=True)

    class Meta:
        db_table = 'syx_export_customer_subscribe'
        unique_together = ('db', 'tb', 'customer')  # 对应UNIQUE KEY `db_tb`
        managed = False

    def __str__(self):
        return f"{self.customer.customer_name} {self.topic} {self.db}.{self.tb}"

class ExportCustomerHdfsFileSyncExec(models.Model):
    class TaskStatusChoice(models.TextChoices):
        # sync_ready(下载准备完成),syncing(下载中),sync_success(下载完成),sync_fail(下载失败),executing(执行中),exec_success(执行完成),exec_fail(执行失败)
        下载准备完成 = 'sync_ready'
        下载中 = 'syncing'
        下载完成 = 'sync_success'
        下载失败 = 'sync_fail'
        执行中 = 'executing'
        执行完成 = 'exec_success'
        执行失败 = 'exec_fail'
    task = models.ForeignKey(ExportHdfsTask, on_delete=models.CASCADE, related_name='sync_executions')
    customer = models.ForeignKey(ExportCustomer, on_delete=models.CASCADE, related_name='customer_syncs')
    file = models.ForeignKey(ExportHdfsFile, on_delete=models.SET_NULL, null=True, verbose_name='文件')
    hdfs_url = models.CharField(max_length=2048, verbose_name='HDFS路径')
    status = models.CharField(max_length=64, verbose_name='执行状态',choices=TaskStatusChoice.choices)
    message = models.TextField(verbose_name='简略信息', null=True)
    message_log_file = models.CharField(max_length=255, verbose_name='日志路径', null=True)
    download_begin_time = models.DateTimeField(verbose_name='下载开始时间', null=True)
    download_end_time = models.DateTimeField(verbose_name='下载结束时间', null=True)
    exec_begin_time = models.DateTimeField(verbose_name='执行开始时间', null=True)
    exec_end_time = models.DateTimeField(verbose_name='执行结束时间', null=True)
    exec_rows = models.BigIntegerField(verbose_name='执行行数', null=True)
    is_valid = models.BooleanField(verbose_name='是否有效')
    create_time = models.DateTimeField(verbose_name='创建时间', null=True, db_index=True)

    class Meta:
        db_table = 'syx_export_customer_hdfs_file_sync_exec'
        managed = False

    def __str__(self):
        return f"{self.customer.customer_name} {self.task} {self.hdfs_url}"