from django.contrib import admin
from .models import (
    ExportCustomer,
    ExportHdfsTask,
    ExportHdfsFile,
    ExportCustomerSubscribe,
    ExportCustomerHdfsFileSyncExec
)
from enum import Enum
from .actions import generate_token,exportor_admin_api
@admin.register(ExportCustomer)
class ExportCustomerAdmin(admin.ModelAdmin):
    list_display = ('id', 'customer_name', 'access_key', 'create_time')
    list_filter = ('create_time',)
    search_fields = ('customer_name', 'access_key')
    ordering = ('-create_time',)
    list_per_page = 20
    actions=[generate_token]
    def save_model(self, request, obj, form, change):
        """
        自定义保存逻辑
        :param request: 请求对象
        :param obj: 模型实例
        :param form: 表单实例
        :param change: 是否是修改操作
        """
        # 保存前操作示例
        if not change:  # 新建操作
            obj.created_by = request.user
            
        # 自动生成access_key的逻辑
        if not obj.access_key:
            obj.access_key = self.generate_access_key()
        # 调用父类保存方法
        super().save_model(request, obj, form, change)
        
    def generate_access_key(self):
        """生成随机访问密钥"""
        import uuid
        return str(uuid.uuid4()).replace('-', '')[:16]


@admin.register(ExportHdfsTask)
class ExportHdfsTaskAdmin(admin.ModelAdmin):
    list_display = ('id', 'task_name', 'db', 'tb', 'partition_date', 'task_type', 'db_shard_num', 'tb_shard_num', 'is_valid')
    list_filter = ('db', 'tb', 'task_type', 'is_valid')
    search_fields = ('task_name', 'hdfs_urls')
    ordering = ('-create_time',)
    list_per_page = 20

@admin.register(ExportHdfsFile)
class ExportHdfsFileAdmin(admin.ModelAdmin):
    list_display = ('id', 'task', 'hdfs_url', 'is_cached', 'file_info', 'create_time')
    list_filter = ('task', 'is_cached', 'is_valid')
    raw_id_fields = ('task',)
    ordering = ('-create_time',)
    list_per_page = 20

@admin.register(ExportCustomerSubscribe)
class ExportCustomerSubscribeAdmin(admin.ModelAdmin):
    
    list_display = ('id', 'customer', 'topic', 'db', 'tb', 'is_valid', 'create_time')
    list_filter = ('customer', 'is_full_syncing', 'is_increment_syncing', 'is_valid')
    search_fields = ('topic', 'db', 'tb')
    raw_id_fields = ('customer',)
    ordering = ('-create_time',)
    list_per_page = 20
    def save_model(self, request, obj, form, change):
        """
        自定义保存逻辑
        :param request: 请求对象
        :param obj: 模型实例
        :param form: 表单实例
        :param change: 是否是修改操作
        """
        # 调用父类保存方法
        super().save_model(request, obj, form, change)
        exportor_admin_api.modify_task(obj.customer.id)
@admin.register(ExportCustomerHdfsFileSyncExec)
class ExportCustomerHdfsFileSyncExecAdmin(admin.ModelAdmin):
    list_display = ('id', 'task', 'customer', 'file', 'status', 'exec_rows', 'is_valid', 'create_time')
    list_filter = ('status', 'is_valid')
    search_fields = ('message', 'hdfs_url')
    raw_id_fields = ('task', 'customer', 'file')
    ordering = ('-create_time',)
    list_per_page = 20