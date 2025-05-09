from django.contrib import admin
from .models import Project, DataSource, Connection, Config, ConfigItem, Task,Log,SplitConfig,ColumnConfig,Tenant,Notification
from executors.extensions.datax.datax_plugin import DataXPluginManager
from loguru import logger
from  pathlib import Path
from django.contrib import admin
 # 消息
from django.contrib import messages
from .extensions.datax.utils import DatabaseTableHandler
admin.site.site_header = '大数据调度管理后台'  # 设置header
admin.site.site_title = '大数据调度管理后台'   # 设置title
admin.site.index_title = '大数据调度管理后台'

class ConfigItemInline(admin.TabularInline):
    model = ConfigItem
    extra = 1

class ConnectionInline(admin.StackedInline):
    model = Connection
    extra = 1

class TaskInline(admin.TabularInline):
    model = Task
    extra = 1
# 复制数据action
def copy_data(modeladmin, request, queryset):
    for obj in queryset:
        obj.id = None  # 重置主键
        obj.save()  # 保存新对象
        messages.success(request, f"数据 {obj.name} 已成功复制！")

copy_data.short_description = "复制数据"


# 传参数的action
def execute_datax_tasks_generate_config_update(modeladmin, request, queryset):
    try:
        settings={
            "execute_way": "update",
        }
        datax=DataXPluginManager(queryset,settings)
        datax.generate_config()
        messages.success(request, "生成DATAX配置JSON成功！")
    except Exception as e:
        logger.exception(e)
        messages.error(request, str(e))

execute_datax_tasks_generate_config_update.short_description = "生成DATAX配置JSON（更新）"
def execute_datax_tasks_generate_config_all(modeladmin, request, queryset):
    try:
        settings={
            "execute_way": "all",
        }
        datax=DataXPluginManager(queryset,settings)
        datax.generate_config()
        messages.success(request, "生成DATAX配置JSON成功！")
    except Exception as e:
        logger.exception(e)
        messages.error(request, str(e))

execute_datax_tasks_generate_config_all.short_description = "生成DATAX配置JSON（全量）"
# 执行json
def execute_datax_tasks_execute_json(modeladmin, request, queryset):
    try:
        settings={
            "execute_way": "action",
        }
        datax=DataXPluginManager(queryset,settings)
        datax.execute_action()
        messages.success(request, "执行DATAX配置JSON成功！")
    except Exception as e:
        logger.exception(e)
        messages.error(request, str(e))

execute_datax_tasks_execute_json.short_description = "执行DATAX配置JSON"

def execute_project_tasks_datax_all(modeladmin, request, queryset):
    try:
        for project in queryset:
            settings={
                "execute_way": "all",
                **project.config
            }
            datax=DataXPluginManager(project.task_set.all(),settings)
            datax.generate_config()
            datax.execute_action()
            messages.success(request, f"项目 {project.name} 运行成功！")
    except Exception as e:
        logger.exception(e)
        messages.error(request, str(e))

execute_project_tasks_datax_all.short_description = "执行项目任务（全量）"
def execute_project_tasks_datax_update(modeladmin, request, queryset):
    try:
        
        for project in queryset:
            settings={
                "execute_way": "update",
                **project.config
            }
            datax=DataXPluginManager(project.task_set.all(),settings)
            datax.generate_config()
            datax.execute_action()
            messages.success(request, f"项目 {project.name} 运行成功！")
    except Exception as e:
        logger.exception(e)
        messages.error(request, str(e))
execute_project_tasks_datax_update.short_description = "执行项目任务（更新）"
# 日志重试
def log_retry(modeladmin, request, queryset):
    try:
        for log in queryset:
            if log.executed_state == "success":
                messages.warning(request, f"日志 {log.id} 已成功执行，无需重试。")
                continue
            settings={
                "execute_way": log.execute_way,
                **log.task.project.config
            }
            datax=DataXPluginManager([log.task],settings)
            datax.execute_retry(log)
            messages.success(request, f"日志 {log.id} 重试成功！")
    except Exception as e:
        logger.exception(e)
        messages.error(request, str(e))

log_retry.short_description = "重新执行"
@admin.register(Project)
class ProjectAdmin(admin.ModelAdmin):
    list_display = ('name','tenant', 'description','engine','config', 'created_at','updated_at')
    search_fields = ('name',)
    list_filter = ('created_at',)
    inlines = [TaskInline]
    ordering = ('-created_at','-updated_at')
    actions = [execute_project_tasks_datax_all,execute_project_tasks_datax_update]
    actions=[copy_data]
@admin.register(DataSource)
class DataSourceAdmin(admin.ModelAdmin):
    list_display = ('name','username', 'type', 'created_at')
    search_fields = ('name', 'type')
    list_filter = ('type', 'created_at')
    inlines = [ConnectionInline]
    ordering = ('-created_at','-updated_at')
    actions=[copy_data]
@admin.register(Config)
class ConfigAdmin(admin.ModelAdmin):
    list_display = ('name','description', 'created_at','updated_at')
    search_fields = ('name',)
    list_filter = ('created_at',)
    inlines = [ConfigItemInline]
    actions=[copy_data]

# 注册 SplitConfig 模型
@admin.register(SplitConfig)
class SplitConfigAdmin(admin.ModelAdmin):
    list_display = ('name','db_split', 'tb_split', 'tb_other', 'db_other')
    list_filter = ('name','db_split', 'tb_split')
    actions=[copy_data]
# 注册 ColumnConfig 模型
@admin.register(ColumnConfig)
class ColumnConfigAdmin(admin.ModelAdmin):
    list_display = ('name','is_partition','is_add_sync_time', 'partition_column','sync_time_column')
    list_filter = ('name', 'partition_column')
    actions=[copy_data]
# 注册 Task 模型
@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ('id','name', 'project','column_config','split_config', 'is_active', 'is_delete', 'created_at', 'updated_at')
    list_filter = ('project', 'is_active', 'is_delete')
    search_fields = ('id','name', 'project__name')
    filter_horizontal = ('data_source', 'data_target')
    readonly_fields = ('created_at', 'updated_at')
    readonly_fields=('tables',)
    list_display_links = ('id','name')
    ordering = ('-created_at','-updated_at')
    actions = [copy_data,execute_datax_tasks_generate_config_update,execute_datax_tasks_generate_config_all,execute_datax_tasks_execute_json]
    



@admin.register(Log)
class LogAdmin(admin.ModelAdmin):
    list_display = ('task', 'project','source_db','source_table','target_db','target_table','partition_date', 'execute_way', 'executed_state', 'start_time', 'end_time', 'numrows', 'created_at', 'updated_at')
    search_fields = ('task__name','partition_date', 'project')
    list_filter = ('execute_way','partition_date','executed_state','task__project__name')
    readonly_fields = ('task','source_db','source_table','target_db','target_table','log_file','partition_date', 'execute_way', 'executed_state','start_time', 'end_time', 'numrows', 'created_at', 'updated_at')
    list_display_links = ('task',)
    actions = [log_retry]
    ordering = ('-created_at','-updated_at')
    
    

    def has_add_permission(self, request):
        return False
    def has_change_permission(self, request, obj=None):
        return False
    def has_delete_permission(self, request, obj=None):
        return False

#Tenant
@admin.register(Tenant)
class TenantAdmin(admin.ModelAdmin):
    list_display = ('name', 'description', 'created_at', 'updated_at')
    search_fields = ('name',)
    list_filter = ('created_at',)
    ordering = ('-created_at','-updated_at')
    actions=[copy_data]

#Notification
@admin.register(Notification)
class NotificationAdmin(admin.ModelAdmin):
    list_display = ('name','engine', 'description', 'created_at', 'updated_at')
    search_fields = ('name',)
    list_filter = ('engine',)
    actions=[copy_data]