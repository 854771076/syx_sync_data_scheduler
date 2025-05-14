from django.contrib import admin
from .models import *
from  pathlib import Path
from django.contrib import admin
from .actions import *


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
    extra = 0
    fields = ('name', 'source_db', 'target_db', 'is_active')
    show_change_link = True
class LogInline(admin.StackedInline):
    model = Log
    extra = 0
    fields = ('execute_way', 'executed_state', 'start_time')
    readonly_fields = ('executed_state', 'start_time')

@admin.register(Project)
class ProjectAdmin(admin.ModelAdmin):
    list_display = ('id','name','is_active','tenant', 'description','engine','config', 'created_at','updated_at')
    search_fields = ('name',)
    list_filter = ('created_at',)
    inlines = [TaskInline]
    ordering = ('-created_at','-updated_at')
    actions = [execute_project_tasks_datax_all,execute_project_tasks_datax_update]
    actions=[copy_data]

@admin.register(DataSource)
class DataSourceAdmin(admin.ModelAdmin):
    list_display = ('id','name', 'type', 'description_short','username', 'type', 'created_at')
    search_fields = ('name', 'type', 'description')
    list_filter = ('type', 'created_at')
    inlines = [ConnectionInline]
    ordering = ('-created_at','-updated_at')
    actions=[copy_data]

    @admin.display(description='描述')
    def description_short(self, obj):
        return f"{obj.description[:20]}..." if obj.description else ""
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
    list_display = ('id','name','db_split', 'tb_split', 'tb_other', 'db_other')
    list_filter = ('name','db_split', 'tb_split')
    actions=[copy_data]
# 注册 ColumnConfig 模型
# @admin.register(ColumnConfig)
# class ColumnConfigAdmin(admin.ModelAdmin):
#     list_display = ('name','is_partition','is_add_sync_time', 'partition_column','sync_time_column')
#     list_filter = ('name', 'partition_column')
#     actions=[copy_data]
# 注册 Task 模型
@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ('id','name', 'project','split_config', 'is_active', 'is_delete', 'created_at', 'updated_at')
    list_filter = ('project', 'is_active', 'is_delete')
    search_fields = ('id','name', 'project__name')
    # filter_horizontal = ('data_source', 'data_target')
    readonly_fields = ('created_at', 'updated_at')
    readonly_fields=('tables_update','tables_all',)
    list_display_links = ('id','name')
    ordering = ('-created_at','-updated_at')
    actions = [copy_data,execute_datax_tasks_generate_config_update,execute_datax_tasks_generate_config_all,execute_datax_tasks_execute_json]
    

@admin.register(Log)
class LogAdmin(admin.ModelAdmin):
    list_display = ('id','task', 'project','source_db','source_table','target_db','target_table','partition_date', 'execute_way', 'executed_state', 'start_time', 'end_time', 'numrows', 'created_at', 'updated_at')
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


@admin.register(MetadataTable)
class MetadataTableAdmin(admin.ModelAdmin):
    list_display = ('name', 'db_name', 'data_source', 'create_time', 'update_time')
    list_filter = ('data_source__type','name','db_name')
    search_fields = ('name', 'db_name')
    raw_id_fields = ('data_source',)

    @admin.display(description='数据源类型')
    def data_source_type(self, obj):
        return obj.data_source.type

    @admin.display(description='字段数')
    def field_count(self, obj):
        return obj.fields.count()
@admin.register(MetadataLineage)
class MetadataLineageAdmin(admin.ModelAdmin):
    list_display = ('lineage_path', 'task_info')
    list_select_related = ('source_table', 'target_table', 'task')
    raw_id_fields = ('task',)

    @admin.display(description='血缘路径')
    def lineage_path(self, obj):
        return f"{obj.source_table.db_name}.{obj.source_table.name} → {obj.target_table.db_name}.{obj.target_table.name}"

    @admin.display(description='关联任务')
    def task_info(self, obj):
        return obj.task.name if obj.task else '系统级'
