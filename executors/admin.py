from django.contrib import admin
from .models import *
from  pathlib import Path
from django.contrib import admin
from .actions import *
from django.utils.html import format_html

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
    list_display_links = ('id','name')
    list_filter = ('created_at',)
    inlines = [TaskInline]
    ordering = ('-created_at','-updated_at')
    actions = [execute_project_tasks_datax_all,execute_project_tasks_datax_update]
    actions=[copy_data,init_scheduler,execute_project_datax_tasks,enable,disable]
    readonly_fields = ('created_at', 'updated_at')
    fieldsets = (
        ('基础信息', {
            'fields': (
                'name', 'description', 'is_active', 'engine'
            )
        }),
        ('配置信息', {
            'classes': ('collapse'),
            'fields': (
                'config', 'notification','tenant'
            ) 
        }),
        ('系统信息', {
            'classes': ('collapse','readonly'),
            'fields': (
                'created_at', 'updated_at'
            )
        }) 
    )
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('configure/', self.admin_site.admin_view(project_configure_view), name='project_configure')
        ]
        return custom_urls + urls
@admin.register(DataSource)
class DataSourceAdmin(admin.ModelAdmin):
    list_display = ('id','name', 'type', 'description_short','username', 'type', 'created_at')
    search_fields = ('name', 'type', 'description')
    list_display_links = ('id','name')
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
    list_display_links = ('id','name')
    actions=[copy_data]
    ordering = ('-created_at','-updated_at')
    readonly_fields = ('created_at', 'updated_at')
    fieldsets = (
        ('基础信息', {
            'fields': (
                'name', 'description',
            ) 
        })
        ,
        ('分库配置', {
            'classes': ('collapse',),
            'fields': (
                'db_split', 'db_other', 'db_split_start_number', 'db_split_end_number',
            )
        }),
        ('分表配置', {
            'classes': ('collapse',),
            'fields': (
                'tb_split', 'tb_other', 'tb_split_start_number', 'tb_split_end_number'
            ) 
        }),
        ('时间分表配置', {
            'classes': ('collapse',),
            'fields': (
                'tb_time_suffix', 'tb_time_suffix_format', 'tb_time_suffix_start_time', 'tb_time_suffix_end_time', 'tb_time_suffix_update_frequency'
            ) 
        }),
        ('自定义分库分表配置', {
            'classes': ('collapse',),
            'fields': (
                ('db_custom_split', 'custom_split_db_list'),
                ('tb_custom_split', 'custom_split_tb_list')
            )
        }),
        ('系统信息', {
            'classes': ('collapse','readonly'),
            'fields': (
                'created_at', 'updated_at'
            )
        })

    )

@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ('id','name','lineage_path', 'project','split_config', 'is_active', 'is_delete', 'created_at', 'updated_at')
    list_filter = ('project', 'is_active', 'is_delete')
    search_fields = ('id','name', 'project__name')
    list_display_links = ('id','name')
    ordering = ('-created_at','-updated_at')
    actions = [copy_data,execute_datax_tasks_generate_config_update,execute_datax_tasks_generate_config_all,execute_datax_tasks_execute_json,execute_datax_tasks,enable,disable]
    fieldsets = (
        ('基础信息', {
            'fields': (
                'name', 
                'project',
            )
        }),
        ('表配置', {
            'fields': (
                ('data_source','data_target'),
                ('source_db', 'source_table'),
                ('target_db', 'target_table'),
                ('is_active', 'is_delete'),
                'split_config',
            )
        }),
        ('字段配置', {
            'fields': (
                'is_partition','partition_column',
                'is_add_sync_time','sync_time_column',
                'exclude_columns','columns','reader_transform_columns',
                'update_column'

                
            )
        }),
        ('高级配置', {
            'classes': ('collapse',),
            'fields': ('is_custom_script','datax_json','spark_code','config')
        }),
        ('系统信息', {
            'classes': ('collapse','readonly'),
            'fields': (
                'tables_update',
                'tables_all',
                'created_at', 'updated_at'
            )
        })
    )
    readonly_fields = ('created_at', 'updated_at', 'tables_update', 'tables_all')
    @admin.display(description='路径')
    def lineage_path(self, obj):
        return f'{obj.source_db}.{obj.source_table} → {obj.target_db}.{obj.target_table}'
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('configure/', self.admin_site.admin_view(task_configure_view), name='datax_configure')
        ]
        return custom_urls + urls

@admin.register(Log)
class LogAdmin(admin.ModelAdmin):
    list_display = ('id','task','partition_date', 'execute_way', 'complit_state_format', 'project','source_db','source_table','target_db','target_table', 'start_time', 'end_time', 'numrows')
    search_fields = ('task__name','partition_date', 'project')
    list_filter = ('execute_way','partition_date','executed_state','task__project__name')
    readonly_fields = ('task','source_db','source_table','target_db','target_table','partition_date', 'execute_way', 'executed_state','start_time', 'end_time', 'numrows', 'created_at', 'updated_at','log_file','datax_json')
    list_display_links = ('task','id')
    actions = [log_retry]
    ordering = ('-created_at','-updated_at')
    fieldsets = (
        ('基础信息', {
            'fields': (
                'task',
                'source_db','source_table','target_db','target_table',
                'partition_date', 'execute_way', 'executed_state','start_time', 'end_time', 'numrows'
            )
        }),
        ('日志信息', {
            'classes': ('collapse',),
            'fields': (
                'log_file',
                'datax_json',
                'spark_code'
            ) 
        }) ,
        ('系统信息', {
            'classes': ('collapse','readonly'),
            'fields': (
                'created_at', 'updated_at'
            ) 
        }),


    )
    
    @admin.display(description='日志详细')
    def log_file(self, obj):
        
        if obj.task.project.engine == 'datax':
            if obj.execute_way=='retry':
                logs_path = f"/static/datax_logs/{obj.partition_date}/{obj.task.id}_retry.log"
            else:
                logs_path = f"/static/datax_logs//{obj.partition_date}/{obj.task.id}.log"
        elif obj.task.project.engine == 'spark':
            if obj.execute_way=='retry':
                logs_path = f"/static/spark_logs/{obj.partition_date}/{obj.task.id}_retry.log"
            else:
                logs_path = f"/static/spark_logs//{obj.partition_date}/{obj.task.id}.log"
        else:
            return "日志文件不存在"
        return format_html('<a href="{}" target="_blank">查看日志</a>', logs_path)
        
        # log_file_path = logs_dir / f"{obj.task.id}.log"
        # if log_file_path.exists():
        #     with open(log_file_path, 'r', encoding='utf-8') as file:
        #         # 取最后200行
        #         lines = file.readlines()[-200:]
        #         return '\n'.join(lines)
        # return "日志文件不存在"
    

    def has_add_permission(self, request):
        return False
    def has_change_permission(self, request, obj=None):
        return False
    # 状态美化
    def complit_state_format(self, obj):
        if obj.complit_state == 0:
            return format_html('<span style="color: red;">失败</span>')
        elif obj.complit_state == 1:
            return format_html('<span style="color: green;">成功</span>')
        elif obj.complit_state == 2:
            return format_html('<span style="color: yellow;">执行中</span>')
        elif obj.complit_state == 3:
            return format_html('<span >备份</span>')
        else:
            return format_html('<span style="color: gray;">未知</span>')
    complit_state_format.short_description = '执行状态'
#Tenant
@admin.register(Tenant)
class TenantAdmin(admin.ModelAdmin):
    list_display = ('name', 'description', 'created_at', 'updated_at')
    search_fields = ('name',)
    list_filter = ('created_at',)
    ordering = ('-created_at','-updated_at')
    actions=[copy_data]
    fieldsets = (
        ('基础信息', {
            'fields': (
                'name', 'description','queue'
            )
        }), 
        ('系统信息', {
            'classes': ('collapse','readonly'),
            'fields': (
                'created_at', 'updated_at'
            ) 
        })
    )
    readonly_fields = ('created_at', 'updated_at')


#Notification
@admin.register(Notification)
class NotificationAdmin(admin.ModelAdmin):
    list_display = ('name','engine', 'description', 'created_at', 'updated_at')
    search_fields = ('name',)
    list_filter = ('engine',)
    actions=[copy_data]
    fieldsets = (
        ('基础信息', {
            'fields': (
                'name', 'engine','description'
            )
        }), 
        ('系统信息', {
            'classes': ('collapse','readonly'),
            'fields': (
                'created_at', 'updated_at'
            )
        })
    )
    readonly_fields = ('created_at', 'updated_at')

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
        return len(obj.meta_data)
    @admin.display(description='字段信息')
    def meta_data_format(self, obj):
        
        # 优先使用meta_data字段数据
        if obj.meta_data:
            html = "<table style='width:100%;border-collapse:collapse;'>"
            html += "<tr><th style='border:1px solid #ddd;padding:8px;text-align:left;'>字段名</th><th style='border:1px solid #ddd;padding:8px;text-align:left;'>类型</th></tr>"
            for field in obj.meta_data:
                html += f"<tr><td style='border:1px solid #ddd;padding:8px;'>{field['name']}</td><td style='border:1px solid #ddd;padding:8px;'>{field['type']}</td></tr>"
            html += "</table>"
            return format_html(html)
        # 备用方案：使用关联的fields数据
        fields = obj.fields.all()
        if not fields:
            return "无字段信息"
        html = "<table style='width:100%;border-collapse:collapse;'>"
        html += "<tr><th style='border:1px solid #ddd;padding:8px;text-align:left;'>字段名</th><th style='border:1px solid #ddd;padding:8px;text-align:left;'>类型</th></tr>"
        for field in fields:
            html += f"<tr><td style='border:1px solid #ddd;padding:8px;'>{field.name}</td><td style='border:1px solid #ddd;padding:8px;'>{field.type}</td></tr>"
        html += "</table>"
        return format_html(html)
    meta_data_format.allow_tags = True
    fieldsets = (
        ('基础信息', {
            'classes': ('readonly'),
            'fields': (
                'name', 'db_name', 'data_source','description'
            )
        }),
        ('字段信息', {
            'classes': ('collapse','readonly'),
            'fields': (
                'data_source_type', 'field_count','meta_data_format'
            )
        }), 
        ('系统信息', {
            'classes': ('collapse','readonly'),
            'fields': (
                'create_time', 'update_time'
            )
        })
    )    
    readonly_fields = ('create_time', 'update_time','data_source_type', 'field_count','meta_data_format', 'db_name', 'data_source')

@admin.register(AsyncTaskStatus)
class AsyncTaskStatusAdmin(admin.ModelAdmin):
    list_display = ('id','user', 'task_name', 'status_format', 'created_at', 'updated_at')
    search_fields = ('task_id', 'status')
    list_filter = ('status',)
    list_display_links = ('id','user')

    @admin.display(description='用户')
    def user(self, obj):
        from django.contrib.auth import get_user_model
        User = get_user_model()
        try:
            user = User.objects.get(id=obj.user_id)
            return user.username
        except User.DoesNotExist:
            return "未知用户"
    # 美化状态
    @admin.display(description='状态')
    def status_format(self, obj):
        if obj.status == 'pending':
            return format_html('<span style="color: #FFA500;">{}</span>', obj.status)  # 橙色
        elif obj.status == 'success':
            return format_html('<span style="color: #008000;">{}</span>', obj.status)  # 绿色
        elif obj.status =='failed':
            return format_html('<span style="color: #FF0000;">{}</span>', obj.status)  # 红色