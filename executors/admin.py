from django.contrib import admin
from .models import *
from  pathlib import Path
from django.contrib import admin
from .actions import *
from django.http import HttpResponse
from django.utils.html import format_html
import os
import signal
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
    list_filter = ('engine', 'is_active', 'tenant')
    # inlines = [TaskInline]
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
    actions = [copy_data,execute_datax_tasks_generate_config_update,execute_datax_tasks_generate_config_all,execute_datax_tasks_execute_json,execute_datax_tasks,enable,disable,create_table]
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
                'task_log_dependency'
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
    list_display = ('id','task','partition_date', 'execute_way', 'numrows', 'complit_state_format','execute_time', 'project','source_db','source_table','target_db','target_table','local_row_update_time_start','local_row_update_time_end')
    search_fields = ('task__name','task__id','partition_date')
    list_filter = ('execute_way','partition_date','executed_state','task__project__name')
    readonly_fields = ('task','source_db','source_table','target_db','target_table','partition_date', 'execute_way', 'executed_state','local_row_update_time_start','local_row_update_time_end','start_time', 'end_time', 'numrows', 'created_at', 'updated_at','log_file','datax_json')
    list_display_links = ('task','id')
    
    ordering = ('-created_at','-updated_at')
    @admin.action(description="停止任务")
    def stop_thread_by_id(modeladmin, request, queryset):
        for log in queryset:
            if log.pid and log.complit_state ==2:
                try:
                    kill_process_group(int(log.pid),1)
                    logger.info(f"进程 {log.pid} 已停止")
                    messages.success(request, f"进程 {log.pid} 已停止")
                    log.executed_state = 'stop'
                    log.complit_state=4
                    log.pid=''
                    log.save()
                    
                except ProcessLookupError:
                    messages.warning(request, f"进程 {log.pid} 不存在")
                    logger.warning(f"进程 {log.pid} 不存在")
                except PermissionError:
                    messages.error(request, f"没有权限停止进程 {log.pid}")
                    logger.error(f"没有权限停止进程 {log.pid}")
                except Exception as e:
                    messages.error(request, f"停止进程 {log.pid} 时发生错误: {e}")
                    logger.error(f"停止进程 {log.pid} 时发生错误: {e}")
            elif not log.pid and log.complit_state ==2: 
                log.executed_state = 'stop'
                log.complit_state=4
                log.pid=''
                log.save()
                messages.success(request, f"进程 {log.pid} 已停止")
                logger.info(f"进程 {log.pid} 已停止")
            else:
                messages.warning(request, f"任务未启动或已停止，无法停止线程")
    actions = [log_retry,log_retry_new,stop_thread_by_id]
    fieldsets = (
        ('基础信息', {
            'fields': (
                'task',
                'source_db','source_table','target_db','target_table',
                'partition_date', 'execute_way', 'executed_state','start_time', 'end_time', 'numrows','local_row_update_time_start','local_row_update_time_end','execute_time'
            )
        }),
        ('日志信息', {
            'classes': ('collapse',),
            'fields': (
                'remark',
                'log_file',
                'datax_json',
                'spark_code'
            ) 
        }) ,
        ('系统信息', {
            'classes': ('collapse','readonly'),
            'fields': (
                'created_at', 'updated_at','pid'
            ) 
        }),


    )
    # 当前文件绝对路径
    current_file_path = os.path.abspath(__file__)
    # static路径
    static_path = os.path.join(os.path.dirname(current_file_path), 'static')
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('log_detail/<int:task_id>/<str:partition_date>/<str:execute_way>', self.admin_site.admin_view(self.log_detail_view), name='log_detail'),
            path('log_file/<int:task_id>/<str:partition_date>/<str:execute_way>', self.admin_site.admin_view(self.log_file_view), name='log_file')
        ]
        return custom_urls + urls

    def log_file_view(self, request, task_id, partition_date, execute_way):
        log = Log.objects.get(task__id=task_id, partition_date=partition_date, execute_way=execute_way)
        
        # 获取实际日志路径
        if log.task.project.engine == 'datax':
            log_path = f"datax_logs/{partition_date}/{task_id}"
        elif log.task.project.engine =='spark':
            log_path = f"spark_logs/{partition_date}/{task_id}"
        else:
            log_path = f"logs/{partition_date}/{task_id}"   
        
        if execute_way == 'retry':
            full_path = self.static_path/Path(f"{log_path}_retry.log")
        else:
            full_path = self.static_path/Path(f"{log_path}.log")

        if full_path.exists():
            with open(full_path, 'r', encoding='utf-8') as f:
                content = f.read()
                response = HttpResponse(content, content_type='text/plain; charset=utf-8')
                response['Content-Disposition'] = f'inline; filename="{full_path.name}"'
                return response
        return HttpResponse("File not found", status=404)

    def log_detail_view(self, request, task_id, partition_date,execute_way):
        log = Log.objects.get(task__id=task_id, partition_date=partition_date,execute_way=execute_way)
        if log.task.project.engine == 'datax':
            log_path = f"datax_logs/{partition_date}/{task_id}"
        elif log.task.project.engine =='spark':
            log_path = f"spark_logs/{partition_date}/{task_id}"
        else:
            log_path = f"logs/{partition_date}/{task_id}"   
        
        if execute_way == 'retry':
            full_path = self.static_path/Path(f"{log_path}_retry.log")
        else:
            full_path = self.static_path/Path(f"{log_path}.log")
        context = {
            'task': log.task,
            'partition_date': partition_date,
            'log_path': log_path,
            'execute_way': execute_way,
            'last_modified': time.ctime(full_path.stat().st_mtime) if full_path.exists() else '',
            'opts': self.model._meta
        }
        return render(request, 'admin/log_detail.html', context)
    # 执行时间local_row_update_time_end-local_row_update_time_start
    @admin.display(description='执行时间')
    def execute_time(self, obj):
        if obj.local_row_update_time_end and obj.local_row_update_time_start:
            return obj.local_row_update_time_end - obj.local_row_update_time_start
    @admin.display(description='日志详细')
    def log_file(self, obj):
        url = reverse('admin:log_detail', args=[obj.task.id, obj.partition_date,obj.execute_way])
        return format_html(f'<a href="{url}" target="_blank">查看详细日志</a>')
    # def log_file(self, obj):
        
    #     if obj.task.project.engine == 'datax':
    #         if obj.execute_way=='retry':
    #             logs_path = f"/static/datax_logs/{obj.partition_date}/{obj.task.id}_retry.log"
    #         else:
    #             logs_path = f"/static/datax_logs/{obj.partition_date}/{obj.task.id}.log"
    #     elif obj.task.project.engine == 'spark':
    #         if obj.execute_way=='retry':
    #             logs_path = f"/static/spark_logs/{obj.partition_date}/{obj.task.id}_retry.log"
    #         else:
    #             logs_path = f"/static/spark_logs/{obj.partition_date}/{obj.task.id}.log"
    #     else:
    #         return "日志文件不存在"
    #     return format_html('<a href="{}" target="_blank">查看日志</a>', logs_path)
        
    

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
        elif obj.complit_state == 4:
            return format_html('<span style="color: gray;">停止</span>')
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
                'name','config','template', 'engine','description'
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
            html += "<tr><th style='border:1px solid #ddd;padding:8px;text-align:left;'>字段名</th><th style='border:1px solid #ddd;padding:8px;text-align:left;'>类型</th><th style='border:1px solid #ddd;padding:8px;text-align:left;'>注释</th></tr>"
            for field in obj.meta_data:
                html += f"<tr><td style='border:1px solid #ddd;padding:8px;'>{field.get('name')}</td><td style='border:1px solid #ddd;padding:8px;'>{field.get('type')}</td><td style='border:1px solid #ddd;padding:8px;'>{field.get('comment')}</td></tr>"
            html += "</table>"
            return format_html(html)
       
        return "无字段信息"
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
        

@admin.register(TaskLogDependency)
class TaskLogDependencyAdmin(admin.ModelAdmin):
    list_display = ('id','name','data_source','description', 'created_at', 'updated_at')
    search_fields = ('data_source__name','description')
    list_filter = ('created_at',)
    list_display_links = ('id','data_source')
    ordering = ('-created_at','-updated_at')
    readonly_fields = ('created_at', 'updated_at')
    actions=[copy_data]
    fieldsets = (
        ('基础信息', {
            'fields': (
                'name','data_source','description', 'query_template'
            ) 
        }) ,
        (
            '系统信息', {
                'classes': ('collapse','readonly'),
                'fields': (
                    'created_at', 'updated_at'
                )
            }
        )
    )