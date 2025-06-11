from .extensions.metadata.utils import DatabaseTableHandler
from django.contrib import messages
from executors.extensions.datax.datax_plugin import DataXPluginManager
from loguru import logger
from django import forms
from django.shortcuts import render
from django.urls import reverse
from django.contrib import messages
from django.http import HttpResponseRedirect
from django.urls import path
from .views import  init_scheduler_task
from concurrent.futures import ThreadPoolExecutor,as_completed
import subprocess
import os
import signal
import time
from datetime import datetime
import os
import signal
from executors.extensions import ManagerFactory
task_executor = ThreadPoolExecutor(max_workers=5)
def async_task_wrapper(func,request, *args, **kwargs):
    def wrapper():
        from .models import AsyncTaskStatus
        status = AsyncTaskStatus.objects.create(user_id=request.user.id, task_name=func.short_description, status='pending')
        
        try:
            result = func(*args, **kwargs)
            status.status = 'success'
            status.message = '任务执行成功'
        except Exception as e:
            status.status = 'failed'
            status.message = f'任务失败: {str(e)}'
            logger.error(f"异步任务执行失败: {str(e)}")
        finally:
            status.save()
            from django.db import connection
            connection.close()
    task_executor.submit(wrapper)


def init_scheduler(modeladmin, request, queryset):
    # 异步执行初始化调度器任务
    def _execute():
        try:
            init_scheduler_task()
            messages.success(request, "初始化调度器成功！")
        except Exception as e:
            logger.exception(e)
            messages.error(request, str(e))
    _execute()

init_scheduler.short_description = "初始化调度器"

# 复制数据action
def copy_data(modeladmin, request, queryset):
    def _execute(obj):
        try:
            obj.id = None  # 重置主键
            obj.created_at = datetime.now()  # 重置创建时间
            obj.updated_at = datetime.now()  # 重置更新时间
            obj.save()
            messages.success(request, "数据复制成功！")
        except Exception as e:
            logger.exception(e)
            messages.error(request, str(e))
    # 批量复制
    def _batch_copy():
        for obj in queryset:
            _execute(obj)
    _batch_copy()
    
copy_data.short_description = "复制数据"


# 传参数的action
def execute_datax_tasks_generate_config_update(modeladmin, request, queryset):
    try:
        settings={
            "execute_way": "update",
        }
        tasks={}
        for task in queryset:
            tasks[task.project.engine]=tasks.get(task.project.engine,[])
            tasks[task.project.engine].append(task)
        for engine in tasks:
            manage=ManagerFactory(engine)
            manage(tasks[engine], settings).generate_config()
            messages.success(request, f"生成{engine}任务配置成功！")
    except Exception as e:
        logger.exception(e)
        messages.error(request, str(e))

execute_datax_tasks_generate_config_update.short_description = "生成配置（更新）"
def execute_datax_tasks_generate_config_all(modeladmin, request, queryset):
    try:
        settings={
            "execute_way": "all",
        }
        tasks={}
        for task in queryset:
            tasks[task.project.engine]=tasks.get(task.project.engine,[])
            tasks[task.project.engine].append(task)
        for engine in tasks:
            manage=ManagerFactory(engine)
            manage(tasks[engine], settings).generate_config()
            messages.success(request, f"生成{engine}任务配置成功！")
    except Exception as e:
        logger.exception(e)
        messages.error(request, str(e))

execute_datax_tasks_generate_config_all.short_description = "生成配置（全量）"
# 执行json
def execute_datax_tasks_execute_json(modeladmin, request, queryset):
    try:
        settings={
            "execute_way": "action",
        }
        tasks={}
        for task in queryset:
            tasks[task.project.engine]=tasks.get(task.project.engine,[])
            tasks[task.project.engine].append(task)
        for engine in tasks:
            manage=ManagerFactory(engine)
            manage(tasks[engine], settings).execute_action()
            messages.success(request, f"执行{engine}任务配置成功！")

    except Exception as e:
        logger.exception(e)
        messages.error(request, str(e))

execute_datax_tasks_execute_json.short_description = "执行当前配置"

def execute_project_tasks_datax_all(modeladmin, request, queryset):
    try:
        for project in queryset:
            settings={
                "execute_way": "all",
                **project.config
            }
            manager_cls=ManagerFactory.get_manager(project)
            manager=manager_cls(project.task_set.all(),settings)
            manager.generate_config()
            manager.execute_action()
            messages.success(request, f"项目 {project.name} 运行成功！")
    except Exception as e:
        logger.exception(e)
        messages.error(request, str(e))

execute_project_tasks_datax_all.short_description = "执行项目任务（全量）"

def execute_project_tasks_datax_update(modeladmin, request, queryset):
    def _execute(project):
        try:
            
            for project in queryset:
                settings={
                    "execute_way": "update",
                    **project.config
                }
                manager_cls=ManagerFactory.get_manager(project)
                manager=manager_cls(project.task_set.all(),settings)
                manager.generate_config()
                manager.execute_action()
        except Exception as e:
            logger.exception(e)
    def _batch_execute():
        for project in queryset:
            _execute(project)
    _batch_execute.short_description = "执行项目任务（更新）"
    async_task_wrapper(_batch_execute,request)
    messages.success(request, "执行项目任务（更新）提交执行成功！")
execute_project_tasks_datax_update.short_description = "执行项目任务（更新）"
# 日志重试
def log_retry(modeladmin, request, queryset):
    def _execute():
        try:
            logs={}
            for log in queryset:
                if log.complit_state!=1:
                    logs[log.task.project.engine]=logs.get(log.task.project.engine,[])
                    logs[log.task.project.engine].append(log)
            for engine in logs:
                manage=ManagerFactory(engine)
                manage.execute_retry(logs[engine])
        except Exception as e:
            logger.exception(e)
   
    _execute.short_description = "日志重试"
    async_task_wrapper(_execute,request)
    messages.success(request, "日志重试提交执行成功！")
log_retry.short_description = "重新执行原任务"
def log_retry_new(modeladmin, request, queryset):
    def _execute():
        try:
            logs={}
            for log in queryset:
                if log.complit_state!=1:
                    logs[log.task.project.engine]=logs.get(log.task.project.engine,[])
                    logs[log.task.project.engine].append(log)
            for engine in logs:
                manage=ManagerFactory(engine)
                manage.execute_retry_new(logs[engine])
        except Exception as e:
            logger.exception(e)
   
    _execute.short_description = "日志重试"
    async_task_wrapper(_execute,request)
    messages.success(request, "日志重试提交执行成功！")
log_retry_new.short_description = "重新执行"
# 启用
def enable(modeladmin, request, queryset):
    queryset.update(is_active=True)
    messages.success(request, "已启用！")

enable.short_description = "启用"

# 禁用
def disable(modeladmin, request, queryset):
    queryset.update(is_active=False)
    messages.success(request, "已禁用！")
disable.short_description = "禁用"


class ConfigForm(forms.Form):
    execute_way = forms.ChoiceField(
        label='执行方式',
        choices=[('update', '增量更新'), ('all', '全量同步'), ('action', '执行JSON'),('other', '其他')]
    )
    start_time = forms.DateTimeField(
        label='开始时间',
        required=False,
        widget=forms.DateTimeInput(attrs={'type': 'datetime-local'})
    )
    end_time = forms.DateTimeField(
        label='结束时间',
        required=False,
        widget=forms.DateTimeInput(attrs={'type': 'datetime-local'})
    )
    partition_date = forms.CharField(
        label='分区日期',
        required=False,
        help_text='格式为 YYYYMMDD，留空则使用昨日日期'
    )

def execute_datax_tasks(modeladmin, request, queryset):
    # 存储选中对象的ID到session
    request.session['selected_ids'] = list(queryset.values_list('id', flat=True))
    # 跳转参数输入页面
    return HttpResponseRedirect(
        f'/admin/executors/task/configure/?ids={",".join(map(str, queryset.values_list("id", flat=True)))}'
    )
execute_datax_tasks.short_description = "执行任务"
def execute_project_datax_tasks(modeladmin, request, queryset):
    # 存储选中对象的ID到session
    request.session['selected_ids'] = list(queryset.values_list('id', flat=True))
    # 跳转参数输入页面
    return HttpResponseRedirect(
        f'/admin/executors/project/configure/?ids={",".join(map(str, queryset.values_list("id", flat=True)))}'
    )
execute_project_datax_tasks.short_description = "执行项目任务"

# 配置参数处理视图
def task_configure_view(request):
    from .models import Task
    def _execute():
        try: 
            # 从session获取选中ID
            selected_ids = request.session.get('selected_ids', [])
            queryset = Task.objects.filter(id__in=selected_ids)
            
            # 执行实际业务逻辑
            settings = {
                "execute_way": form.cleaned_data['execute_way'],
                "start_time": form.cleaned_data['start_time'],
                "end_time": form.cleaned_data['end_time'],
                "partition_date": form.cleaned_data['partition_date']
            }
            tasks={}
            for task in queryset:
                tasks[task.project.engine]=tasks.get(task.project.engine,[])
                tasks[task.project.engine].append(task)
            for engine in tasks:
                manage=ManagerFactory(engine)
                manage(tasks[engine], settings).execute_tasks()
        except Exception as e:
            logger.exception(e)
    _execute.short_description = "执行任务"

    if request.method == 'POST':
        form = ConfigForm(request.POST)
        if form.is_valid():
            # 异步执行任务
            async_task_wrapper(_execute,request)
            messages.success(request, "提交任务成功，执行中...")
            return HttpResponseRedirect('/admin/executors/task/')
        else:
            messages.error(request, "表单验证失败，请检查输入。")
    # 初始化表单
    form = ConfigForm()
    return render(request, 'admin/datax_config_form.html', {'form': form,'back_url': reverse('admin:executors_task_changelist')})

def project_configure_view(request):
    from.models import Project,Task
    def _execute_task(project, settings):
        settings.update(project.config)
        tasks=Task.objects.filter(project=project,is_active=True)
        manage=ManagerFactory(project.engine)
        manage(tasks, settings).execute_tasks()
    def _execute_project():
        try:
            selected_ids = request.session.get('selected_ids', [])
            queryset = Project.objects.filter(id__in=selected_ids)

            # 执行实际业务逻辑
            settings = {
                "execute_way": form.cleaned_data['execute_way'],
                "start_time": form.cleaned_data['start_time'],
                "end_time": form.cleaned_data['end_time'],
                "partition_date": form.cleaned_data['partition_date'],
            }
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(_execute_task, project, settings) for project in queryset]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"任务执行失败: {str(e)}")
        except Exception as e:
            logger.exception(e)
    _execute_project.short_description = "执行项目任务"
                
    if request.method == 'POST':
        form = ConfigForm(request.POST)
        if form.is_valid():
            # 从session获取选中ID
            async_task_wrapper(_execute_project,request)
            messages.success(request, "提交任务成功，执行中...")
            return HttpResponseRedirect('/admin/executors/project/')
        else:
            messages.error(request, "表单验证失败，请检查输入。")

    # 初始化表单
    form = ConfigForm()
    return render(request, 'admin/datax_config_form.html', {'form': form,'back_url': reverse('admin:executors_project_changelist')})

def create_table(modeladmin, request, queryset):
    for obj in queryset:
        try:
            obj.create_table()
            messages.success(request, f"表 {obj.name} 创建成功！")
        except Exception as e:
            logger.exception(e)
            messages.error(request, str(e))
create_table.short_description = "创建表"
# 一段时间后需要终止进程
def kill_process_group(pid, timeout=10):
    """终止整个进程组（Unix 系统）"""
    if os.name == 'nt':
        os.kill(pid, signal.SIGTERM)
    
    try:
        # 获取进程组 ID（与主进程 PID 相同）
        pgid = os.getpgid(pid)
        
        # 发送 SIGTERM 信号到整个进程组
        os.killpg(pgid, signal.SIGTERM)
        
        # 等待进程组终止
        time.sleep(timeout)
        
        # 检查进程组是否仍存在
        try:
            os.killpg(pgid, 0)  # 发送 0 信号仅用于检查进程是否存在
            # 进程组仍存在，发送 SIGKILL
            os.killpg(pgid, signal.SIGKILL)
            logger.info(f"进程组 {pgid} 已被强制终止")
        except OSError:
            logger.info(f"进程组 {pgid} 已成功终止")
        
        return True
    except Exception as e:
        logger.error(f"终止进程组时出错: {e}")
        return False
    
def clear_cache_columns(modeladmin, request, queryset):
    from .models import MetadataTable
    for obj in queryset:
        try:
            MetadataTable.objects.filter(data_source=obj.data_source,
            db_name=obj.source_db,
            name=obj.source_table).delete()
            MetadataTable.objects.filter(data_source=obj.data_target,
            db_name=obj.target_db,
            name=obj.target_table).delete()
            logger.debug(f"清除元数据缓存：{obj.data_source} {obj.source_db} {obj.source_table}")
            logger.debug(f"清除元数据缓存：{obj.data_target} {obj.target_db} {obj.target_table}")
        except Exception as e:
            logger.exception(e)
            messages.error(request, str(e))
clear_cache_columns.short_description = "清除元数据缓存"