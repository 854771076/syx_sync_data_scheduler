from .extensions.metadata.utils import DatabaseTableHandler
from django.contrib import messages
from executors.extensions.datax.datax_plugin import DataXPluginManager
from loguru import logger
from django import forms
from django.shortcuts import render
from django.contrib import messages
from django.http import HttpResponseRedirect
from django.urls import path

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
