from django.shortcuts import render
from .models import Log
from django.db.models import Count
from rest_framework.views import APIView
from django.db.models import ExpressionWrapper, DurationField
from rest_framework.response import Response
from rest_framework import status
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from django_apscheduler.jobstores import DjangoJobStore
from django.db import models
from django.shortcuts import redirect
# from django.utils import timezone
from django.db.models import Q, Avg, F
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import psutil
from datetime import datetime
# from django.utils import timezone
from executors.alerts import AlertFactory
from loguru import logger
from executors.extensions import ManagerFactory
from executors.models import *
# 加载 .env 文件中的环境变量
load_dotenv()

scheduler = BackgroundScheduler({
    'apscheduler.executors.default': {
        'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
        'max_workers': int(os.environ.get('SCHEDULER_MAX_WORKERS', 100))
    }
})

scheduler.add_jobstore(DjangoJobStore(), "default")
scheduler.start()


def execute_datax_tasks(project,settings):
    try:
        alert=AlertFactory(project.notification)
        
        tasks = Task.objects.filter(is_active=True, project=project)
        manager = ManagerFactory(project.engine)
        manager = manager(tasks, settings=settings)
        start_time=datetime.now()
        alert.send_message(**{
            "name":project.name,
            "partition_date":manager.settings.get('partition_date'),
            'start_time':start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time':None,
            "content":f"同步开始"
        })
        
        manager.execute_tasks()
        end_time=datetime.now()
        alert.send_message(**{
            "name":project.name,
            "partition_date":manager.settings.get('partition_date'),
            'start_time':start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time':end_time.strftime('%Y-%m-%d %H:%M:%S'),
            "content":f"同步完成"
        })

    except Exception as e:
        logger.exception(e)
# 日志检查,并重试
def check_logs_and_retry():
    from executors.extensions.datax.datax_plugin import DataXPluginManager
    try:
        # 查询出当天失败日志
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        logs = Log.objects.filter(complit_state=0, created_at__gte=today_start)
        if not logs:
            logger.debug("No failed logs found today.")
            return
        logger.warning(f"Found {logs.count()} failed logs today.")
        DataXPluginManager.execute_retry(logs)
    except Exception as e:
        logger.exception(e)
# 日志检查
def check_logs():
    try:
        # 查询出当天失败日志
        today_start = datetime.now().date()
        # 根据项目每日统计情况，发送钉钉消息
        projects = Project.objects.filter(is_active=True)
        msg=f"### 每日项目执行情况统计 \n #### 日期：{today_start} \n | 项目名称 | 执行次数 | 失败次数 | 成功率 |\n | --- | --- | --- | --- |\n"
        for project in projects:
            # 获取今天的日志
            today_logs = Log.objects.filter(
                task__project=project,
                created_at__date=today_start,
            )
            # 统计执行次数和失败次数
            total_executions = today_logs.count()
            failed_executions = today_logs.filter(complit_state=0).count()
            # 计算成功率
            success_rate = (total_executions - failed_executions) / total_executions * 100 if total_executions > 0 else 0
            # 构建消息
            msg += f"| {project.name} | {total_executions} | {failed_executions} | {success_rate:.2f}% |\n"
        # 默认通知
        notification=Notification.objects.get(name='默认')
        AlertFactory(notification,'日常任务执行情况统计').send_custom_message(msg)
        
    except Exception as e:
        logger.exception(e)

# 备份除日志外的所有表数据为sql文件
def backup_data():
    from django.apps import apps 
    
    try:
        # 获取所有表名
        tables = apps.get_models()
        # 今天日期
        today = datetime.now().date()
        # 构建备份文件路径
        backup_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups', today.strftime('%Y-%m-%d'))
        os.makedirs(backup_dir, exist_ok=True)
        config=dict(ConfigItem.objects.all().values_list("key", "value"))
        # django项目绝对路径
        project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        backup_file = os.path.join(backup_dir, f'executors.json')
        # 执行备份命令
        python_path=config.get('PYTHON_BIN_PATH')
        os.system(f'{python_path} {project_path}/manage.py dumpdata --exclude executors.log --exclude executors.metadatatable > {backup_file}')
        logger.info(f'Backup of executors completed.')
    except Exception as e:
        logger.exception(e)


def init_scheduler_task():
    project=Project.objects.filter(is_active=True)
    for p in project:
        settings={
            "execute_way": "update",
            **p.config
        }
        if not p.config.get('cron'):
            logger.warning(f"Project {p.name} has no cron expression. Skipping.")
            continue
        trigger = CronTrigger.from_crontab(p.config['cron'])
        scheduler.add_job(
            execute_datax_tasks,
            args=[p, settings],
            trigger=trigger,
            id=f"datax_task_{p.name}",
            replace_existing=True,
            misfire_grace_time=3600  # 允许3600秒的容错时间
        )
        logger.info(f"DataX scheduler started successfully for project {p.name}")
    # 检查日志
    scheduler.add_job(
        check_logs,
        trigger=CronTrigger.from_crontab('30 9,12,23 * * *'),
        id='检查日志', 
        replace_existing=True,
        misfire_grace_time=3600
    )
    scheduler.add_job(
        update_all_tasks_metadata,
        # 每天执行一次
        trigger=CronTrigger.from_crontab('0 0,12 * * *'),
        id='更新元数据', 
        replace_existing=True,
        misfire_grace_time=3600
    )
    scheduler.add_job(
        check_logs_and_retry,
        # 每天执行一次
        trigger=CronTrigger.from_crontab('0 6,18,23 * * *'),
        id='重试任务', 
        replace_existing=True,
        misfire_grace_time=3600
    )
    scheduler.add_job(
        backup_data,
        # 每天执行一次
        trigger=CronTrigger.from_crontab('0 6 * * *'),
        id='备份数据', 
        replace_existing=True,
        misfire_grace_time=3600
    )
    logger.success("All DataX schedulers started successfully.")



class SystemMonitorAPI(APIView):
    def get(self, request):
        try:
            # 获取系统性能数据
            cpu_percent = psutil.cpu_percent(interval=1)
            mem = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            net_io = psutil.net_io_counters()
            
            data = {
                "timestamp": datetime.now().isoformat(),
                "cpu": {
                    "percent": cpu_percent,
                    "cores": psutil.cpu_count(logical=False)
                },
                "memory": {
                    "total": mem.total,
                    "used": mem.used,
                    "percent": mem.percent
                },
                "disk": {
                    "total": disk.total,
                    "used": disk.used,
                    "percent": disk.percent
                },
                "network": {
                    "sent": net_io.bytes_sent,
                    "recv": net_io.bytes_recv
                }
            }
            return Response(data)
        except Exception as e:
            return Response({"error": str(e)}, status=500)

class LogStatisticsAPI(APIView):
    def get(self, request):
        # 获取筛选参数
        project = request.GET.get('project')
        start_date = request.GET.get('start_date')
        end_date = request.GET.get('end_date')
        
        # 构建查询条件
        filters = Q()
        if project:
            filters &= Q(task__project__name=project)
        if start_date:
            try:
                start_date = datetime.strptime(start_date, '%Y-%m-%d')
                filters &= Q(created_at__gte=start_date)
            except ValueError:
                pass
        if end_date:
            try:
                end_date = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
                filters &= Q(created_at__lte=end_date)
            except ValueError:
                pass

        # 状态统计
        status_stats = Log.objects.filter(filters).values('executed_state').annotate(
            count=Count('id'),
            avg_time=Avg(F('end_time') - F('start_time'))
        ).order_by()
        # `end_time` datetime(6) DEFAULT NULL,
        # `start_time` datetime(6) DEFAULT NULL,
        # 项目统计
        project_stats = Log.objects.filter(filters).values('task__project__name').annotate(
            success=Count('id', filter=Q(executed_state='success')),
            fail=Count('id', filter=Q(executed_state='fail')),
            running=Count('id', filter=Q(executed_state='process')),
            bak=Count('id', filter=Q(executed_state='bak')),
            stop=Count('id', filter=Q(executed_state='stop')),
            total=Count('id'),
            avg_time=Avg(
                ExpressionWrapper(F('local_row_update_time_end') - F('local_row_update_time_start'), output_field=DurationField())
            )
        ).order_by('task__project__name')
        
        # 每日统计
        daily_stats = Log.objects.filter(filters).extra(
            select={'day': 'DATE(executors_log.updated_at)'}
        ).values('day').annotate(
            count=Count('id'),
            avg_time=Avg(
                ExpressionWrapper(F('local_row_update_time_end') - F('local_row_update_time_start'), output_field=DurationField())
            )
        ).order_by('day')

        # 处理状态统计结果
        status_data = {
            'success': 0,
            'fail': 0,
            'running': 0,
            'bak': 0,
            'stop': 0,
        }
        for stat in status_stats:
            if stat['executed_state'] == 'success':
                status_data['success'] = stat['count']
            elif stat['executed_state'] == 'fail':
                status_data['fail'] = stat['count']
            elif stat['executed_state'] == 'process':
                status_data['running'] = stat['count']
            elif stat['executed_state'] == 'bak':
                status_data['bak'] = stat['count']
            elif stat['executed_state'] == 'stop':
                status_data['stop'] = stat['count']

        data = {
            'status_stats': status_data,
            'project_stats': list(project_stats),
            'daily_stats': list(daily_stats),
        }
        return Response(data, status=status.HTTP_200_OK)
def log_statistics(request):
    from .models import Project
    projects = Project.objects.all()
    monitor_data =SystemMonitorAPI().get(request).data
    return render(request, 'log_statistics.html',  {
        'projects': projects,
        'monitor_data': monitor_data
    })

# 重定向admin路由
def redirect_to_admin(request):
    return redirect('/admin/')