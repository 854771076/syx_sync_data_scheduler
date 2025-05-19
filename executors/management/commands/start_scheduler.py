from django.core.management.base import BaseCommand
from executors.extensions import ManagerFactory
from executors.models import Task,Project,Log,update_all_tasks_metadata
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from django_apscheduler.jobstores import DjangoJobStore
from functools import partial
# from django.utils import timezone
from datetime import datetime, timedelta
from executors.alerts import AlertFactory
from loguru import logger

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
# 日志检查
def check_logs():
    try:
        # 查询出当天失败日志
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        logs = Log.objects.filter(complit_state=0, created_at__gte=today_start)
        if not logs:
            logger.debug("No failed logs found today.")
            return
        logger.warning(f"Found {logs.count()} failed logs today.")
        for log in logs:
            # 发送钉钉消息
            alert=AlertFactory(log.task.project.notification)
            alert.send_message(**{
            "name":log.task.project.name,
            "partition_date":log.partition_date,
            'start_time':log.start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time':log.end_time.strftime('%Y-%m-%d %H:%M:%S'),
            "content":log.remark
        })

    except Exception as e:
        logger.exception(e)



class Command(BaseCommand):
    help = '''
    启动DataX任务调度器。
    --project 项目名称
    '''

    def handle(self, *args, **options):
        scheduler = BackgroundScheduler()
        scheduler.add_jobstore(DjangoJobStore(), "default")
        # 示例：每分钟执行任务
        # 指令传入定时任务的cron表达式
        
        if options.get('project'):
            project_name = options.get('project')
            project=Project.objects.filter(name=project_name,is_active=True)
        else:
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
            trigger=CronTrigger.from_crontab('*/5 * * * *'),  # 每5分钟执行一次
            id='检查日志', 
            replace_existing=True,
            misfire_grace_time=60
        )
        scheduler.add_job(
            update_all_tasks_metadata,
            # 每天执行一次
            trigger=CronTrigger.from_crontab('0 0 * * *'),
            id='更新元数据', 
            replace_existing=True,
            misfire_grace_time=60
        )
        scheduler.start()
        logger.success("All DataX schedulers started successfully.")
        
