from django.shortcuts import render
from .models import Log
from django.db.models import Count
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from django_apscheduler.jobstores import DjangoJobStore
from django.db import models
from django.shortcuts import redirect
from django.utils import timezone
from django.db.models import Q, Avg, F
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
# ... 原有导入保持不变...
import psutil
from datetime import datetime
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
                start_date = timezone.datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.get_current_timezone())
                filters &= Q(created_at__gte=start_date)
            except ValueError:
                pass
        if end_date:
            try:
                end_date = timezone.datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.get_current_timezone()) + timezone.timedelta(days=1)
                filters &= Q(created_at__lte=end_date)
            except ValueError:
                pass

        # 状态统计
        status_stats = Log.objects.filter(filters).values('executed_state').annotate(
            count=Count('id'),
            avg_time=Avg(F('end_time') - F('start_time'))
        ).order_by()

        # 项目统计
        project_stats = Log.objects.filter(filters).values('task__project__name').annotate(
            success=Count('id', filter=Q(executed_state='success')),
            fail=Count('id', filter=Q(executed_state='fail')),
            running=Count('id', filter=Q(executed_state='process')),
            bak=Count('id', filter=Q(executed_state='bak')),
            total=Count('id'),
            avg_time=Avg(F('end_time') - F('start_time'))
        ).order_by('task__project__name')

        # 每日统计
        daily_stats = Log.objects.filter(filters).extra(
            select={'day': 'DATE(executors_log.updated_at)'}
        ).values('day').annotate(
            count=Count('id'),
            avg_time=Avg(F('end_time') - F('start_time'))
        ).order_by('day')

        # 处理状态统计结果
        status_data = {
            'success': 0,
            'fail': 0,
            'running': 0,
            'bak': 0
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