from django.contrib import messages
from executors.extensions.datax.datax_plugin import DataXPluginManager
from loguru import logger
from django.urls import reverse
from django.contrib import messages
from concurrent.futures import ThreadPoolExecutor,as_completed
from .utils import exportor_admin_api
from django.http import HttpResponse
import csv
from io import StringIO

def copy_data(modeladmin, request, queryset):
    def _execute(obj):
        try:
            obj.id = None  # 重置主键
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

def generate_token(modeladmin, request, queryset):
    # 创建内存中的CSV文件
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="generated_tokens.csv"'
    
    writer = csv.writer(response)
    writer.writerow(['客户ID', '客户名称', '生成的Token'])
    
    for obj in queryset:
        token = exportor_admin_api.generate_token(obj.id)
        # 直接写入CSV而不保存到数据库
        writer.writerow([
            obj.id,
            obj.customer_name,
            token
        ])
        messages.success(request, f"已为 {obj.customer_name} 生成Token并导出")
    
    return response

generate_token.short_description = "生成Token并导出CSV"



