from django.apps import AppConfig


class ExecutorsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "executors"
    verbose_name = "执行器管理"
    # def ready(self):
    #     from .models import update_all_tasks_metadata
    #     # 在应用启动时执行同步表元数据的操作
    #     update_all_tasks_metadata()