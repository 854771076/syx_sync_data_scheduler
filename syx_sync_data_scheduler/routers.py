class ExportorRouter:
    """
    一个路由器，用于将 exportor 的数据库操作路由到 exportor
    """

    def db_for_read(self, model, **hints):
        """
        尝试将读操作路由到 exportor
        """
        if model._meta.app_label == 'exportor':
            return 'exportor_db'
        return None

    def db_for_write(self, model, **hints):
        """
        尝试将写操作路由到 exportor
        """
        if model._meta.app_label == 'exportor':
            return 'exportor_db'
        return None

    def allow_relation(self, obj1, obj2, **hints):
        """
        确保 exportor 中的模型之间可以建立关系。
        """
        if obj1._meta.app_label == 'exportor' and obj2._meta.app_label == 'exportor':
            return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        确保 exportor 的迁移操作只应用到 exportor
        """
        if app_label == 'exportor':
            return db == 'exportor_db'
        return None