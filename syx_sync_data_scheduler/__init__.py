# -*- coding: utf-8 -*-
import pymysql
# 伪装版本号，解决版本兼容问题
pymysql.version_info = (1, 4, 13, "final", 0)
pymysql.install_as_MySQLdb()
 
from django.db.backends.mysql.base import CursorWrapper, DatabaseWrapper
from django.utils import asyncio
 
def create_cursor(self, name=None):
    # 解决连接失效的错误，
    # 检查连接的有效性，如果否则回收并且重创建
    self.connection.ping(reconnect=True)
    cursor = self.connection.cursor()
    return CursorWrapper(cursor)
 
DatabaseWrapper.create_cursor = asyncio.async_unsafe(create_cursor)