from executors.extensions.datax.datax_plugin import DataXPlugin,DataXPluginManager
from executors.extensions.spark.spark_plugin import SparkPlugin,SparkPluginManager
from executors.extensions.base import BasePluginManager,BasePlugin
def ManagerFactory(engine):
    subclasses = BasePluginManager.__subclasses__()
    # 查找匹配名称的子类
    for cls in subclasses:
        if cls.name == engine:
            return cls

def PluginFactory(engine):
    subclasses = BasePlugin.__subclasses__()
    # 查找匹配名称的子类
    for cls in subclasses:
        if cls.name == engine:
            return cls

