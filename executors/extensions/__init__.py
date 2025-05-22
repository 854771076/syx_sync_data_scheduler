from executors.extensions.datax.datax_plugin import DataXPlugin,DataXPluginManager
from executors.extensions.spark.spark_plugin import SparkPlugin,SparkPluginManager
# TODO 拓展引擎支持
def ManagerFactory(engine):
    if engine == 'datax':
        return DataXPluginManager
    elif engine == 'spark':
        return SparkPluginManager
    else:
        raise Exception("Unsupported engine: {}".format(engine))


