from executors.extensions.datax.datax_plugin import DataXPlugin,DataXPluginManager
# TODO 拓展引擎支持
def ManagerFactory(engine):
    if engine == 'datax':
        return DataXPluginManager
    else:
        raise Exception("Unsupported engine: {}".format(engine))


