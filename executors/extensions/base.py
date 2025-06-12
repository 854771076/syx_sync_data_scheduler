from abc import ABC, abstractmethod
from typing import List, Dict, Any
class settings:
    execute_way = None
    start_time = None
    end_time = None
    partition_date = None
    max_worker = None
    
class BasePluginManager(ABC):
    name = "base"
    @abstractmethod
    def __init__(self, task, config, settings):
        pass
    @abstractmethod
    def execute_tasks(self):
        '''
        批量执行任务
        '''
        pass
    @abstractmethod
    def execute_action(self):
        '''
        执行自定义任务
        '''
        pass
    @abstractmethod
    def execute_retry( logs):
        '''
        执行重试日志记录的原始任务
        '''
        pass
    @abstractmethod
    def execute_retry_new( logs):
        '''
        执行重试任务，重新生成配置
        '''
        pass
    @abstractmethod
    def generate_config(self):
        '''
        批量生成配置
        '''
        pass

class BasePlugin(ABC):
    name = "base"
    @abstractmethod
    def __init__(self, tasks, settings: Dict[str, Any] = {}):
        pass
    
    @abstractmethod
    def generate_config(self):
        """生成配置文件"""
        pass
    @abstractmethod
    def execute_retry(self, log):
        """执行重试任务"""
        pass
    @abstractmethod
    def execute_retry_new(self, log):
        """执行重试任务"""
        pass
    @abstractmethod
    def is_completed(self):
        """检查任务是否完成"""
        pass
    @abstractmethod
    def is_depend(self):
        '''
        检查是否有依赖
        '''
        pass
    @abstractmethod
    def execute_action(self):
        """执行任务"""
        pass
    @abstractmethod
    def pre_execute(self):
        """预执行"""
        pass
    @abstractmethod
    def execute(self):
        pass
