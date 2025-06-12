from abc import ABC, abstractmethod
from typing import Dict, Any

class BaseDBUtil(ABC):
    """数据库工具基类"""
    name = "base"  # 子类需要覆盖这个属性
    
    @abstractmethod
    def get_client(config: Dict[str, Any], datasource: Any):
        """获取数据库连接"""
        pass
    
    @abstractmethod 
    def get_table_schema(client, database_name: str, table_name: str):
        """获取表结构"""
        pass
