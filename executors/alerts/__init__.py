from .dingding.dingdingbot import ding_ding_robot
from .base.base import BaseAlert




def AlertFactory(notification,*args,**kwargs):
    subclasses = BaseAlert.__subclasses__()
    # 查找匹配名称的子类
    for cls in subclasses:
        if cls.name == notification.engine:
            return cls(notification,*args,**kwargs)