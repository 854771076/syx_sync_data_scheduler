from .dingding.dingdingbot import ding_ding_robot
from .base.base import BaseAlert




def AlertFactory(notification):
    from ..models import Log,Task,Notification,Project
    if notification is None:
        return BaseAlert
    if notification.is_active == False:
        return BaseAlert
    if notification.engine == Notification.Engine.DINGTALK:
        return ding_ding_robot
    else:
        raise NotImplementedError(f"Alert type {notification.engine} not implemented")