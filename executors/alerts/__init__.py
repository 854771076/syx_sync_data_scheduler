from .dingding.dingdingbot import ding_ding_robot
from .base.base import BaseAlert




def AlertFactory(notification):
    from ..models import Log,Task,Notification,Project
    if notification is None:
        return BaseAlert(notification)
    if notification.is_active == False:
        return BaseAlert(notification)
    if notification.engine == Notification.Engine.DINGTALK:
        return ding_ding_robot(notification)
    else:
        raise NotImplementedError(f"Alert type {notification.engine} not implemented")