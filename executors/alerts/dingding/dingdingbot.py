import time
import hmac
import hashlib
import base64
import urllib.parse
import requests
import json
import sys
import os
from ...models import Log,Task,Notification,Project,ConfigItem
from executors.alerts.base.base import BaseAlert

class ding_ding_robot(BaseAlert):

    def __init__(self, project: Project = None):
        config=dict(ConfigItem.objects.all().values_list("key", "value"))
        self.timestamp = str(round(time.time() * 1000))
        self.URL = config.get('DINGDING_URL')
        if config.get('ENV')=='prod':
            self.URL = self.URL.replace('https','http')
        
        self.token = project.notification.config.get('ACCESS_TOKEN')
        self.headers = {"Content-Type": "application/json"}
        self.secret = project.notification.config.get("SECRET")
        assert self.token, "ACCESS_TOKEN is required"
        assert self.secret, "SECRET is required"
        self.at=project.notification.config.get("AT")
        self.params = {}
        self.title=project.name
        self.template=project.notification.template

    def get_message(self, content):
        '''
        at 为手机号或all
        '''
        if self.at=='all':
            return {
                "msgtype": "actionCard",
                "actionCard": {"title": self.title, "text": content+f'\n@{self.at}'},
                "at": {
                    "atMobiles": [],
                    "atUserIds": ["user123"],
                    "isAtAll": True,
                },
            }
        elif self.at:
            return {
                "msgtype": "actionCard",
                "actionCard": {"title": self.title, "text": content+f'\n@{self.at}'},
                "at": {
                    "atMobiles": [self.at],
                    "atUserIds": ["user123"],
                    "isAtAll": False,
                },
            }
        else:
            return {
                "msgtype": "actionCard",
                "actionCard": {"title": self.title, "text": content},
                
            }
    def get_timestamp_sign(self):
        timestamp = str(round(time.time() * 1000))
        secret = self.secret
        secret_enc = secret.encode("utf-8")
        string_to_sign = "{}\n{}".format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode("utf-8")
        hmac_code = hmac.new(
            secret_enc, string_to_sign_enc, digestmod=hashlib.sha256
        ).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return {"timestamp": timestamp, "sign": sign}

    def send_message(self,**kwargs):
        """
        发送文本
        @param content: str, 文本内容
        """
        content=self.template.format(**kwargs)
        data = self.get_message(content)
        
        p = self.get_timestamp_sign()
        self.params["access_token"] = self.token
        self.params["timestamp"] = p.get("timestamp")
        self.params["sign"] = p.get("sign")
        
        resp=requests.post(
            url=self.URL,
            data=json.dumps(data),
            params=self.params,
            headers=self.headers,
        )
        return resp
    # 自定义模板发送
    def send_custom_message(self,content):
        """
        发送文本
        @param content: str, 文本内容
        """
        data = self.get_message(content)
        
        p = self.get_timestamp_sign()
        self.params["access_token"] = self.token
        self.params["timestamp"] = p.get("timestamp")
        self.params["sign"] = p.get("sign")
        
        resp=requests.post(
            url=self.URL,
            data=json.dumps(data),
            params=self.params,
            headers=self.headers,
        )
        return resp

if __name__ == "__main__":
    robot = ding_ding_robot()
    robot.send_message("这是一个测试",at='19122486487')
