import requests
# 读取设置
from django.conf import settings
# 读取 EXPORTOR_ADMIN_TOKEN
EXPORTOR_ADMIN_TOKEN = settings.EXPORTOR_ADMIN_TOKEN
EXPORTOR_ADMIN_URL = settings.EXPORTOR_ADMIN_URL
class ExportorAdminApi:
    def __init__(self, token, url):
        self.token = token
        self.url = url
        self.headers = {
            'Authorization': f'Bearer {self.token}',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    def generate_token(self,clientId):
        params={
            'clientId':clientId
        }
        response = self.session.get(f'{self.url}/data/admin/task/generate/key',params=params)
        if response.status_code == 200:
            if 'Authorization' in response.json():
                return response.json()['Authorization']
            else:
                raise Exception(f'生成 token 失败: {response.status_code} {response.text}')
        else:
            raise Exception(f'生成 token 失败: {response.status_code} {response.text}')
    def get_task_progress(self,clientId):
        if clientId:
            params={
                'clientId':clientId
            }
            response = self.session.get(f'{self.url}/data/admin/task/progress',params=params)
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f'获取任务失败: {response.status_code} {response.text}')
        else:
            response = self.session.get(f'{self.url}/data/admin/task/progress/all',params=params)
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f'获取任务失败: {response.status_code} {response.text}')
    def modify_task(self,clientId):
        params={
            'clientId':clientId,
        }
        response = self.session.get(f'{self.url}/data/admin/task/client/modify',params=params)
        if response.status_code == 200:

            return response.json()
        else:
            raise Exception(f'修改任务失败: {response.status_code} {response.text}')

exportor_admin_api = ExportorAdminApi(EXPORTOR_ADMIN_TOKEN, EXPORTOR_ADMIN_URL)
