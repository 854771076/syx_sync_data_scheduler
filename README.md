# 使用文档

## 一、环境要求

### 1.1 系统要求

- 操作系统：Ubuntu 20.04+/CentOS 7+/windows
- Python：3.6.8+
- 数据库：MySQL 5.7+/PostgreSQL 12+
- Java：JDK 1.8+
- DataX：2023+

## 二、项目安装

### 2.1 源码获取

```bash
git clone https://github.com/yourrepo/syx_sync_data_scheduler.git
cd syx_sync_data_scheduler
```

### 2.2 虚拟环境

```bash
conda create -m syx_sync_data_scheduler python==3.6.8
conda activate syx_sync_data_scheduler
```

### 2.3 依赖安装

```bash
pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
```

## 三、配置说明

### 3.1 数据库配置(.env)

```ini
DEBUG=True
SECRET_KEY="django-xxx"
DB_HOST="xxx"
DB_PORT='xxx'
DB_NAME="syx_sync_data_scheduler"
DB_USER="xxx"
DB_PASSWORD="xxx"
HOST="0.0.0.0"
SCHEDULER_MAX_WORKERS='100'
PORT='8088'
```

### 

## 四、初始化项目

```bash
python manage.py makemigrations
python manage.py migrate
```

### 4.2 创建管理员

```bash
python manage.py createsuperuser
```

### 4.3 服务启动

```bash
python bin/manage_server.py start
python bin/manage_server.py stop
python bin/manage_server.py restart

```

## 五、功能使用

### 5.1 任务配置

...

### 5.2 调度器管理

...

## 六、监控告警

### 6.1 钉钉通知配置

...

### 6.2 日志监控

...

## 七、附录

### 7.1 目录结构

```plainText
syx_sync_data_scheduler/
├── executors/          # 核心模块
│   ├── admin.py        # 管理后台配置
│   ├── extensions/     # 数据同步插件

```

### 7.2 管理后台操作

...