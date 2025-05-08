#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys
from dotenv import load_dotenv
# 加载 .env 文件中的环境变量
load_dotenv()



def main():
    """Run administrative tasks."""
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "syx_sync_data_scheduler.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    if len(sys.argv) == 2 and sys.argv[1] == 'runserver':
        sys.argv.append(os.environ.get('PORT', '8088'))
    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
