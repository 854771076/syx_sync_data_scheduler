from django.test import TestCase
from .extensions.datax.datax_plugin import DataXPlugin
from .models import Task,Log
# Create your tests here.
from executors.models import Task
from executors.extensions.datax.datax_plugin import DataXPlugin  ,config
task=Task.objects.get(pk=1)
datax=DataXPlugin(task)
datax.generate_config()