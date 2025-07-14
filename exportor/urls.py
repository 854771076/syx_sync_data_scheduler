from django.urls import path
from .views import TaskProgressAPI
from django.views.generic import TemplateView
from .views import TaskProgressView
urlpatterns = [
    path('api/task-progress/', TaskProgressAPI.as_view()),
    path('task-progress/', TaskProgressView.as_view()),
]