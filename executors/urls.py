from django.urls import path
from . import views

urlpatterns = [
    path('api/log-statistics/', views.LogStatisticsAPI.as_view(), name='log_statistics_api'),
    path('log_statistics/', views.log_statistics, name='log_statistics'),
    path('', views.redirect_to_admin, name='redirect_to_admin'),
]