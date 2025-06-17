from django.urls import path
from . import views
from django.contrib.staticfiles.storage import staticfiles_storage
from django.views.generic.base import RedirectView
urlpatterns = [
    path('api/log-statistics/', views.LogStatisticsAPI.as_view(), name='log_statistics_api'),
    path('log_statistics/', views.log_statistics, name='log_statistics'),
    path('', views.redirect_to_admin, name='redirect_to_admin'),
    path('api/system-monitor/', views.SystemMonitorAPI.as_view(), name='system-monitor'),
    path('favicon.ico', RedirectView.as_view(url=staticfiles_storage.url('favicon.ico')))




]