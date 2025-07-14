from django.shortcuts import render
from .utils import exportor_admin_api
from .models import ExportCustomer
from django.views.generic import TemplateView
from rest_framework.views import APIView
from rest_framework.response import Response
class TaskProgressView(TemplateView):
    template_name = 'task_progress.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['customers'] = ExportCustomer.objects.all()
        return context
class TaskProgressAPI(APIView):
    def get(self, request):
        client_id = request.GET.get('client_id')
        try:
            progress_data = exportor_admin_api.get_task_progress(client_id)
            return Response(progress_data)
        except Exception as e:
            return Response({
                'code': 500,
                'msg': str(e)
            }, status=500)