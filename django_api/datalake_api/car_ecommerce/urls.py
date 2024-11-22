from django.urls import path
from . import views
from .views import TableDataView

urlpatterns = [
    path('data/<str:table_name>/', TableDataView.as_view(), name='table_data'),
]
