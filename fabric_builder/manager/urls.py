from django.urls import path
from . import views


urlpatterns = [
    path('', views.index ),
    path('cvpInfo', views.cvpInfo),
    path('builderInfo', views.builderInfo),
    path('devices', views.devices),
    path('processXls', views.processXls),
    path('debug', views.debug),
    path('saveAndDeploy', views.saveAndDeploy),
    path('verifyLastDeployment', views.verifyLastDeployment),
    path('download', views.download),
    path('sync', views.sync),
    path('log', views.log),
    
]