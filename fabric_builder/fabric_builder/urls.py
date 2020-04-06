"""fabric_builder URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import include, path
from django.conf.urls import url
from manager.models import Deployment, Template
from rest_framework import routers, serializers, viewsets

router = routers.DefaultRouter()
admin.site.site_header = "Arista Deployment Manager Admin"


# Serializers define the API representation.
class DeploymentSerializer(serializers.HyperlinkedModelSerializer):
    #devices = ParamsSerializer(source='deployment_to_device', many=True)
    class Meta:
        model = Deployment
        fields = ['id', 'name', 'last_deployment', 'current_deployment_var', 'last_deployed_var']

    
# ViewSets define the view behavior.
class DeploymentViewSet(viewsets.ModelViewSet):
    queryset = Deployment.objects.all()
    serializer_class = DeploymentSerializer


router.register(r'deployments', DeploymentViewSet)

# Serializers define the API representation.
class basicDeploymentSerializer(serializers.HyperlinkedModelSerializer):
    #devices = ParamsSerializer(source='deployment_to_device', many=True)
    class Meta:
        model = Deployment
        fields = ['id','name']
        read_only_fields = ['id','name']

# ViewSets define the view behavior.
class basicDeploymentViewSet(viewsets.ModelViewSet):
    #params = ParamsSerializer()
    queryset = Deployment.objects.all()
    serializer_class = basicDeploymentSerializer


router.register(r'basic_deployments', basicDeploymentViewSet)

# Serializers define the API representation.
class TemplateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Template
        fields = ['id', 'name', 'template','required']

# ViewSets define the view behavior.
class TemplateViewSet(viewsets.ModelViewSet):
    queryset = Template.objects.all()
    serializer_class = TemplateSerializer


router.register(r'templates', TemplateViewSet)



urlpatterns = [
    path('admin/', admin.site.urls),
    path('manager/', include('manager.urls')),
    url(r'^', include(router.urls)),
    url(r'^api-auth/', include('rest_framework.urls'))
]
