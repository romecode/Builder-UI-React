from django.contrib import admin
from manager.models import Deployment, Template, Global_Config
from django.contrib.postgres import fields
from django_json_widget.widgets import JSONEditorWidget
# Register your models here.


@admin.register(Template)
class TemplateAdmin(admin.ModelAdmin):
    formfield_overrides = {
        fields.JSONField: {'widget': JSONEditorWidget},
    }


    
@admin.register(Deployment)
class DeploymentAdmin(admin.ModelAdmin):
    formfield_overrides = {
        fields.JSONField: {'widget': JSONEditorWidget},
    }
    
@admin.register(Global_Config)
class ConfigAdmin(admin.ModelAdmin):
    formfield_overrides = {
        fields.JSONField: {'widget': JSONEditorWidget},
    }
    



    