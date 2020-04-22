from django.contrib import admin
from manager.models import Deployment, Template, Global_Config
from django.contrib.postgres import fields
from django_json_widget.widgets import JSONEditorWidget
# Register your models here.
from django.db import models
from codemirror import CodeMirrorTextarea
from django.forms import TextInput, Textarea

codemirror_widget = CodeMirrorTextarea(
    mode="jinja2",
    theme="idea",
    config={
        'fixedGutter': True
    },
    custom_css=('/static/frontend/css/codemirror/custom.css',)
)


@admin.register(Template)
class TemplateAdmin(admin.ModelAdmin):

    formfield_overrides = {
        fields.JSONField: {'widget': JSONEditorWidget},
        models.TextField: {'widget': codemirror_widget}
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
    



    