from django.db import models
from django.contrib.postgres.fields import JSONField



# Create your models here.
def defaultTemplate():
    return {
      "device": [],
      "iterables": [],
      "variables": []
    }

class Template(models.Model):
    name = models.CharField(max_length=196)
    template = models.TextField(blank=False)
    required = JSONField(default=defaultTemplate)
    
    def __str__(self):
        return self.name
    
    
class Deployment(models.Model):
    name = models.CharField(max_length=196, unique = True)
    last_deployment = models.IntegerField(default=0)
    current_deployment_var = JSONField(default=dict, blank=True)
    last_deployed_var = JSONField(default=dict, blank=True)

    def __str__(self):
        return self.name

    
class Global_Config(models.Model):
    name = models.CharField(primary_key=True, max_length=196)
    params = JSONField(default=dict)
    def __str__(self):
        return self.name
    class Meta:
        verbose_name = 'Global Configuration'
        verbose_name_plural = 'Global Configurations'