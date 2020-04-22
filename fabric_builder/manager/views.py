from django.shortcuts import render
from django.http import JsonResponse, HttpResponse
import json
from django.views.decorators.csrf import csrf_exempt
import xlrd
import xlwt
from django.core.cache import cache
from manager.models import Global_Config
import urllib3
import threading
from manager import builder
import socket
import json
from cvprac.cvp_client import CvpClient
from cvprac.cvp_client_errors import CvpClientError
import os

class CVP():
    def __init__(self):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # to supress the warnings for https
        self.cvprac = CvpClient()
        config = Global_Config.objects.get(name='master').params
        #self.cvprac.connect(config['server'], config['user'], config['password'])
        
    def connect(self):
        try:
            config = Global_Config.objects.get(name='master').params
            self.cvprac.connect(config['server'], config['user'], config['password'])
            return True
        except Exception as e:
            print(e)
            return False
            
    def status(self):
        try:
            self.cvprac.api.get_cvp_info()
            return True
        except Exception as e:
            print(e)
            if self.connect():
                return True
            else:
                return False
            
    def call(self, endpoint):
        if self.status():
            try:
                return getattr(self.cvprac.api, endpoint)()
            except CvpClientError as e:
                return False
        else:
            return False
        
CVPINSTANCE = CVP()

def send_cmd(cmd):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', 54999))
    builder.send_message(s,cmd)
    return builder.read_message(s)


def cvpInfo(request):
    #===========================================================================
    # cvprac = CvpClient()
    # try:
    #     config = Global_Config.objects.get(name='master').params
    #     cvprac.connect(config['server'], config['user'], config['password'])
    #     cvp_status = {'status': cvprac.api.get_cvp_info()['version'],'name':'CVP'}
    # except:
    #     cvp_status = {'status': 'Disconnected','name':'CVP'}
    #===========================================================================
    try:
        cvp_status = {'status': CVPINSTANCE.call('get_cvp_info')['version'],'name':'CVP'}
    except:
        cvp_status = {'status': 'Disconnected','name':'CVP'}
    
    return JsonResponse(cvp_status)

def builderInfo(request):
    try:
        response = send_cmd({"action":"status"})
        status = "Online"
    except Exception as e: 
        response = e
        status = "Disconnected"
    builder_status = {'status': status,'name':'Builder'}
    return JsonResponse(builder_status)

def devices(request):
    try:
        devices = CVPINSTANCE.call('get_inventory')
        devices.sort(key=lambda item: item['hostname'])
    except Exception as e:
        devices = []
    return JsonResponse(devices, safe=False)

def containers(request):
    try:
        containers = [c['name'] for c in CVPINSTANCE.call('get_containers')['data']]
        containers.sort()
    except:
        containers = []
    return JsonResponse(containers, safe=False)

def leafs(request):
    try:
        devices = CVPINSTANCE.call('get_inventory')
        devices.sort(key=lambda item: item['hostname'])
        device = [d for d in devices if d not in config['spines']]
    except:
        devices = []
    return JsonResponse(devices, safe=False)

def spines(request):
    config = Global_Config.objects.get(name='master').params
    spines = config['spines']
    
    return JsonResponse(spines, safe=False)

def index(request):
    
    return render(request, 'frontend/index.html')


    
@csrf_exempt
def debug(request):
    if request.method == 'POST':
        _json = json.loads(request.body)
        _json['action'] = 'debug'
        send_cmd(_json)
    return JsonResponse({'status':'success'})

@csrf_exempt
def saveAndDeploy(request):
    if request.method == 'POST':
        _json = json.loads(request.body)
        _json['action'] = 'saveAndDeploy'
        send_cmd(_json)
    return JsonResponse({'status':'success'})

@csrf_exempt
def verifyLastDeployment(request):
    if request.method == 'POST':
        _json = json.loads(request.body)
        _json['action'] = 'verifyLastDeployment'
        send_cmd(_json)
    return JsonResponse({'status':'success'})  

@csrf_exempt
def sync(request):
    if request.method == 'POST':
        _json = json.loads(request.body)
        _json['action'] = 'sync'
        send_cmd(_json)
    return JsonResponse({'status':'success'})       

def log(request):
    module_dir = os.path.dirname(__file__)
    try:
        f = open(module_dir + "/fabric_builder_log.txt",'r')
        text = f.read()
        f.close()
    except:
        text = "Error reading log"
    return HttpResponse(text)

class Echo:
    """An object that implements just the write method of the file-like
    interface.
    """
    def write(self, value):
        """Write the value by returning it, instead of storing in a buffer."""
        return value

@csrf_exempt
def download(request):
    # if this is a POST request we need to process the form data
    toReturn = {}
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        import io
        f = io.BytesIO()
        wb = xlwt.Workbook()
        
        r = 0
        c = 0
        data = json.loads(request.body)
        ws = wb.add_sheet(data['meta']['name'])

        for header in data['columns']:
            ws.write(r, c, header['title'])
            c+=1
            
        r+=1
        c=0
        
        for row in data['data']:
            for _data in row:
                ws.write(r, c, _data)
                c+=1
            r+=1
            c=0

        
    """A view that streams a large CSV file."""
    # Generate a sequence of rows. The range is based on the maximum number of
    # rows that can be handled by a single sheet in most spreadsheet
    # applications.
    wb.save(f) 
    # Create the HttpResponse object with the appropriate PDF headers.
    response = HttpResponse(content_type='application/msexcel')
    response['Content-Disposition'] = 'attachment; filename="builder.xls"'

    # Get the value of the BytesIO buffer and write it to the response.
    xls = f.getvalue()
    f.close()
    response.write(xls)
    return response

@csrf_exempt
def processXls(request):
    # if this is a POST request we need to process the form data
    toReturn = {}
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:

        with xlrd.open_workbook(file_contents=request.FILES['file'].read()) as f:
            
            
            
            
            def format(v):
                if type(v) == float:
                    return {'type':'text','title':str(int(val)), 'width':200 }
                else:
                    return {'type':'text','title':v.lower(), 'width':200 }
                
            

            for n in range(0, f.nsheets):
                _sheet=f.sheet_by_index(n)
                _sheet.cell_value(0,0)
                toReturn[_sheet.name] = {'data':[],'columns':_sheet.row_values(0)}
                for row in range(1, _sheet.nrows):
                    row = _sheet.row_values(row)
                    row = list(map(lambda x:str(int(x)) if type(x) == float else x, row))
                    toReturn[_sheet.name]['data'].append(row)
    return JsonResponse(toReturn)