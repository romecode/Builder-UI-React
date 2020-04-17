import urllib3
import re
import sys
import datetime
import xlrd
import socket
import json
from manager.extensions import SwitchBase
from collections import OrderedDict, defaultdict
from types import SimpleNamespace


LOGGER = None
CVP = None
DEBUG = False
MANAGER = {}

from cvprac.cvp_client import CvpClient
from cvprac.cvp_client_errors import CvpClientError
from cvprac.cvp_client_errors import CvpApiError

import os
import django
from django.conf import settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'fabric_builder.settings')
django.setup()
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

from manager.models import Deployment, Template, Global_Config
from django.db import Error as dbError
        
           
class Log:
    def __init__(self):
        fabric_builder_log = open(MODULE_DIR + '/fabric_builder_log.txt', 'w')
        fabric_builder_log.close()
        
    def log(self,string, stamp = True):
        fabric_builder_log = open(MODULE_DIR + '/fabric_builder_log.txt', 'a')
        fabric_builder_log_complete = open(MODULE_DIR + '/fabric_builder_log_complete.txt', 'a')
        
        string = "{0}: {1}\n".format( datetime.datetime.now().strftime('%a %b %d %H:%M'), string )
        no_stamp = "{0}\n".format( string )
        sys.stderr.write(string)
        fabric_builder_log.write(string)
        fabric_builder_log.close()
        
        fabric_builder_log_complete.write(string)
        fabric_builder_log_complete.close()
        
    def log_noTs(self, string):
        fabric_builder_log = open(MODULE_DIR + '/fabric_builder_log.txt', 'a')
        fabric_builder_log_complete = open(MODULE_DIR + '/fabric_builder_log_complete.txt', 'a')
        
        string = "{0}\n".format( string )
        sys.stderr.write(string)
        fabric_builder_log.write(string)
        fabric_builder_log.close()
        
        fabric_builder_log_complete.write(string)
        fabric_builder_log_complete.close()


class Cvp:
    def __init__(self):

        self.cvprac = None
        self.containerTree = {}
        self.devices = {}
        self.host_to_device = {}
        self.containers = {}
        self.configlets = {}
        
        self.cvprac = CvpClient()
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # to supress the warnings for https
        self.cvprac.connect(searchConfig('server'), searchConfig('user'), searchConfig('password'))
        LOGGER.log("Successfully authenticated to CVP")
        
    def loadContainers(self):
        LOGGER.log_noTs("-loading CVP containers: please wait...")
        self.configlets = {item['name'].lower():item for item in self.cvprac.api.get_containers()['data']}

    def loadConfiglets(self):
        LOGGER.log_noTs("-loading CVP configlets: please wait...")
        self.configlets = {item['name'].lower():item for item in self.cvprac.api.get_configlets()['data']}
        
    def loadInventory(self):
        LOGGER.log_noTs("-loading CVP inventory: please wait...")
        for device in self.cvprac.api.get_inventory():
            #if device['parentContainerId'] != "undefined_container":
            serialNumber = device['serialNumber']
            host = device['hostname'].lower()
            device['configlets'] = {}
            self.devices[serialNumber] = device
            self.host_to_device[host] = self.devices[serialNumber]
        
    def loadDeviceConfiglets(self, serialNumber):
        if serialNumber in list(self.devices.keys()):
            device = self.devices[serialNumber]
            host = device['hostname']
            device_configlets = self.cvprac.api.get_configlets_by_device_id(device['systemMacAddress'])
            device['configlets'] = {item['name'].lower():item for item in device_configlets}
            return True
        return False
    
    def getBySerial(self, serialNumber):
        return self.devices.get(serialNumber, None)
    
    def getByHostname(self, hostname):
        return self.host_to_device.get(hostname.lower(), None)
    
    def getContainerByName(self, name):
        return self.containers.get(name.lower(), None)
    
    def getContainerDevices(self, containerName, follow = False):
        containerName = containerName.lower()
        tree = [containerName] + self.containerTree[containerName] if follow else [containerName]
        return [device for device in self.devices.values() if device['containerName'].lower() in tree]
    
    def fetchDevices(self, search):
        search = search if type(search) == list else [search]
        devices = []
        for _search in search:
            
            try:
                device = CVP.getBySerial(_search) or CVP.getByHostname(_search)
                if device:
                    devices.append(device)
                    continue
            except KeyError as e:
                LOGGER.log_noTs("Could not find {0}".format(_search))
        return devices
    
    def createConfiglet(self, configlet_name, configlet_content):
        # Configlet doesn't exist let's create one
        LOGGER.log_noTs("--creating configlet {0}; please wait...".format(configlet_name))
        self.cvprac.api.add_configlet(configlet_name, configlet_content)
        return self.cvprac.api.get_configlet_by_name(configlet_name)
                
        
    def updateConfiglet(self, configlet, new_configlet_content):
        # Configlet does exist, let's update the content only if not the same (avoid empty task)
        configlet_name = configlet['name']
        LOGGER.log_noTs("--found configlet {0}".format(configlet_name))

        if configlet['config'] != new_configlet_content:
            LOGGER.log_noTs("---updating configlet {0}; please wait...".format(configlet_name))
            self.cvprac.api.update_configlet(new_configlet_content, configlet['key'], configlet_name)
        else:
            LOGGER.log_noTs("---nothing to do".format(configlet_name))
        return self.cvprac.api.get_configlet_by_name(configlet_name)
                
    def deployDevice(self, device, container, configlets_to_deploy):
        try:
            ids = self.cvprac.api.deploy_device(device.cvp, container, configlets_to_deploy)
        except CvpApiError as e:
            LOGGER.log_noTs("---deploying device {0}: failed, could not get task id from CVP".format(device.hostname))
        else:
            ids = ','.join(map(str, ids['data']['taskIds']))
            LOGGER.log_noTs("---deploying device {0}: {1} to {2} container".format(device.hostname, device.mgmt_ip, device.container))
            LOGGER.log_noTs("---CREATED TASKS {0}".format(ids))
            
    def applyConfiglets(self, to, configlets):
        app_name = "CVP Configlet Builder"
        to = to if type(to) == list else [to]
        configlets = configlets if type(configlets) == list else [configlets]
        toContainer = None
        toDevice = None
        
        # dest is a container, sn. or hostname string
        for dest in to:
            toContainer = self.getContainerByName(dest)
            if toContainer:
                LOGGER.log_noTs("---applying configlets to {0}; please wait...".format(toContainer.name))
                _result = self.cvprac.api.apply_configlets_to_container(app_name, toContainer, configlets)
                dest = toContainer
            else:
                #apply to device
                toDevice = getBySerial(dest)
                hostname = searchSource('hostname', toDevice, 'no_hostname_error')
                LOGGER.log_noTs("---applying configlets to {0}; please wait...".format(dest))
                _result = self.cvprac.api.apply_configlets_to_device(app_name, toDevice.cvp, configlets) if toDevice.cvp else None
                
            if not (toDevice or toContainer):
                errorOn = [_conf['name'] for _conf in configlets]
                LOGGER.log_noTs("---failed to push {0}; {1} not found".format(','.join(errorOn), dest))
            elif _result and _result['data']['status'] == 'success':
                
                LOGGER.log_noTs("---CREATED TASKS {0}".format(','.join(map(str, _result['data']['taskIds']))))
                
        return None
    
        
class Task:
    def __init__(self, device = None, mode = None):
        self.device = device
        self.mode = mode
    
    def verify(self, ignoreDeleted = False, ignoreNotAssigned = False, ignoreNotAssigned_Mismatched = False):
        error = 0

        LOGGER.log_noTs('')
        LOGGER.log_noTs("******* {0} / {1} *******".format(self.device.hostname, self.device.serialNumber))
        
        for name, configlet in self.device.to_deploy:
            
            new_configlet_content, compile_info = configlet.compile(self.device)
            name_lower = name.lower()
            
            exists = searchSource(name_lower, CVP.configlets, False)
            
            if exists:
                match = exists['config'] == new_configlet_content
                assigned = searchSource(name_lower, searchSource('configlets', self.device.cvp), False)
            else:
                match = False
                assigned = False
            
            if not exists:
                if ignoreDeleted:
                    continue
                LOGGER.log_noTs("Configlet does not exist: {0}".format(name))
                error += 1
            elif not assigned and match:
                if ignoreNotAssigned:
                    continue
                LOGGER.log_noTs("Configlet not assigned: {0}".format(name))
                error += 1
            elif not assigned and not match:
                if ignoreNotAssigned_Mismatched:
                    continue
                LOGGER.log_noTs("Configlet does not match and is not assigned: {0}".format(name))
                LOGGER.log_noTs("compilation log:")
                LOGGER.log_noTs('\n'.join(compile_info) if compile_info else "-no messages")
                LOGGER.log_noTs("expected:")
                LOGGER.log_noTs('-'*50)
                LOGGER.log_noTs(new_configlet_content)
                LOGGER.log_noTs('-'*50)
                LOGGER.log_noTs("actual:")
                LOGGER.log_noTs('-'*50)
                LOGGER.log_noTs(exists['config'])
                LOGGER.log_noTs('-'*50)
                error += 1
            elif assigned and not match:
                LOGGER.log_noTs("Configlet does not match: {0}".format(name))
                LOGGER.log_noTs("compilation log:")
                LOGGER.log_noTs('\n'.join(compile_info) if compile_info else "-no messages")
                LOGGER.log_noTs("expected:")
                LOGGER.log_noTs('-'*50)
                LOGGER.log_noTs(new_configlet_content)
                LOGGER.log_noTs('-'*50)
                LOGGER.log_noTs("actual:")
                LOGGER.log_noTs('-'*50)
                LOGGER.log_noTs(exists['config'])
                LOGGER.log_noTs('-'*50)
                error += 1
        if not error:
            LOGGER.log_noTs("Device is consistent with CVP")        
        
        LOGGER.log_noTs('')
        
        self.device.to_deploy = []
        
        return not bool(error)
    
    # the task finally figures out what to assign and compile
    def execute(self):
        configlet_keys = []
        # apply_configlets = searchConfig('apply_configlets')
        
        def pushToCvp():
            container = searchSource('container', self.device)
            
            if self.device.cvp['parentContainerId'] == "undefined_container" and container:
                CVP.deployDevice(self.device, container, configlet_keys)
            elif self.device.cvp['parentContainerId'] == "undefined_container" and not container:
                LOGGER.log("---cannot deploy {0}; non-provisioned device with no destination container defined".format(self.device.hostname))
            else:
                CVP.applyConfiglets(self.device.serialNumber, configlet_keys) 
                
        # DAY1 and DAY2 EXECUTION HAPPENS HERE
        LOGGER.log_noTs('')
        LOGGER.log_noTs("******* {0} / {1} *******".format(self.device.hostname.upper(), self.device.serialNumber.upper()))
        
        for name, configlet in self.device.to_deploy:
            new_configlet_content, compile_info = configlet.compile(self.device)
            LOGGER.log_noTs('CONFIGLET NAME: '+ name)

            LOGGER.log_noTs("compilation log:")
            LOGGER.log_noTs('\n'.join(compile_info) if compile_info else "-no messages")
            LOGGER.log_noTs("compiled:")
            LOGGER.log_noTs('-'*50)
            LOGGER.log_noTs(new_configlet_content)
            LOGGER.log_noTs('-'*50)
            
            if not DEBUG:
                name_lower = name.lower()
                
                exists = searchSource(name_lower, CVP.configlets, False)
                assigned = searchSource(name_lower, searchSource('configlets', self.device.cvp), False)

                configlet = None
                
                if not exists:
                    configlet_keys.append(CVP.createConfiglet(name, new_configlet_content))
                elif not assigned:
                    configlet_keys.append(CVP.updateConfiglet(exists, new_configlet_content))
                else:
                    CVP.updateConfiglet(exists, new_configlet_content)
                    
                    LOGGER.log_noTs('')
                    LOGGER.log_noTs('')
                    LOGGER.log_noTs('')

        if configlet_keys and self.device.cvp:
            pushToCvp()
            
        LOGGER.log_noTs('')
        LOGGER.log_noTs('')
        LOGGER.log_noTs('')
        LOGGER.log_noTs('')
        LOGGER.log_noTs('')
        LOGGER.log_noTs('')
          
        self.device.to_deploy = []


class Switch(SwitchBase):

    def __init__(self, params={}, cvpDevice={}):
        super(Switch, self).__init__()
        # list to hold leaf compiled spine underlay interface init
        self.underlay_inject = []
        self.to_deploy = []
        self.cvp = {}

        self.MANAGER = MANAGER

        for k, v in params.items():
            setattr(self, k, v)
        
        self.hostname = searchSource('hostname', self) or searchSource('hostname', cvpDevice)
        self.serialNumber = searchSource('serialNumber', self) or searchSource('serialNumber', cvpDevice)
        
        LOGGER.log_noTs("-loading {0}".format(self.hostname or self.serialNumber))
        
        MANAGER.DEVICES[self.serialNumber] = self
        MANAGER.HOST_TO_DEVICE[self.hostname] = self
        
        if self.serialNumber in searchConfig('spines'):
            self.role = 'spine'
            MANAGER.SPINES.append(self)
        else:
            self.role = 'leaf'
    
    def loadCVPRecord(self):
        self.cvp = searchSource(self.serialNumber, CVP.devices, {})
        LOGGER.log_noTs("-loading CVP record: {1}".format(self.hostname, 'success' if self.cvp else 'not found'))
        if not self.cvp:
            return False
        return True
        
    def loadCVPConfiglets(self):
        success = CVP.loadDeviceConfiglets(self.serialNumber)
        LOGGER.log_noTs("-loading CVP configlets: {1}".format(self.hostname, 'success' if success else 'not found'))
    
    def searchConfig(self, key):
        return searchConfig(key)
            
    def assign_configlet(self, template):
        # TODO: MAKE HANDLE LIST LOOKUPS, RIGHT NOW ONLY WORKS FOR ONE CONTAINER OR ONE DEVICE i.e. USELESS
        exception = getattr(template, "skip_container", None)
        if exception == self.role:
            return None
        exception = getattr(template, "skip_device", None)
        if exception == self.serialNumber:
            return None
        configlet_name = "DEPLOYMENT:{0} HOST:{1} SN:{2} TEMPLATE:{3}".format(searchConfig('name'), self.hostname.upper(), self.serialNumber.upper(), template.name)
        self.to_deploy.append((configlet_name, template))
     
    def compile_configlet(self, template):
        # TODO: MAKE HANDLE LIST LOOKUPS, RIGHT NOW ONLY WORKS FOR ONE CONTAINER OR ONE DEVICE i.e. USELESS
        exception = getattr(template, "skip_container", None)
        if exception == self.role:
            return ('',[])
        exception = getattr(template, "skip_device", None)
        if exception == self.serialNumber:
            return ('',[])
        return template.compile(self)    
    
    
    
class Math:
    def __init__(self, start, op, qty):
        self.iter = None
        self.counter = None
        
        if type(start) == list:
            self.iter = iter(start)
        else:  
            self.counter = int(start)
        
        if op == '+':
            self.do = self.increment
            self.qty = int(qty) if qty else 1
        elif op == '++':
            self.do = self.increment
            self.qty = int(qty) if qty else 10
        elif op == '*':
            self.do = self.multiply
            self.qty = int(qty) if qty else 1
    
    def current(self):
        return int(next(self.iter)) if self.iter else self.counter
    
    def increment(self):
        current = self.current()
        if self.iter:
            return current + self.qty
        else:
            self.counter += self.qty
            return current
    
    def multiply(self):
        current = self.current()
        if self.iter:
            return current * self.qty
        else:
            self.counter *= self.qty
            return current
        
class Configlet:
    def __init__(self, name, template, params = {}):        
        self.name = name
        self.template = template
        for k, v in params.items():
            setattr(self, k, v)  
              
    
        
    def compileIterables(self, source, baseTemplate):
        compiled = {}
        compiled['error'] = []
        iterables = parseForIterables(baseTemplate)

        for whitespace, template in iterables:
            
            extractedTemplates = [v.strip('[]') for v in re.split(r'(?<=\])[\s]*else[\s]*(?=\[)',template)]
            #iteration for []else[]i.e. 2 at most

            for i, _template in enumerate(extractedTemplates):
                valueDict = buildValueDict(source, _template)
                errorKeys = valueDict.pop('error')
                if not errorKeys:
                    #values is a dict
                    keys = list(valueDict.keys())
                    values_list = valueDict.values()

                    #basically turn lists into iterables and static values into functions which return the same thing everytime
                    #this way we can exhause iterators until they fail as we build new dicts to pass as args
                    #if the flag is never set i.e. no lists are found just return one
                    values_and_getters = []
                    _compiled = []
                    flag = False
                    error = False
                    for y, item in enumerate(values_list): 
                        #if the item is just a list without math then use StopIteration exception to stop iterations
                        if type(item) == list:
                            #found at least one list
                            flag = not flag if not flag else flag
                            values_and_getters.append((iter(item), lambda item:next(item)))
                        #if the item is a tuple then it wraps the item inside the tuple with math ops to be done e.g. (value, op, qty) where value can be a list, if so compile until exhausted
                        elif type(item) == tuple:
                            test = []
                            if type(item[0]) == list:
                                flag = not flag if not flag else flag
                                test = [t.isdigit() for t in item[0]]
                            else:
                                test.append(item[0].isdigit())
                            if not all(test):
                                compiled['error'].append((template, [keys[y]]))
                                error = True
                            values_and_getters.append((Math(*item), lambda item:item.do()))
                        #this is a single value, no math, compile once
                        else:
                            values_and_getters.append((item, lambda item:item))
                    #sanitize format syntax from templates and replase actual keys with positionals
                    if error:
                        break
                       
                    _keys = []
                    
                    #don't modify existing i; this is to sanitize and replace invalid keys for the format function used later
                    for x, key in enumerate(keys, 0):
                        x = 'i'+str(x)
                        _template = _template.replace('{'+key+'}', '{'+x+'}')
                        _keys.append(x)
                        

                    #exhaust iterators
                    z = 0
                    try:
                        #if flag is tripped then we know to iterate until the exception
                        while flag:
                            _compiled.append((whitespace if z else '')+_template.format(**dict(zip(_keys, [function(value) for value, function in values_and_getters]))))
                            z+=1
                        else:
                            #no lists were found return once
                            compiled[template] = _template.format(**dict(zip(_keys, [function(value) for value, function in values_and_getters])))    
                    except StopIteration as e:
                        compiled[template] = '\n'.join(_compiled)
                    
                    if i == 0:
                        break
                    if i == 1:
                        compiled['error'].pop()
                        
                else:
                    compiled['error'].append((template, errorKeys))
        
        return compiled
    #source can be either dict or object class i.e. getattr(CLASS, VALUE, None) or DICT.get(value,None)
    #will be used accordingly
    
    def compileTemplate(self, source, baseTemplate):
        _baseTemplate = baseTemplate
        compiled = {}
        compiled['error'] = []
        #now deal with the base template after sections/iterables are worked out
        valueDict = buildValueDict(source, baseTemplate)
        errorKeys = valueDict.pop('error')
        if errorKeys:
            compiled['error'] = errorKeys
            #verbose.append("-error building configlet {0}: global/device definition for {1} undefined".format(self.name, ','.join(errorKeys)))
            compiled[baseTemplate] = ''
            return compiled
        
        #this is to sanitize and replace invalid keys in the format function    
        _keys = []
        for i, key in enumerate(valueDict.keys(), 0):
            i = 'i'+str(i)
            _baseTemplate = _baseTemplate.replace('{'+key+'}', '{'+i+'}')
            _keys.append(i)
            
        try:
            compiled[baseTemplate] = _baseTemplate.format(**dict(zip(_keys, valueDict.values())))
            return compiled
        except KeyError as e:
            #verbose.append("-error building configlet {0}: global/device definition for {1} undefined".format(self.name, e))
            compiled['error'] = [str(e)]
            #must return a value which passes a boolean test
            #we will usually get here if the parent configlet requires device @property functions but the 
            #return ('', verbose)
            compiled[baseTemplate] = ''
            return compiled

        
        
    def compile(self, source):
        #TODO: Right now all the string replacements happen literally carrying the groups as the toReplace parameters
        #can definitely do this better
        baseTemplate = self.template
        #parse for sections @...@{test}
        #and recurse on stripped sections
        sections = parseForSections(baseTemplate)
        verbose = []
        for section in sections:
            #has clause to enable/disable
            _section, _test = section
            __section = _section.strip('@')

            
            
            #test the "tests" arguments i.e @...@{tests}
            #parseCondition returns a (value, function) tuple the fn(value) will return true/false if the test passes
            #here we collect the key which failed a test
            
            __test = _test.strip('{}')
            
            if len(__test):
                failedTests = [v[0] for v, fn in buildConditionTest(__test) if not fn(*v, source = source)]
            else:
                failedTests = []
                
            
            if len(__test) and failedTests:
                #there is a test but failed -> WIPE
                verbose.append("-skipping configlet section {0} in {1}: test condition for {2} failed".format(
                    re.sub(r"[\n\t\r]*", "", _section[:15]),
                    self.name,
                    ','.join(failedTests)
                ))
                baseTemplate = baseTemplate.replace(_section + _test, '')
                continue
                
            compiledIterables = self.compileIterables(source, __section)
            errorIterables = compiledIterables.pop('error')
            
            for toReplace, compiled in compiledIterables.items():
                __section = __section.replace(toReplace, compiled)
            
            for toReplace, errorKeys in errorIterables:
                    verbose.append("-skipping configlet section {0} in {1}: iterations failed on {2}".format(
                        re.sub(r"[\n\t\r]*", "", toReplace[:15]),
                        self.name,
                        ','.join(errorKeys)
                    ))
                    __section = re.sub(cleanRemoveWrap(toReplace), '', __section)
                    
            #deal with variables
            #if len(__section):
            compiledTemplate = self.compileTemplate(source, __section)
            templateErrors = compiledTemplate.pop('error')
            
            if templateErrors:
                verbose.append("-error building section {0}: global/device definition for {1} undefined".format(self.name, ','.join(templateErrors)))
                __section = ''
            else:
                for toReplace, compiled in compiledTemplate.items():   
                    __section = __section.replace(toReplace, compiled)
                    
            baseTemplate = baseTemplate.replace(_section + _test, __section)
                
        #parse stuff in [] for iterations outside of sections
        #support only one iterable for now from the global space
        compiledIterables = self.compileIterables(source, baseTemplate)
        errorIterables = compiledIterables.pop('error')
            
        for toReplace, compiled in compiledIterables.items():   
            baseTemplate = baseTemplate.replace(toReplace, compiled)  
            
        for toReplace, errorKeys in errorIterables:
            verbose.append("-skipping configlet option {0} in {1}: variable {2} undefined".format(
                        re.sub(r"[\n\t\r]*", "", toReplace[:15]) + '...',
                        self.name,
                        ','.join(errorKeys)
            ))
            #baseTemplate = baseTemplate.replace(toReplace+'\r\n', '')
            baseTemplate = re.sub(cleanRemoveWrap(toReplace), '', baseTemplate)

        compiledTemplate = self.compileTemplate(source, baseTemplate)
        templateErrors = compiledTemplate.pop('error')
        
        if templateErrors:
            verbose.append("-error building configlet {0}: global/device definition for {1} undefined".format(self.name, ','.join(templateErrors)))
            return ('', verbose)
        else:
            for toReplace, compiled in compiledTemplate.items():   
                baseTemplate = baseTemplate.replace(toReplace, compiled)
                
        return (baseTemplate.strip(), verbose)


class Manager:
    
    def __init__(self, fromUI):
        self.tasks_to_deploy = []   
        self.fromUI = fromUI
        id = searchSource('id', fromUI, None)
        
        self.COMPILE_FOR = []
        self.DEVICES = {}
        self.HOST_TO_DEVICE = {}
        self.SPINES = []
        self.TEMPLATES = {}

        self.CONFIG = {'global': Global_Config.objects.get(name='master').params}
        
        templates = Template.objects.all()
        for _template in templates:
            self.TEMPLATES[_template.id] = Configlet(_template.name, _template.template)

        self.mode = searchSource('mode', fromUI, 'no_mode_error')

        if id:
            self.dbRecord = Deployment.objects.get(id=id)
            self.last_deployment = searchSource('last_deployment', self.dbRecord, 0)
            self.last_deployed_var = searchSource('last_deployed_var', self.dbRecord, {})
            self.name = self.dbRecord.name

            self.previous_device_vars = {}
            d = self.last_deployed_var

            if self.mode == "nested":
                try:
                    for spineSerialNumber in d['device_vars'].keys():
                        self.previous_device_vars[spineSerialNumber] = defaultdict(list)

                        for row in d['device_vars'][spineSerialNumber]['data']:

                            if not any(row):
                                continue

                            # row[0] -> serialNumber
                            self.previous_device_vars.setdefault(row[0], defaultdict(list))

                            for i, column in enumerate(d['device_vars'][spineSerialNumber]['columns']):
                                # serialNumber and hostname are used to init Switch class and should not be lists
                                if column == 'serialNumber':
                                    self.previous_device_vars[row[0]][column] = row[i]
                                    column = 'leaf_serialNumber'
                                elif column == 'hostname':
                                    self.previous_device_vars[row[0]][column] = row[i]
                                    column = 'leaf_hostname'
                                else:
                                    self.previous_device_vars[row[0]][column].append(row[i])

                                self.previous_device_vars[spineSerialNumber][column].append(row[i])
                            self.previous_device_vars[row[0]]['spine_hostname'].append(spineSerialNumber)
                                
                            #_vars = OrderedDict(zip(d['device_vars'][spineSerialNumber]['columns'], row))
                            #_vars['__link__'] = row
                            #self.previous_device_vars[spineSerialNumber][_vars['serialNumber']] = _vars

                except Exception as e:
                    print(e)
            else:
                try:
                    for row in d['device_vars']['Tab0']['data']:
                        if not any(row):
                            continue
                        _vars = OrderedDict(zip(d['device_vars']['Tab0']['columns'], row))
                        #_vars['__link__'] = row
                        self.previous_device_vars[_vars['serialNumber']] = _vars
                except Exception as e:
                    print(e)
        else:
            self.dbRecord = Deployment()
            self.last_deployment = 0
            self.last_deployed_var = {}
            self.previous_device_vars = {}
            self.name = searchSource('name', fromUI, 'no_name_error')

        
        
        # since we allow for the frontend to remember data filled out in iterables for mutable template list
        # we get rid of the unnecessary data here
        iterables = searchSource('iterables', fromUI, {})

        selected_template_names = [searchSource('name', self.TEMPLATES[_id]) for _id in
                                   searchSource('selected_templates', fromUI, [])]

        for tab in [tab for tab in iterables.keys()]:
            if tab.split('#')[0] not in selected_template_names:
                iterables.pop(tab)

        self.current_deployment_var = {
            "compile_for": searchSource('compile_for', fromUI, []),
            "selected_templates": searchSource('selected_templates', fromUI, []),
            "device_vars": searchSource('device_vars', fromUI, {}),
            "iterables": iterables,
            "variables": searchSource('variables', fromUI, {}),
            "mode": self.mode
        }
        
        self.current_device_vars = {}
        d = self.current_deployment_var

        if self.mode == "nested":

            try:
                """ for spineSerialNumber in d['device_vars'].keys():
                    self.current_device_vars[spineSerialNumber] = {}
                    for row in d['device_vars'][spineSerialNumber]['data']:
                        if not any(row):
                            continue
                        _vars = OrderedDict(zip(d['device_vars'][spineSerialNumber]['columns'], row))
                        _vars['__link__'] = row
                        self.current_device_vars[spineSerialNumber][_vars['serialNumber']] = _vars """

                for spineSerialNumber in d['device_vars'].keys():
                        self.current_device_vars[spineSerialNumber] = defaultdict(list)

                        for row in d['device_vars'][spineSerialNumber]['data']:

                            if not any(row):
                                continue

                            # row[0] -> serialNumber
                            self.current_device_vars.setdefault(row[0], defaultdict(list))

                            for i, column in enumerate(d['device_vars'][spineSerialNumber]['columns']):
                                # serialNumber and hostname are used to init Switch class and should not be lists
                                if column == 'serialNumber':
                                    self.current_device_vars[row[0]][column] = row[i]
                                    column = 'leaf_serialNumber'
                                elif column == 'hostname':
                                    self.current_device_vars[row[0]][column] = row[i]
                                    column = 'leaf_hostname'
                                else:
                                    self.current_device_vars[row[0]][column].append(row[i])

                                self.current_device_vars[spineSerialNumber][column].append(row[i])

                            self.current_device_vars[row[0]]['spine_hostname'].append(spineSerialNumber)
            except Exception as e:
                print(e)
        else:
            try:
                for row in d['device_vars']['Tab0']['data']:
                    if not any(row):
                        continue
                    _vars = OrderedDict(zip(d['device_vars']['Tab0']['columns'], row))
                    _vars['__link__'] = row
                    self.current_device_vars[_vars['serialNumber']] = _vars
            except Exception as e:
                print(e)
        
    def stageDeployment(self, d, loadInventory=True,  loadConfiglets=True, loadContainers=False, skipCvp=False):
        LOGGER.log_noTs("-initializing internal state: please wait...")

        self.COMPILE_FOR = []
        self.DEVICES = {}
        self.SPINES = []
        self.HOST_TO_DEVICE = {}

        CVP.loadInventory() if loadInventory else None
        CVP.loadConfiglets() if loadConfiglets else None
        CVP.loadContainers() if loadContainers else None

        if d == 'current':
            d = self.current_deployment_var
            device_vars = self.current_device_vars
        elif d == 'last':
            d = self.last_deployed_var
            device_vars = self.previous_device_vars

        self.CONFIG = {'global': Global_Config.objects.get(name='master').params}

        # fix disjointed keys/values from UI
        variables = {}
        if 'variables' in d:
            variables = dict(zip(d['variables']['keys'], d['variables']['values']))
        
        iterables = {}
        if 'iterables' in d:
            for data in d['iterables'].values():
                for i, column in enumerate(data['columns']):
                    _data = [v[i] for v in data['data']]
                    if not any(_data):
                        continue
                    iterables[column] = _data

        if self.mode == 'nested':

            for serialNumber, data in device_vars.items():

                cvp_record = CVP.getBySerial(serialNumber)

                # original data has SerialNumbers; need to translate to hostnames here
                if 'spine_hostname' in data:
                    spine_hostnames = []
                    for _serialNumber in data['spine_hostname']:
                        _cvp_record = CVP.getBySerial(_serialNumber)
                        spine_hostnames.append(_cvp_record['hostname'] if _cvp_record else 'NOT_IN_CVP')
                    data['spine_hostname'] = spine_hostnames

                if not cvp_record:
                    # leaf
                    if 'spine_hostname' in data:
                        self.COMPILE_FOR.append(Switch(data))
                    # spine
                    else:
                        self.COMPILE_FOR.append(Switch({'serialNumber': serialNumber, 'hostname': 'NOT_IN_CVP', **data}))
                else:
                    self.COMPILE_FOR.append(Switch(data, cvp_record))

        else:
            for serialNumber in searchSource('compile_for', d, []):
                device = Switch(searchSource(serialNumber, device_vars, {}))

                if not skipCvp and device.loadCVPRecord():
                    device.loadCVPConfiglets()
                self.COMPILE_FOR.append(device)
        
        self.CONFIG = {**variables, **iterables, **self.CONFIG}
        self.CONFIG['selected_templates'] = searchSource('selected_templates', d, [])
        self.CONFIG['name'] = self.name
                
    def stageTasks(self):
        LOGGER.log_noTs("-staging builder tasks")
            
        selected_templates = searchConfig('selected_templates')
        spines = searchConfig('spines')

        if not (spines):
            LOGGER.log('-spines must be defined in the global master config; aborting')
            return False

        for device in self.COMPILE_FOR:    

            for id in selected_templates:
                device.assign_configlet(MANAGER.TEMPLATES[id])
                self.tasks_to_deploy.append(Task(device))

        return True
        
    def sync(self):
        LOGGER.log("Running: Sync")
        
        if self.last_deployment:

            self.stageDeployment('last')
            self.stageTasks()
            
            for task in self.tasks_to_deploy:
                task.execute()
            self.tasks_to_deploy = []
            
        else:
            LOGGER.log("-last deployment record does not exist; aborting")
        LOGGER.log("Done: Sync")
         
    def verifyLastDeployment(self):
        LOGGER.log("Running: Verify Last Deployment")
        
        if self.last_deployment:

            self.stageDeployment('last')
            self.stageTasks()
            
            for task in self.tasks_to_deploy:
                task.verify()
            self.tasks_to_deploy = []
            LOGGER.log("Done: Verify Last Deployment")
            
    def _verifyPreDeployment(self):
        LOGGER.log_noTs('')
        LOGGER.log_noTs("-verifying pre deployment; ignoring non-existent and matched un-assigned configlets")
        
        error = 0
        
        self.stageDeployment('current', loadContainers=True)
        self.stageTasks()

        for task in self.tasks_to_deploy:
            if not task.verify(ignoreDeleted = True, ignoreNotAssigned = True, ignoreNotAssigned_Mismatched = False):
                error += 1
        self.tasks_to_deploy = []
        LOGGER.log_noTs("-done verifying pre deployment")
        LOGGER.log_noTs('')
        return not bool(error)
    
    def _verifyLastDeployment(self):
        LOGGER.log_noTs('')
        LOGGER.log_noTs("-verifying last deployment; ignoring deleted and un-assigned configlets")
        
        error = 0
        
        self.stageDeployment('last', loadContainers=True)
        self.stageTasks()
        
        for task in self.tasks_to_deploy:
            if not task.verify(ignoreDeleted = True, ignoreNotAssigned = True, ignoreNotAssigned_Mismatched = False):
                error += 1
        self.tasks_to_deploy = []

        LOGGER.log_noTs("-done verifying last deployment")
        LOGGER.log_noTs('')
        return not bool(error)
    
    def _sync(self):
        LOGGER.log_noTs("-running pre deployment checks and cleanup")

        if self.mode == "flat":
            old_compile_for = searchSource('compile_for', self.last_deployed_var, [])
            new_compile_for = self.current_deployment_var['compile_for']

        elif self.mode == "nested":

            old_compile_for = set()
            new_compile_for = set()

            for spine_serial_number, leafs in self.last_deployed_var.items():
                old_compile_for.add(spine_serial_number)

                for leaf_serial_number in leafs.keys():
                    old_compile_for.add(leaf_serial_number)

            for spine_serial_number, leafs in self.current_deployment_var.items():
                new_compile_for.add(spine_serial_number)

                for leaf_serial_number in leafs.keys():
                    new_compile_for.add(leaf_serial_number)

        old_templates = searchSource('selected_templates', self.last_deployed_var, [])
        new_templates = self.current_deployment_var['selected_templates']
        
        removeDevices = [dev for dev in old_compile_for if dev not in new_compile_for]
        removeTemplates = [temp for temp in old_templates if temp not in new_templates]   
            
        generated_deployment_configlets = [c for c in CVP.configlets.values() if c['name'].startswith("DEPLOYMENT:"+self.fromUI['name'])]
        
        templates_to_remove = []
        
        # remove for all devices
        for id in removeTemplates:
            template_name =  searchSource('name', self.TEMPLATES[id], '')
            templates_to_remove = [c for c in generated_deployment_configlets if re.split('DEPLOYMENT:| HOST:| SN:| TEMPLATE:', c['name'])[4] == template_name]
            
        # for all previous and current devices
        for serialNumber in list(set(old_compile_for + new_compile_for)):
            
            device = CVP.getBySerial(serialNumber)
            
            if device:
                CVP.loadDeviceConfiglets(serialNumber)
            
            moveContainer = False
            
            generated_device_configlets = [c for c in generated_deployment_configlets if re.split('DEPLOYMENT:| HOST:| SN:| TEMPLATE:', c['name'])[3] == serialNumber]
            generated_template_device_configlets = [c for c in templates_to_remove if re.split('DEPLOYMENT:| HOST:| SN:| TEMPLATE:', c['name'])[3] == serialNumber]
            
            # prune templates then unassign then remove all
            templates_to_remove = [c for c in templates_to_remove if c not in generated_template_device_configlets]
            
            if serialNumber in removeDevices:
                # have to unassign templates first so filter out global template removals
                toRemove = [(c['name'],c['key']) for c in generated_device_configlets]
                
            else:
                toRemove = [(c['name'],c['key']) for c in generated_template_device_configlets]
                
                if self.name == 'Management':
                    
                    oldContainer = searchSource('container', searchSource(serialNumber, self.previous_device_vars), 'not_found')
                    newContainer = searchSource('container', searchSource(serialNumber, self.current_device_vars), 'not_found')
                    if newContainer != oldContainer:
                        moveContainer = newContainer
                
            toRemove = list(set(toRemove))
            toRemove = [{'name':c[0], 'key':c[1]} for c in toRemove]
            
            if moveContainer and device:
                try:
                    container = searchSource(moveContainer.lower(), CVP.containers, {})
                    if container:
                        LOGGER.log_noTs('--moving {0} from container {1} to {2}'.format(searchSource('hostname', device, 'no_hostname_error'), oldContainer, newContainer))
                        CVP.cvprac.api.move_device_to_container('builder', device, container)
                        varsDict = searchSource(serialNumber, self.previous_device_vars, {})
                        varsDict['container'] = moveContainer
                        link = varsDict['__link__']
                        #this updates the actual row data in self.last_deployed_var without __link__
                        link[:] = list(varsDict)[:-1]
                except (CvpApiError) as e:
                    LOGGER.log_noTs("--exception moving containers, please try again; aborting")
                    LOGGER.log_noTs(e)
                    return False
            
            if toRemove:
                try:
                    if device:
                        LOGGER.log_noTs('--unassigning configlets from {0}'.format(searchSource('hostname', device, 'no_hostname_error')))
                        CVP.cvprac.api.remove_configlets_from_device('builder', device, toRemove)
                    for c in toRemove:
                        LOGGER.log_noTs('--removing configlet {0}'.format(searchSource('name', c, 'no_name_error')))
                        CVP.cvprac.api.delete_configlet(c['name'], c['key'])
                    if serialNumber in removeDevices:
                        self.last_deployed_var['compile_for'].remove(serialNumber)
                        row = list(searchSource(serialNumber, self.previous_device_vars, {}).values())
                        #row = [row for row in self.last_deployed_var['device_vars']['Tab0']['data'] if row[0] == serialNumber][0]
                        self.last_deployed_var['device_vars']['Tab0']['data'].remove(row)
                    
                except (CvpApiError) as e:
                    
                    LOGGER.log_noTs("--exception deleting configlets (979), please try again; aborting")
                    LOGGER.log_noTs(e)
                    return False

                
            
        
        for id in removeTemplates:
            self.last_deployed_var['selected_templates'].remove(id)
        
        #this makes the last_deployed state true with what was removed during this operation to match current state in case of further exceptions and subsequent attempts to update which will not verify.
        try:    
            self.dbRecord.last_deployed_var = self.last_deployed_var
            self.dbRecord.save()
        except dbError as e:
            LOGGER.log_noTs('--error updating last deployment in DB to reflect removed CVP records (line1016); aborting')
            LOGGER.log_noTs('--please update the DB manually, last deployment should be:')
            LOGGER.log_noTs(json.dumps(self.last_deployed_var))
            LOGGER.log_noTs('')
            LOGGER.log_noTs(e)
            return False
        LOGGER.log_noTs("-done running pre deployment checks and cleanup")
        LOGGER.log_noTs("")
                                          
        return True
    
    def saveAndDeploy(self):
        import time
        
        LOGGER.log("Running: Save and Deploy")
        
        if self.last_deployment:
            
            #diff curr/last deployed i.e. currently deployed and being modified
            try:
                self.dbRecord.current_deployment_var = self.current_deployment_var
                self.dbRecord.save()
            except (dbError) as e:
                LOGGER.log_noTs('-error saving current deployment (line952); aborting')
                LOGGER.log_noTs('-following should manually pushed to DB as the current deployment:')
                LOGGER.log_noTs(json.dumps(self.current_deployment_var, indent=4))
                LOGGER.log_noTs('')
                LOGGER.log_noTs(e)
                return False
            
            # when sync returns it should have removed all configlets and updated last_deployed_var to reflect those removals
            # if it failed on any of the cvprac calls then the configlets might have been partially removed from CVP but not reflected in the DB for last_deployed_vars
            # worst case scenario we can now rerun and remove non existent cvp configlets and finally update the db
            if self._verifyLastDeployment() and self._sync():
                
                # ===============================================================
                # if self.current_deployment_var == self.last_deployed_var:
                #     LOGGER.log_noTs('')
                #     LOGGER.log_noTs("Nothing to do")
                #     return True
                # ===============================================================
                
                try:
                    self.dbRecord.last_deployment = int(time.time())
                    self.dbRecord.last_deployed_var = self.current_deployment_var
                    self.dbRecord.save()
                except dbError as e:
                    LOGGER.log_noTs('-error pushing current deployment vars into history (line1016); aborting')
                    LOGGER.log_noTs('-following should manually pushed to DB as current/last after which \"Sync\" should fix state:')
                    LOGGER.log_noTs(json.dumps(self.current_deployment_var,indent=4))
                    LOGGER.log_noTs('')
                    LOGGER.log_noTs(e)
                    return False
                
                # inventory and containers loaded in _verifyLastDeployment()
                self.stageDeployment('current', loadInventory=False, loadContainers=False)
                self.stageTasks()

                for task in self.tasks_to_deploy:
                    task.execute()
                self.tasks_to_deploy = []
                
            else:
                LOGGER.log_noTs('')
                LOGGER.log_noTs("Inconsistent state. Aborting.")
            
        else:
            #first deployment
            try:
                self.dbRecord.name = self.name
                self.dbRecord.current_deployment_var = self.current_deployment_var
                self.dbRecord.save()
            except dbError as e:
                LOGGER.log_noTs('-error creating deployment (line952); aborting')
                LOGGER.log_noTs(json.dumps(self.fromUI,indent=4))
                return False
            
            if self._verifyPreDeployment():
                try:
                    self.dbRecord.last_deployment = int(time.time())
                    self.dbRecord.last_deployed_var = self.current_deployment_var
                    self.dbRecord.save()
                except dbError as e:
                    LOGGER.log_noTs('-error creating deployment (line1113); aborting')
                    LOGGER.log_noTs(json.dumps(self.fromUI,indent=4))
                    return False
                
                self.stageTasks()
    
                for task in self.tasks_to_deploy:
                    task.execute()
                self.tasks_to_deploy = []
            else:
                LOGGER.log_noTs('')
                LOGGER.log_noTs("Inconsistent state. Aborting.")
                
        LOGGER.log_noTs('')    
        LOGGER.log("Done: Save and Deploy")
        
    def debug(self):
        
        global DEBUG
        DEBUG = True
        
        LOGGER.log("Running: Debug")

        self.stageDeployment('current', loadConfiglets=False, skipCvp=True)
        self.stageTasks()
            
        for task in self.tasks_to_deploy:
            task.execute()
        self.tasks_to_deploy = []
        LOGGER.log("Done: Debug")
        

# get if dict, getattr if else
def searchSource(key, source, default = None):
    if type(source) is dict:
        return source.get(key, default)
    elif type(source) is list:
        for _source in source:
            found = searchSource(key, _source, default)
            if found != default:
                return found
        return default
    else:
        return getattr(source, key, default)

def searchConfig(key, default = None):
    config = default
    try:
        config = MANAGER.CONFIG[key]
    except:
        pass
    if config == default:
        try:
            config = MANAGER.CONFIG['global'][key]
        except:
            return config
    if type(config) == list or dict:
        return config
    
    if config.lower() == 'true':
        return True
    if config.lower() == 'false':
        return False

    return config

def getKeyDefinition(key, source):
    csv_telemetry_source = key.split('#')
    
    path = None
    truncate = None
    op = None
    qty = None
    
    if len(csv_telemetry_source) == 2:
         
        path = csv_telemetry_source[0]
        key  = csv_telemetry_source[1]
    
    math = parseForMath(key)
    
    #can't truncate math op's; so either or
    if math:
        key, op, qty = math[0]
    else:
        key, truncate = parseForTruncation(key)[0]
        
    if truncate:
        start, end = truncate[1:-1].split(':')
        start = int(start) if start else None
        end = int(end) if end else None
    else:
        start = None
        end = None
    
    def truncateValues(values, start = None, end = None):
        if type(values) == list:
            return [str(val)[start:end] for val in values]
        else:
            return str(values)[start:end]

    def fetchTelemOrFileData(path, key):
        if path.startswith('/') and hasattr(CVP, 'cvprac'):
            #this is super hacked need a telemetry Data Model parser. cvp-connector has one but in js
            
            try:
                found = CVP.cvprac.get('/api/v1/rest/' + searchSource('serialNumber', source, '').upper() + path)
                found = found['notifications'][0]['updates'][key]['value']

                if type(found) == dict:
                    __keys = found.keys()
                    if 'Value' in __keys:
                        found = found['Value']
                    elif 'value' in __keys:
                        found = found['value']
                    _type, val = next(iter(found.items()))
                    return val
                else:
                    return found
            except:
                LOGGER.log("-failed to properly fetch/decode telemetry data")
                return None
        return None
    
    if path:
        toReturn = fetchTelemOrFileData(path, key)
    elif key.isdigit():
        toReturn = key
    else:
        toReturn = searchSource(key, source) or searchConfig(key)

        if toReturn == 'ERROR' or not toReturn or (type(toReturn)==list and not all(toReturn)):
            toReturn = None
            
    if math:
        return (toReturn, op, qty)
    elif truncate:
        return truncateValues(toReturn, start, end)
    else:
        return toReturn

def cleanRemoveWrap(text):
    text = re.escape(text)
    return '[ \t]*{0}[\t ]*(?:\r\n|\n)'.format(text)

def parseForRequiredKeys(template):
    return re.findall('{(.*?)}', template)

def parseForIterables(template):
    return re.findall('([\t ]*)(\[[\s\S]*?\](?![\s]*else))', template)
    #return re.findall('(^\s*)\[[\s\S]*?\](?!else)', template)

def parseForSections(template):
    return re.findall('(@[\s\S]*?@)({.*?})*', template)

def parseForTruncation(key):
    return re.findall('([\w]+)(\([-+]?\d*:[-+]?\d*\))?', key)

def parseForMath(key):
    return re.findall('(\w+)([+\-*]+)(\d+)?', key)

#builds a tuple of values followed by a comparator lambda
#used to check if tests pass while supporting section injections from the global variable space
def buildConditionTest(keys):
    condition_list = []
    _keys = keys.split('&')
    for key in _keys:
        key = re.split('([^a-z0-9A-Z_]+)', key)
        if len(key) > 1:
            condition = key[1]
            if condition == '=':
                condition_fn = lambda key, value, source = None : value == getKeyDefinition(key, source)
            else:
                condition_fn = lambda key, value, source = None : value != getKeyDefinition(key, source)
            condition_list.append( ((key[0], key[2]), condition_fn) )
        else:
            
            condition_list.append( ((key[0],), lambda key, source = None : bool(getKeyDefinition(key, source))) )
    return condition_list

def buildValueDict(source, template):
    valueDict = {}
    valueDict['error'] = []
    
    keys = parseForRequiredKeys(template)

    for key in keys:
        #check if dict already has defined
        if valueDict.get(key, None):
            continue
        
        defined = getKeyDefinition(key, source)
        if not defined:
            valueDict['error'].append(key)
        else:
            valueDict[key] = defined
    return valueDict

def getBySerial(serialNumber):
    return searchSource(serialNumber, MANAGER.DEVICES, {})

def getByHostname(hostname):
    return searchSource(hostname, MANAGER.HOST_TO_DEVICE, {})


def send_message(conn, message):
    message = json.dumps(message)
    conn.sendall('{}\n'.format(message).encode())


def read_message(conn):
    buffer = b''
    while True:
        d = conn.recv(4096)
        if d[-1] == 10:
            buffer += d[:-1]
            break
        buffer = buffer + d
    return json.loads(buffer.decode())



def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('127.0.0.1', 54999))
        while True:
            print('Builder Listening')
            s.listen()
            while True:
                conn, addr = s.accept()
                with conn:
                    
                    current_deployment_var = read_message(conn)
                    action = current_deployment_var['action']
                    
                    if action == "status":
                        send_message(conn, {"status":"Online"})
                        continue
                    print('Established: ', conn)
                    
                    global DEBUG 
                    DEBUG = False

                    
                    #INIT LOGGER
                    global LOGGER
                    LOGGER = Log()
                    
                    #INIT MANAGER
                    global MANAGER 
                    MANAGER = Manager(current_deployment_var)
                        
                    #INIT CVP
                    global CVP
                    try:
                        CVP = Cvp()
                    except (ImportError, CvpClientError) as e:
                        LOGGER.log("Failed to init CVP")
                        LOGGER.log_noTs(e)
                        continue
                    
                    
                    
                    getattr(MANAGER, action)()
                    
                    send_message(conn, {"status":"success"})




if __name__ == '__main__':
    main()