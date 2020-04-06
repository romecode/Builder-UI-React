import urllib3
import re
from ipaddress import ip_address
import sys
import datetime
import xlrd
import socket
import json

LOGGER = None
CVP = None
DEBUG = False

import os
import django
from django.conf import settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'fabric_builder.settings')
django.setup()
MODULE_DIR = os.path.dirname(__file__)
from manager.models import Deployment, Template, Global_Config
from django.db import Error as dbError
        
           
class Log():
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
  
class Cvp():
    def __init__(self):

        self.cvprac = None
        self.containerTree = {}
        self.CvpApiError = None
        self.devices = {}
        self.host_to_device = {}
        self.containers = {}
        self.configlets = {}
        
        
        

        from cvprac.cvp_client import CvpClient
        from cvprac.cvp_client_errors import CvpClientError
        from cvprac.cvp_client_errors import CvpApiError
        self.CvpClientError = CvpClientError
        self.CvpApiError = CvpApiError
        self.cvprac = CvpClient()
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # to supress the warnings for https
        self.cvprac.connect(searchConfig('server'), searchConfig('user'), searchConfig('password'))
        LOGGER.log("Successfully authenticated to CVP")

    def loadConfiglets(self):
        LOGGER.log_noTs("-loading CVP configlets: please wait...")
        self.configlets = {item['name'].lower():item for item in self.cvprac.api.get_configlets()['data']}
        
    def loadInventory(self):
        LOGGER.log_noTs("-loading CVP inventory: please wait...")
        for device in self.cvprac.api.get_inventory():
            if device['parentContainerId'] != "undefined_container":
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
        except self.CvpApiError as e:
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
        
        #dest is a container, sn. or hostname string
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
    
        
class Task():
    def __init__(self, device = None, mode = None):
        self.device = device
        self.mode = mode
    
    def verify(self, ignoreDeleted = False, ignoreNotAssigned = False, ignoreNotAssigned_Mismatched = False):
        error = 0

        LOGGER.log_noTs('')
        LOGGER.log_noTs("******* {0} / {1} *******".format(self.device.hostname, self.device.serialNumber))
        configlet_keys = []
        
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
                LOGGER.log_noTs("Configlet does not match and is not assigned expected/actual: {0}".format(name))
                LOGGER.log_noTs('-'*50)
                LOGGER.log_noTs(new_configlet_content)
                LOGGER.log_noTs('-'*50)
                LOGGER.log_noTs(exists['config'])
                LOGGER.log_noTs('-'*50)
                error += 1
            elif assigned and not match:
                LOGGER.log_noTs("Configlet does not match expected/actual: {0}".format(name))
                LOGGER.log_noTs('-'*50)
                LOGGER.log_noTs(new_configlet_content)
                LOGGER.log_noTs('-'*50)
                LOGGER.log_noTs(exists['config'])
                LOGGER.log_noTs('-'*50)
                error += 1
        if not error:
            LOGGER.log_noTs("Device is consistent with CVP")        
        
        LOGGER.log_noTs('')
        
            
        self.device.to_deploy = []
        
        return not bool(error)
    
    #the task finally figures out what to assign and compile
    def execute(self):
        configlet_keys = []
        #apply_configlets = searchConfig('apply_configlets')
        
        def pushToCvp():
            container = searchSource('container', self.device)
            
            if self.device.cvp['parentContainerId'] == "undefined_container" and container:
                CVP.deployDevice(self.device, container, configlet_keys)
            elif self.device.cvp['parentContainerId'] == "undefined_container" and not container:
                LOGGER.log("---cannot deploy {0}; non-provisioned device with no destination container defined".format(self.device.hostname))
            else:
                CVP.applyConfiglets(self.device.serialNumber, configlet_keys) 
                
        #DAY1 and DAY2 EXECUTION HAPPENS HERE
        LOGGER.log_noTs('')
        LOGGER.log_noTs("******* {0} / {1} *******".format(self.device.hostname.upper(), self.device.serialNumber.upper()))
        
        for name, configlet in self.device.to_deploy:
            new_configlet_content, compile_info = configlet.compile(self.device)
            LOGGER.log_noTs('CONFIGLET NAME: '+ name)

            LOGGER.log_noTs('-'*50)
            LOGGER.log_noTs('\n'.join(compile_info)) if compile_info else None
            LOGGER.log_noTs('-'*50)
            LOGGER.log_noTs(new_configlet_content)
            LOGGER.log_noTs('')
            

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
            
                

class Switch():
    
    def __init__(self, params={}, cvpDevice={}):
        #list to hold leaf compiled spine underlay interface init
        self.underlay_inject = []
        self.to_deploy = []
        self.cvp = {}
        for k, v in params.items():
            setattr(self, k, v)
        
        
        self.hostname = searchSource('hostname', self) or searchSource('hostname', cvpDevice)
        self.serialNumber = searchSource('serialNumber', self) or searchSource('serialNumber', cvpDevice)
        
        LOGGER.log_noTs("-loading {0}".format(self.hostname))
        
        self.hostname_lower = self.hostname.lower()
        

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
        #TODO: MAKE HANDLE LIST LOOKUPS, RIGHT NOW ONLY WORKS FOR ONE CONTAINER OR ONE DEVICE i.e. USELESS
        exception = getattr(template, "skip_container", None)
        if exception == self.role:
            return None
        exception = getattr(template, "skip_device", None)
        if exception == self.serialNumber:
            return None
        configlet_name = "DEPLOYMENT:{0} HOST:{1} SN:{2} TEMPLATE:{3}".format(searchConfig('name'), self.hostname.upper(), self.serialNumber.upper(), template.name)
        self.to_deploy.append((configlet_name, template))
     
    def compile_configlet(self, template):
        #TODO: MAKE HANDLE LIST LOOKUPS, RIGHT NOW ONLY WORKS FOR ONE CONTAINER OR ONE DEVICE i.e. USELESS
        exception = getattr(template, "skip_container", None)
        if exception == self.role:
            return ('',[])
        exception = getattr(template, "skip_device", None)
        if exception == self.serialNumber:
            return ('',[])
        return template.compile(self)    
    
    @property
    def peer_desc(self, peer):
        return "TO-{0}".format(peer.hostname)
        
    #===========================================================================
    # @property    
    # def mlag_address(self):
    #     try:
    #         neighbor = getByHostname(self.mlag_neighbor)
    #         mgmt_ip = ip_address(unicode(self.mgmt_ip[:-3]))
    #         neighbor_mgmt = ip_address(unicode(neighbor.mgmt_ip[:-3]))
    #         global_mlag_address = ip_address(unicode(self.searchConfig('mlag_address')))
    #         if mgmt_ip > neighbor_mgmt:
    #             return global_mlag_address + 1
    #         else:
    #             return global_mlag_address
    #     except:
    #         return 'ERROR'
    #     
    # @property
    # def mlag_peer_address(self):
    #     try:
    #         neighbor = getByHostname(self.mlag_neighbor)
    #         return str(neighbor.mlag_address)
    #     except:
    #         return 'ERROR'
    #===========================================================================
    
    @property
    def reload_delay_0(self):
        if getattr(self, "is_jericho", None):
            return self.searchConfig('reload_delay_jericho')[0]
        else:
            return self.searchConfig('reload_delay')[0]
        
    @property
    def reload_delay_1(self):
        if getattr(self, "is_jericho", None):
            return self.searchConfig('reload_delay_jericho')[1]
        else:
            return self.searchConfig('reload_delay')[1]
    
    @property
    def underlay(self):
        #TODO!!!!
        template = MANAGER.TEMPLATES.get('underlay_private')
        i = 0
        
        if len(self.underlay_inject):
            return "\n".join([t[0] for t in self.underlay_inject])
        
        for i, spine in enumerate(MANAGER.SPINES, start = 1):
            #compile p2p link to spine
            
            try:
                ipAddress = ip_address(unicode(getattr(self, "sp{0}_ip".format(i))))
                spine_args = {
                    "interface" : getattr(self, "sp{0}_int".format(i)),
                    "address" : ipAddress,
                    "interface_speed" : getattr(self, "sp{0}_speed".format(i), self.searchConfig('fabric_speed')),
                    "description" : "TO-{0}-UNDERLAY Ethernet{1}".format(self.hostname, getattr(self, "lf{0}_int".format(i)))
                }
                spine.underlay_inject.append(template.compile(spine_args))
                self_args = {
                    "interface" : getattr(self, "lf{0}_int".format(i)),
                    "address" : ipAddress + 1,
                    "interface_speed" : getattr(self, "sp{0}_speed".format(i), self.searchConfig('fabric_speed')),
                    "description" : "TO-{0}-UNDERLAY Ethernet{1}".format(spine.hostname, getattr(self, "sp{0}_int".format(i)))
                }
                self.underlay_inject.append(template.compile(self_args))
                
            except Exception as e:
                LOGGER.log("-error building configlet section underlay for {0}<->{1}: {2}".format(spine.hostname, self.hostname, e))
                sys.exit(0)
            
        return "\n".join(self.underlay_inject)

    @property
    def spine_asn(self):
        if len(MANAGER.SPINES) >= 1:
            return MANAGER.SPINES[0].asn
        else:
            return 'ERROR'

          
    @property
    def spine_lo0_list(self):
        return [spine.lo0 for spine in MANAGER.SPINES]
    
    @property
    def spine_lo1_list(self):
        return [spine.lo1 for spine in MANAGER.SPINES]
    
    @property
    def spine_ipv4_list(self):
        ipAddresses = []
        for i, spine in enumerate(MANAGER.SPINES, start = 1):
            #compile p2p link to spine
            ipAddresses.append(getattr(self, "sp{0}_ip".format(i)))
        return ipAddresses
    
    @property
    def spine_hostname_list(self):
        return [spine.hostname for spine in MANAGER.SPINES]
    
    @property
    def vrf_ibgp_peer_address(self):
        ip = self.searchConfig('vrf_ibgp_ip')
        return ip_address(unicode(ip)) + 1 if ip else 'ERROR'
    
class Math():
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
        
class Configlet():
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
            
            extractedTemplates = [v.strip('[]') for v in template.split('else')]
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
                                test.append(item.isdigit())
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
            compiledIterables = self.compileIterables(source, __section)
            errorIterables = compiledIterables.pop('error')
            #test the "tests" arguments i.e @...@{tests}
            #parseCondition returns a (value, function) tuple the fn(value) will return true/false if the test passes
            #here we collect the key which failed a test

            failedTests = [v[0] for v, fn in buildConditionTest(_test.strip('{}')) if not fn(*v, source = source)]
            
            if _test and not (failedTests or errorIterables):
                #there is a test and iterables with no errors -> COMPILE
                for toReplace, compiled in compiledIterables.items():
                    __section = __section.replace(toReplace, compiled)        
            elif _test and failedTests:
                #there is a test but failed WIPE
                verbose.append("-skipping configlet section {0} in {1}: test condition for {2} failed".format(
                    re.sub(r"[\n\t\r]*", "", _section[:15]),
                    self.name,
                    ','.join(failedTests)
                ))
                __section = ''
            elif compiledIterables and not errorIterables:
                #there is no test, and all iterables passed COMPILE
                for toReplace, compiled in compiledIterables.items():
                    __section = __section.replace(toReplace, compiled) 
            else:
                #no test, iterables failed WIPE
                for toReplace, errorKeys in errorIterables:
                    verbose.append("-skipping configlet section {0} in {1}: iterations failed on {2}".format(
                        re.sub(r"[\n\t\r]*", "", toReplace[:15]),
                        self.name,
                        ','.join(errorKeys)
                    ))
                __section = ''
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
            
        #now deal with the base template after sections/iterables are worked out
        valueDict = buildValueDict(source, baseTemplate)
        errorKeys = valueDict.pop('error')
        if errorKeys:
            verbose.append("-error building configlet {0}: global/device definition for {1} undefined".format(self.name, ','.join(errorKeys)))
            return ('', verbose)
        
        #this is to sanitize and replace invalid keys in the format function    
        _keys = []
        for i, key in enumerate(valueDict.keys(), 0):
            i = 'i'+str(i)
            baseTemplate = baseTemplate.replace('{'+key+'}', '{'+i+'}')
            _keys.append(i)
        try:
            baseTemplate = baseTemplate.format(**dict(zip(_keys, valueDict.values())))
        except KeyError as e:
            verbose.append("-error building configlet {0}: global/device definition for {1} undefined".format(self.name, e))
            #must return a value which passes a boolean test
            #we will usually get here if the parent configlet requires device @property functions but the 
            return ('', verbose)

        return (baseTemplate.strip(), verbose)
  
class Manager():
    
    def __init__(self, fromUI):
        self.tasks_to_deploy = []   
        self.fromUI = fromUI
        id = searchSource('id', fromUI, None)
        
        self.COMPILE_FOR = []
        self.DEVICES = {}
        self.HOST_TO_DEVICE = {}
        self.SPINES = []
        self.TEMPLATES = {}

        self.CONFIG = {'global':Global_Config.objects.get(name='master').params}
        
        templates = Template.objects.all()
        for _template in templates:
            self.TEMPLATES[_template.id] = Configlet(_template.name, _template.template)
        
        if(id):
            self.dbRecord = Deployment.objects.get(id=id)
            self.last_deployment = searchSource('last_deployment', self.dbRecord, 0)
            self.last_deployed_var = searchSource('last_deployed_var', self.dbRecord, {})
            
        else:
            self.dbRecord = Deployment()
            self.last_deployment = 0
            self.last_deployed_var = {}
        
        self.name = self.dbRecord.name if self.last_deployment else searchSource('name', fromUI, 'no_name_error')
        self.mode = searchSource('mode', fromUI, 'no_mode_error')
        
        #since we allow for the frontend to remember data filled out in interables for mutatable template list
        #we get rid of the unnecessary data here
        iterables = searchSource('iterables', fromUI, {})
        selected_template_names = [searchSource('name', self.TEMPLATES[id]) for id in searchSource('selected_templates', fromUI, [])]
        for tab in [tab for tab in iterables.keys()]:
            if tab.split('#')[0] not in selected_template_names:
                iterables.pop(tab)
                    
            
        self.current_deployment_var = {
            "compile_for": searchSource('compile_for', fromUI, []),
            "selected_templates": searchSource('selected_templates', fromUI, []),
            "device_vars": searchSource('device_vars', fromUI, {}),
            "iterables": iterables,
            "variables": searchSource('variables', fromUI, {}),
        }
        
    def stageDeployment(self, d):
        LOGGER.log_noTs("-initializing internal state: please wait...")

        self.CONFIG = {'global':Global_Config.objects.get(name='master').params}
        #fix disjointed keys/values from UI
        variables = dict(zip(d['variables']['keys'], d['variables']['values']))

        device_vars = {}
        for row in d['device_vars']['Tab0']['data']:
            if not any(row):
                continue
            vars = dict(zip(d['device_vars']['Tab0']['columns'],row))
            device_vars[vars['serialNumber']] = vars
        
        iterables = {}
        for t, data in d['iterables'].items():
            for i, column in enumerate(data['columns']):
                _data = [v[i] for v in data['data']]
                if not any(_data):
                    continue
                iterables[column] = _data

        compile_for = [serialNumber for serialNumber in searchSource('compile_for', d, [])]
        
        self.CONFIG = {**variables, **iterables, **self.CONFIG}    
        self.CONFIG['device_vars'] = device_vars
        self.CONFIG['compile_for'] = compile_for
        self.CONFIG['selected_templates'] = searchSource('selected_templates', d, [])
        self.CONFIG['name'] = self.name
        self.CONFIG['mode'] = self.mode
        
        print(json.dumps(self.CONFIG, indent=4))
        
    def sync(self):
        LOGGER.log("Running: Sync")
        
        if self.last_deployment:
            
            CVP.loadInventory()
            CVP.loadConfiglets()
            self.stageDeployment(self.last_deployed_var)
            
            compile_for = searchConfig('compile_for', [])
            
            for serialNumber in compile_for:
                
                device_vars = searchSource(serialNumber, searchConfig('device_vars'), {})
                device = Switch(device_vars)
                if device.loadCVPRecord():
                    device.loadCVPConfiglets()
                self.COMPILE_FOR.append(device)
                
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

            
            CVP.loadInventory()
            CVP.loadConfiglets()
            self.stageDeployment(self.last_deployed_var)
            
            compile_for = searchConfig('compile_for', [])
            
            
            
            for serialNumber in compile_for:
                
                device_vars = searchSource(serialNumber, searchConfig('device_vars'), {})
                device = Switch(device_vars)
                if device.loadCVPRecord():
                    device.loadCVPConfiglets()
                    
                self.COMPILE_FOR.append(device)
            
            self.stageTasks()
            
            for task in self.tasks_to_deploy:
                task.verify()
            self.tasks_to_deploy = []
            LOGGER.log("Done: Verify Last Deployment")
            
    def _verifyPreDeployment(self):
        LOGGER.log_noTs('')
        LOGGER.log_noTs("-verifying pre deployment; ignoring non-existent and matched un-assigned configlets")
        
        error = 0
        
        self.stageDeployment(self.current_deployment_var)
        
        compile_for = searchConfig('compile_for', [])
        
        
        
        for serialNumber in compile_for:
            
            device_vars = searchSource(serialNumber, searchConfig('device_vars'), {})
            device = Switch(device_vars)
            if device.loadCVPRecord():
                device.loadCVPConfiglets()
            self.COMPILE_FOR.append(device)

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
        
        self.stageDeployment(self.last_deployed_var)
        
        compile_for = searchConfig('compile_for', [])
        
        
        for serialNumber in compile_for:
            
            device_vars = searchSource(serialNumber, searchConfig('device_vars'), {})
            device = Switch(device_vars)
            if device.loadCVPRecord():
                device.loadCVPConfiglets()
            self.COMPILE_FOR.append(device)
        
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
        old_compile_for = searchSource('compile_for', self.last_deployed_var, [])
        new_compile_for = self.current_deployment_var['compile_for']

        old_templates = searchSource('selected_templates', self.last_deployed_var, [])
        new_templates = self.current_deployment_var['selected_templates']
        
        removeDevices = [dev for dev in old_compile_for if dev not in new_compile_for]
        removeTemplates = [temp for temp in old_templates if temp not in new_templates]
            
        generated_deployment_configlets = [c for c in CVP.configlets.values() if c['name'].startswith("DEPLOYMENT:"+self.fromUI['name'])]
        
        templates_to_remove = []
        
        #remove for all devices 
        for id in removeTemplates:
            template_name =  searchSource('name', self.TEMPLATES[id], '')
            templates_to_remove = [c for c in generated_deployment_configlets if re.split('DEPLOYMENT:| HOST:| SN:| TEMPLATE:', c['name'])[4] == template_name]
            
        #for all previous and current devices
        for serialNumber in list(set(old_compile_for + new_compile_for)):
            
            device = CVP.getBySerial(serialNumber)
            
            if device:
                CVP.loadDeviceConfiglets(serialNumber)
                
            generated_device_configlets = [c for c in generated_deployment_configlets if re.split('DEPLOYMENT:| HOST:| SN:| TEMPLATE:', c['name'])[3] == serialNumber]
            generated_template_device_configlets = [c for c in templates_to_remove if re.split('DEPLOYMENT:| HOST:| SN:| TEMPLATE:', c['name'])[3] == serialNumber]
            
            #prune templates then unassign then remove all
            templates_to_remove = [c for c in templates_to_remove if c not in generated_template_device_configlets]
            
            if serialNumber in removeDevices:
                #have to unassign templates first so filter out global template removals
                toRemove = [(c['name'],c['key']) for c in generated_device_configlets]
            else:
                toRemove = [(c['name'],c['key']) for c in generated_template_device_configlets]
                
            toRemove = list(set(toRemove))
            toRemove = [{'name':c[0], 'key':c[1]} for c in toRemove]
            
            
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
                        row = [row for row in self.last_deployed_var['device_vars']['Tab0']['data'] if row[0] == serialNumber][0]
                        self.last_deployed_var['device_vars']['Tab0']['data'].remove(row)
                    
                except (CVP.CvpApiError) as e:
                    
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

                                          
        return True
    
    def saveAndDeploy(self):
        import time
        
        LOGGER.log("Running: Save and Deploy")
        
        CVP.loadInventory()

        
        if self.last_deployment:
            
            #diff curr/last deployed i.e. currently deployed and being modified
            try:
                self.dbRecord.current_deployment_var = self.current_deployment_var
                self.dbRecord.save()
            except (dbError) as e:
                LOGGER.log_noTs('-error saving current deployment (line952); aborting')
                LOGGER.log_noTs('-following should manually pushed to DB as the current deployment:')
                LOGGER.log_noTs(json.dumps(self.current_deployment_var,indent=4))
                LOGGER.log_noTs('')
                LOGGER.log_noTs(e)
                return False
            
            CVP.loadConfiglets()
            
            #when sync returns it should have removed all configlets and updated last_deployed_var to reflect those removals
            #if it failed on any of the cvprac calls then the configlets might have been partially removed from CVP but not reflected in the DB for last_deployed_vars
            #worst case scenario we can now rerun and remove non existent cvp configlets and finally update the db
            if self._verifyLastDeployment() and self._sync():
                
                #===============================================================
                # if self.current_deployment_var == self.last_deployed_var:
                #     LOGGER.log_noTs('')
                #     LOGGER.log_noTs("Nothing to do")
                #     return True
                #===============================================================
                
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
                
                self.COMPILE_FOR = []
                self.DEVICES = {}
                self.SPINES = []
                self.HOST_TO_DEVICE = {}

                CVP.loadConfiglets()
                
                self.stageDeployment(self.current_deployment_var)
                compile_for = searchConfig('compile_for', [])
        
                for serialNumber in compile_for:
                    
                    device_vars = searchSource(serialNumber, searchConfig('device_vars'), {})
                    device = Switch(device_vars)
                    if device.loadCVPRecord():
                        device.loadCVPConfiglets()
                    self.COMPILE_FOR.append(device)
                
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
            
            CVP.loadConfiglets()
            
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
            
    def stageTasks(self):
        LOGGER.log_noTs("-staging builder tasks")
        mode = searchConfig('mode')
        selected_templates = searchConfig('selected_templates')

        if mode == 'day1':
            
            for serialNumber, device in self.DEVICES.items():
                for id in selected_templates:
                    template = MANAGER.TEMPLATES[id]
                    device.assign_configlet(template)
                if device.role == "spine":
                    self.tasks_to_deploy.append(Task(device, mode = 1))
                else:
                    self.tasks_to_deploy.insert(0,Task(device, mode = 1))
            
        elif mode == 'day2':
            spines = searchConfig('spines')
            
            if not (spines):
                LOGGER.log('-error for mode=day2, spines must be defined in the global master config; aborting')
                return False

            for device in self.COMPILE_FOR:                
                for id in selected_templates:
                    template = MANAGER.TEMPLATES[id]
                    device.assign_configlet(template)
                if device.role == "spine":
                    self.tasks_to_deploy.append(Task(device, mode = 2))
                else:
                    self.tasks_to_deploy.insert(0,Task(device, mode = 2))
            return True
        
    def debug(self):
        LOGGER.log("Running: Debug")
        
        global DEBUG
        DEBUG = True
        
        self.stageDeployment(self.current_deployment_var)
        compile_for = searchConfig('compile_for', [])

        for serialNumber in compile_for:
            
            device_vars = searchSource(serialNumber, searchConfig('device_vars'), {})
            self.COMPILE_FOR.append(Switch(device_vars))
        
        self.stageTasks()
            
        for task in self.tasks_to_deploy:
            task.execute()
        self.tasks_to_deploy = []
        LOGGER.log("Done: Debug") 

#get if dict, getattr if else
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
    
    file = None
    truncate = None
    op = None
    qty = None
    
    if len(csv_telemetry_source) == 2:
         
        file = csv_telemetry_source[0]
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

    def fetchTelemOrFileData(file, key):
        if file.startswith('/') and hasattr(CVP, 'cvprac'):
            #this is super hacked need a telemetry Data Model parser. cvp-connector has one but in js
            
            try:
                found = CVP.cvprac.get('/api/v1/rest/' + searchSource('serialNumber', source, '').upper() + file)
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
    
    if file:
        toReturn = fetchTelemOrFileData(file, key)
    elif key.isdigit():
        toReturn = key
    else:
        toReturn = searchSource(key, source) or searchConfig(key)
        if toReturn == 'ERROR' or not toReturn:
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
    return re.findall('([\t ]*)(\[[\s\S]*?\](?!else))', template)
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
                    except (ImportError, self.CvpClientError) as e:
                        LOGGER.log("Failed to init CVP")
                        LOGGER.log_noTs(e)
                        continue
                    
                    
                    
                    getattr(MANAGER, action)()
                    
                    send_message(conn, {"status":"success"})




if __name__ == '__main__':
    main()