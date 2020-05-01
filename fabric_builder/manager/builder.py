import urllib3
import re
import sys
import datetime
import xlrd
import socket
import json
from collections import OrderedDict, defaultdict
from types import SimpleNamespace
from ansible.plugins.filter.ipaddr import FilterModule


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

from jinja2 import BaseLoader, TemplateNotFound, Environment, meta

from collections.abc import MutableMapping

import difflib

from ipaddress import ip_address, ip_network
        
class Telemetry(MutableMapping):
    """A dictionary that applies an arbitrary key-altering
       function before accessing the keys"""

    def __init__(self, serialNumber, *args, **kwargs):
        self.serialNumber = serialNumber
        self.store = dict()
        self.update(dict(*args, **kwargs))  # use the free update to set keys

    def __getitem__(self, key):
        return self.fetch(*key.split('#'))

    def __setitem__(self, key, value):
        self.store[self.__keytransform__(key)] = value

    def __delitem__(self, key):
        del self.store[self.__keytransform__(key)]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __keytransform__(self, key):
        return key

    def __bool__(self):
        return True

    def fetch(self, path, key = None):
        if path.startswith('/') and hasattr(CVP, 'cvprac') and key:
            #this is super hacked need a telemetry Data Model parser. cvp-connector has one but in js
            
            try:
                found = CVP.cvprac.get('/api/v1/rest/' + self.serialNumber.upper() + path)
                if not found['notifications']:
                    LOGGER.log_noTs("-telemetry data for {0} not found".format(path+'#'+key), "red")
                    return ''
                source = found['notifications']
                for item in source:
                    if key in item['updates']:
                        source = item['updates'][key]['value']

                if source:
                    if type(source) == dict:
                        __keys = source.keys()
                        if 'Value' in __keys:
                            source = source['Value']
                        elif 'value' in __keys:
                            source = source['value']
                        _type, val = next(iter(source.items()))
                        return val
                    else:
                        return source
            except (KeyError, IndexError, CvpClientError, CvpApiError) as e:
                LOGGER.log_noTs("-failed to properly fetch/decode telemetry data for {0}".format(path+'#'+key), "red")
                LOGGER.log_noTs("-exception: {0}".format(e), "red")

        return ''

    
 
class Log:
    def __init__(self):
        fabric_builder_log = open(MODULE_DIR + '/fabric_builder_log.txt', 'w')
        fabric_builder_log.close()
        
    def log(self, string, color = None):
        fabric_builder_log = open(MODULE_DIR + '/fabric_builder_log.txt', 'a')
        fabric_builder_log_complete = open(MODULE_DIR + '/fabric_builder_log_complete.txt', 'a')
        
        string = "{0}: {1}\n".format( datetime.datetime.now().strftime('%a %b %d %H:%M'), string )
        sys.stderr.write(string)
        string = self.wrap(string, color)
        fabric_builder_log.write(string)
        fabric_builder_log.close()
        
        fabric_builder_log_complete.write(string)
        fabric_builder_log_complete.close()
        
    def log_noTs(self, string, color = None):
        fabric_builder_log = open(MODULE_DIR + '/fabric_builder_log.txt', 'a')
        fabric_builder_log_complete = open(MODULE_DIR + '/fabric_builder_log_complete.txt', 'a')
        
        string = "{0}\n".format( string )
        sys.stderr.write(string)
        string = self.wrap(string, color)
        fabric_builder_log.write(string)
        fabric_builder_log.close()
        
        fabric_builder_log_complete.write(string)
        fabric_builder_log_complete.close()
    
    def wrap(self, string, color):
        return "<font color={0}>{1}</font>".format(color, string)


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
        LOGGER.log_noTs("-loading CVP containers: please wait...", "green")
        self.containers = {item['name'].lower():item for item in self.cvprac.api.get_containers()['data']}

    def loadConfiglets(self):
        LOGGER.log_noTs("-loading CVP configlets: please wait...", "green")
        self.configlets = {item['name'].lower():item for item in self.cvprac.api.get_configlets()['data']}
        
    def loadInventory(self):
        LOGGER.log_noTs("-loading CVP inventory: please wait...", "green")
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
            except KeyError:
                LOGGER.log_noTs("Could not find {0}".format(_search), "red")
        return devices
    
    def createConfiglet(self, configlet_name, configlet_content):
        # Configlet doesn't exist let's create one
        LOGGER.log_noTs("--creating configlet {0}; please wait...".format(configlet_name), "green")
        self.cvprac.api.add_configlet(configlet_name, configlet_content)
        return self.cvprac.api.get_configlet_by_name(configlet_name)
                
        
    def updateConfiglet(self, configlet, new_configlet_content):
        # Configlet does exist, let's update the content only if not the same (avoid empty task)
        configlet_name = configlet['name']
        LOGGER.log_noTs("--found configlet {0}".format(configlet_name), "green")

        if configlet['config'] != new_configlet_content:
            LOGGER.log_noTs("---updating configlet {0}; please wait...".format(configlet_name), "green")
            self.cvprac.api.update_configlet(new_configlet_content, configlet['key'], configlet_name)
        else:
            LOGGER.log_noTs("---nothing to do", "green")
        return self.cvprac.api.get_configlet_by_name(configlet_name)
                
    def deployDevice(self, device, container, configlets_to_deploy):
        try:
            ids = self.cvprac.api.deploy_device(device.cvp, container, configlets_to_deploy)
        except CvpApiError:
            LOGGER.log_noTs("---deploying device {0}: failed, could not get task id from CVP".format(device.hostname), "red")
        else:
            ids = ','.join(map(str, ids['data']['taskIds']))
            LOGGER.log_noTs("---deploying device {0}: {1} to {2} container".format(device.hostname, device.mgmt_ip, device.container), "green")
            LOGGER.log_noTs("---CREATED TASKS {0}".format(ids), "green")
            
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
                LOGGER.log_noTs("---applying configlets to {0}; please wait...".format(toContainer.name), "green")
                _result = self.cvprac.api.apply_configlets_to_container(app_name, toContainer, configlets)
                dest = toContainer
            else:
                #apply to device
                toDevice = getBySerial(dest)
                LOGGER.log_noTs("---applying configlets to {0}; please wait...".format(dest), "green")
                _result = self.cvprac.api.apply_configlets_to_device(app_name, toDevice.cvp, configlets) if toDevice.cvp else None
                
            if not (toDevice or toContainer):
                errorOn = [_conf['name'] for _conf in configlets]
                LOGGER.log_noTs("---failed to push {0}; {1} not found".format(','.join(errorOn), dest))
            elif _result and _result['data']['status'] == 'success':
                
                LOGGER.log_noTs("---CREATED TASKS {0}".format(','.join(map(str, _result['data']['taskIds']))), "green")
                
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
            LOGGER.log_noTs("compilation log:")
            new_configlet_content = configlet.compile(self.device)
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
                LOGGER.log_noTs("Configlet does not exist: {0}".format(name), "red")
                error += 1
            elif not assigned and match:
                if ignoreNotAssigned:
                    continue
                LOGGER.log_noTs("Configlet not assigned: {0}".format(name), "red")
                error += 1
            elif not assigned and not match:
                if ignoreNotAssigned_Mismatched:
                    continue
                LOGGER.log_noTs("Configlet does not match and is not assigned: {0}".format(name), "red")
                LOGGER.log_noTs('-'*50)
                LOGGER.log_noTs(show_diff(new_configlet_content, exists['config']))
                LOGGER.log_noTs('-'*50)
                error += 1
            elif assigned and not match:
                LOGGER.log_noTs("Configlet does not match: {0}".format(name), "red")
                LOGGER.log_noTs('-'*50)
                LOGGER.log_noTs(show_diff(new_configlet_content, exists['config']))
                LOGGER.log_noTs('-'*50)
                
                error += 1
        if not error:
            LOGGER.log_noTs("Device is consistent with CVP", "green")        
        
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
                LOGGER.log_noTs("---cannot deploy {0}; non-provisioned device with no destination container defined".format(self.device.hostname), "red")
            else:
                CVP.applyConfiglets(self.device.serialNumber, configlet_keys) 
                
        # DAY1 and DAY2 EXECUTION HAPPENS HERE
        LOGGER.log_noTs('')
        LOGGER.log_noTs("******* {0} / {1} *******".format(self.device.hostname.upper(), self.device.serialNumber.upper()))
        
        for name, configlet in self.device.to_deploy:
            
            LOGGER.log_noTs('CONFIGLET NAME: '+ name)

            LOGGER.log_noTs("compilation log:")
            new_configlet_content = configlet.compile(self.device)
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


class Switch:

    def __init__(self, params={}, cvpDevice={}):
        # list to hold leaf compiled spine underlay interface init
        self.underlay_inject = []
        self.to_deploy = []
        self.cvp = {}

        for k, v in params.items():
            setattr(self, k, v)
        
        self.hostname = searchSource('hostname', self) or searchSource('hostname', cvpDevice)
        self.serialNumber = searchSource('serialNumber', self) or searchSource('serialNumber', cvpDevice)
        
        LOGGER.log_noTs("-loading {0}".format(self.hostname or self.serialNumber), "green")
        
        MANAGER.DEVICES[self.serialNumber] = self
        MANAGER.HOST_TO_DEVICE[self.hostname] = self
        
        if self.serialNumber in searchConfig('spines'):
            self.role = 'spine'
            MANAGER.SPINES.append(self)
        else:
            self.role = 'leaf'
    
    def loadCVPRecord(self):
        self.cvp = searchSource(self.serialNumber, CVP.devices, {})
        LOGGER.log_noTs("-loading CVP record: {0}".format('success' if self.cvp else 'not found'), "green")
        if not self.cvp:
            return False
        return True
        
    def loadCVPConfiglets(self):
        success = CVP.loadDeviceConfiglets(self.serialNumber)
        LOGGER.log_noTs("-loading CVP configlets: {0}".format('success' if success else 'not found'), "green")
    
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

        return template.compile(self)

    # the property definitions should return ERROR if the search for a definition should not look in the global config
    # e.g. we do not want mlag_address to return the global config variable for every device
    # otherwise return None

    @property
    def telemetry(self):
        return Telemetry(self.serialNumber)
          
    @property
    def spine_lo0_list(self):
        return [(spine.hostname, spine.loopback0) for spine in MANAGER.SPINES if spine.loopback0]
        
    
    @property
    def spine_lo1_list(self):
        return [(spine.hostname, spine.loopback1) for spine in MANAGER.SPINES if spine.loopback1]
    
    @property
    def spine_uplinks(self):
        found = []
        try:   
            source = None
            
            if self.__context__ == 'current' and MANAGER.name == 'Underlay':
                # go this route if we are debugging the current context from the MLAG deployment profile itself but mlag_address was not explicitly defined
                source = MANAGER.current_deployment_var['device_vars']
            else:
                # otherwise always use the last deployed data
                # this is expected to be a "nested" deployment profile
                d = Deployment.objects.get(name='Underlay')

                if d.last_deployment:
                    source = d.last_deployed_var['device_vars']

            if not source:
                raise ValueError('deployment profile "Underlay" not deployed')

            for spine_serial_number, collection in source.items():
                spine_ip_index = collection['columns'].index('spine_Ip')
                spine_hostname = MANAGER.DEVICES[spine_serial_number].hostname

                for row in collection['data']:
                    spine_ip = row[spine_ip_index]
                    if row[0] == self.serialNumber and spine_ip:
                        found.append((spine_hostname, spine_ip))
            return found

        except Deployment.DoesNotExist as e:
            LOGGER.log_noTs('-exception in to_spine__hostname_ip__tuple_list: deployment profile "Underlay" not found', "red")
        except (KeyError, TypeError, ValueError) as e:
            LOGGER.log_noTs("-exception in to_spine__hostname_ip__tuple_list: {0}".format(e), "red")

        return 'STOP'

    @property
    def spine_hostname_list(self):
        return [spine.hostname for spine in MANAGER.SPINES]

    @property
    def loopback0(self):
        try:
            return self._loopback0
        except:
            pass

        try:
            source = record = None
            
            if self.__context__ == 'current' and MANAGER.name == 'Loopback':
                # go this route if we are debugging the current context from the MLAG deployment profile itself but mlag_address was not explicitly defined
                source = MANAGER.current_deployment_var['device_vars']['Tab0']
            else:
                # otherwise always use the last deployed data
                # this is expected to be a "flat" deployment profile
                d = Deployment.objects.get(name='Loopback')

                if d.last_deployment:
                    source = d.last_deployed_var['device_vars']['Tab0']

            if not source:
                raise ValueError('deployment profile "Loopback" not deployed')
            
            record = next(iter([r for r in source['data'] if r[0] == self.serialNumber]))
            lo0_index = source['columns'].index('loopback0')
            record = record[lo0_index]

            if record:
                return record

        except Deployment.DoesNotExist:
            LOGGER.log_noTs('-exception in loopback0: deployment profile "Loopback" not found', "orange")
        except StopIteration:
            LOGGER.log_noTs('-exception in loopback0: {0} not defined in "Loopback" deployment profile'.format(self.hostname), "orange")
        except (KeyError, IndexError, TypeError, ValueError) as e:
            LOGGER.log_noTs("-exception in loopback0: {0}".format(e), "orange")
        
        LOGGER.log_noTs("--trying telemetry", "orange")
        return self.telemetry['/Sysdb/ip/config/ipIntfConfig/Loopback0#addrWithMask']

    @loopback0.setter
    def loopback0(self, lo0):
        if lo0:
            self._loopback0 = lo0
    
    @property
    def loopback1(self):
        try:
            return self._loopback1
        except:
            pass

        try:
            source = record = None
            
            if self.__context__ == 'current' and MANAGER.name == 'Loopback':
                # go this route if we are debugging the current context from the MLAG deployment profile itself but mlag_address was not explicitly defined
                source = MANAGER.current_deployment_var['device_vars']['Tab0']
            else:
                # otherwise always use the last deployed data
                # this is expected to be a "flat" deployment profile
                d = Deployment.objects.get(name='Loopback')

                if d.last_deployment:
                    source = d.last_deployed_var['device_vars']['Tab0']

            if not source:
                raise ValueError('deployment profile "Loopback" not deployed')

            record = next(iter([r for r in source['data'] if r[0] == self.serialNumber]))
            lo1_index = source['columns'].index('loopback1')
            record = record[lo1_index]
            if record:
                return record

        except Deployment.DoesNotExist:
            LOGGER.log_noTs("-exception in loopback1: deployment profile not found", "orange")
        except StopIteration:
            LOGGER.log_noTs('-exception in loopback1: {0} not defined in "Loopback" deployment profile'.format(self.hostname), "orange")
        except (KeyError, IndexError, TypeError, ValueError) as e:
            LOGGER.log_noTs("-exception in loopback1: {0}".format(e), "orange")
        
        LOGGER.log_noTs("--trying telemetry", "orange")
        return self.telemetry['/Sysdb/ip/config/ipIntfConfig/Loopback1#addrWithMask']
            
    @loopback1.setter
    def loopback1(self, lo1):
        if lo1:
            self._loopback1 = lo1

    @property
    def asn(self):
        try:
            return self._asn
        except:
            pass

        try:
            source = record = None
            
            if self.__context__ == 'current' and MANAGER.name == 'Loopback':
                # go this route if we are debugging the current context from the MLAG deployment profile itself but mlag_address was not explicitly defined
                source = MANAGER.current_deployment_var['device_vars']['Tab0']
            else:
                # otherwise always use the last deployed data
                # this is expected to be a "flat" deployment profile
                d = Deployment.objects.get(name='Loopback')

                if d.last_deployment:
                    source = d.last_deployed_var['device_vars']['Tab0']

            if not source:
                raise ValueError('deployment profile "Loopback" not deployed')

            record = next(iter([r for r in source['data'] if r[0] == self.serialNumber]))
            asn_index = source['columns'].index('asn')
            record = record[asn_index]
            if record:
                return record

        except Deployment.DoesNotExist:
            LOGGER.log_noTs('-exception in asn: deployment profile "Loopback" not found', "orange")
        except StopIteration:
            LOGGER.log_noTs('-exception in asn: {0} not defined in "Loopback" deployment profile'.format(self.hostname), "orange")
        except (KeyError, IndexError, TypeError, ValueError) as e:
            LOGGER.log_noTs("-exception in asn: {0}".format(e), "orange")
        
        LOGGER.log_noTs("--trying telemetry", "orange")
        return self.telemetry['/Sysdb/routing/bgp/config#asNumber']

    @asn.setter
    def asn(self, asn):
        if asn:
            self._asn = asn

    @property
    def mlag_address(self):
        # this will be available only if we are in a template context which defines mlag_address directly in the device variable table in the "current" context
        try:
            return self._mlag_address
        except:
            pass
        
        # try to find data in the MLAG deployment profile
        try:
            source = record = None
            
            if self.__context__ == 'current' and MANAGER.name == 'MLAG':
                # go this route if we are debugging the current context from the MLAG deployment profile itself but mlag_address was not explicitly defined
                source = MANAGER.current_deployment_var['device_vars']['Tab0']
                record = next(iter([r for r in source['data'] if r[0] == self.serialNumber]))
            else:
                # otherwise always use the last deployed data
                # this is expected to be a "flat" deployment profile
                d = Deployment.objects.get(name='MLAG')

                if d.last_deployment:
                    source = d.last_deployed_var['device_vars']['Tab0']
                    record = next(iter([r for r in source['data'] if r[0] == self.serialNumber]))

                    #maybe it's defined in the last deployment?
                    mlag_address_index = source['columns'].index('mlag_address')
                    mlag_address = record[mlag_address_index]
                    # defined address expected to be properly formatted with prefixlen
                    if mlag_address:
                        return mlag_address
            
            if not source:
                raise ValueError('deployment profile "MLAG" not deployed')
            
            # not directly defined, not in last deployment, fall back to neighbor
            mlag_neighbor_index = source['columns'].index('mlag_neighbor')
            mlag_neighbor_hostname = record[mlag_neighbor_index]

            # break tie via hostname
            # this will either get the static mlag_address in the MLAG deployment profile or global space
            mlag_network = searchConfig('mlag_network')

            if mlag_neighbor_hostname and mlag_network:
                mlag_network = ip_network(mlag_network if '/' in mlag_network else mlag_network + '/31', False)
                if self.hostname.lower() < mlag_neighbor_hostname.lower():
                    return str(next(mlag_network.hosts())) + '/' + str(mlag_network.prefixlen)
                else:
                    return str(list(mlag_network.hosts())[1]) + '/' + str(mlag_network.prefixlen)

        except Deployment.DoesNotExist:
            LOGGER.log_noTs('-exception in mlag_address: deployment profile "MLAG" not found', "orange")
        except StopIteration:
            LOGGER.log_noTs('-exception in mlag_address: not defined in "MLAG" deployment profile', "orange")
        except (KeyError, IndexError, TypeError, ValueError) as e:
            LOGGER.log_noTs("-exception in mlag_address: {0}".format(e), "orange")

        mlag_vlan = searchConfig('mlag_vlan', default='4094')
        LOGGER.log_noTs("--trying telemetry", "orange")
        return self.telemetry['/Sysdb/ip/config/ipIntfConfig/Vlan{0}/addrWithMask'.format(mlag_vlan)]


    @mlag_address.setter
    def mlag_address(self, mlag_address):
        if mlag_address:
            self._mlag_address = mlag_address if '/' in mlag_address else mlag_address + '/31'

    @property
    def mlag_peer_address(self):
        try:
            return self._mlag_peer_address
        except:
            pass

        try:
            source = record = None
            
            if self.__context__ == 'current' and MANAGER.name == 'MLAG':
                # go this route if we are debugging the current context from the MLAG deployment profile itself but mlag_address was not explicitly defined
                source = MANAGER.current_deployment_var['device_vars']['Tab0']
                record = next(iter([r for r in source['data'] if r[0] == self.serialNumber]))
            else:
                # otherwise always use the last deployed data
                # this is expected to be a "flat" deployment profile
                d = Deployment.objects.get(name='MLAG')

                if d.last_deployment:
                    source = d.last_deployed_var['device_vars']['Tab0']
                    record = next(iter([r for r in source['data'] if r[0] == self.serialNumber]))
                    mlag_peer_address_index = source['columns'].index('mlag_peer_address')
                    mlag_peer_address = record[mlag_peer_address_index]

                    # defined peer address
                    if mlag_peer_address:
                        return mlag_peer_address

            if not source:
                raise ValueError('deployment profile "MLAG" not deployed')


            mlag_neighbor_index = source['columns'].index('mlag_neighbor')
            mlag_neighbor_hostname = record[mlag_neighbor_index]
            # break tie via hostname
            mlag_address = self.mlag_address
            if mlag_neighbor_hostname and mlag_address:
                mlag_network = ip_network(mlag_address, False)
                available_hosts = set(list(mlag_network.hosts()))
                available_hosts.remove(ip_address(mlag_address.split('/')[0]))
                return str(next(iter(available_hosts)))

        except Deployment.DoesNotExist:
            LOGGER.log_noTs('-exception in mlag_peer_address: deployment profile "MLAG" not found', "orange")
        except StopIteration:
            LOGGER.log_noTs('-exception in mlag_peer_address: not defined in "MLAG" deployment profile', "orange")
        except (KeyError, IndexError, TypeError, ValueError) as e:
            LOGGER.log_noTs("-exception in mlag_peer_address: {0}".format(e), "orange")

        LOGGER.log_noTs("--trying telemetry", "orange")
        return self.telemetry['/Sysdb/mlag/config/peerAddress']

    @mlag_peer_address.setter
    def mlag_peer_address(self, mlag_peer_address):
        if mlag_peer_address:
            self._mlag_peer_address = mlag_peer_address
        
class Configlet:
    jinjaenv = Environment(trim_blocks=True, lstrip_blocks=True)
    jinjaenv.filters = {**jinjaenv.filters, **FilterModule().filters()}

    def __init__(self, name, template, params = {}):        
        self.name = name
        self.template = template
        for k, v in params.items():
            setattr(self, k, v)
        
    def compile(self, source):
        
        try:
            parsedTemplate = Configlet.jinjaenv.parse(self.template)
            valueDict = buildValueDict(source, parsedTemplate)
            valueDict.pop('error')
            template = Configlet.jinjaenv.from_string(self.template).render(**valueDict)
        except Exception as e:
            LOGGER.log_noTs("-exception compiling: {0}".format(e), "red")
            return ''
        return template


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

            if self.last_deployment:
                if self.mode == "nested":
                    try:
                        for spineSerialNumber, collection in d['device_vars'].items():
                            #spine = self.previous_device_vars.setdefault(spineSerialNumber, {})

                            for leaf_data_row in collection['data']:

                                if not any(leaf_data_row):
                                    continue

                                # row[0] -> serialNumber
                                # these will be individual leaf records with spine references
                                leaf = self.previous_device_vars.setdefault(leaf_data_row[0], {})
                                # sN and host indexes are known
                                leaf['serialNumber'] = leaf_data_row[0]
                                leaf['hostname'] = leaf_data_row[1]
                                
                                # init rows to list and prevent overwrite and allow appending to
                                leaf_row = leaf.setdefault('data', [])
                                # for multiple leaf records in each spine tab append the data to 'rows' with sN and host stripped out
                                rest_of_leaf_data = dict(zip(collection['columns'][2:], leaf_data_row[2:]))
                                # each row will have a parent spine pointer
                                rest_of_leaf_data['spine'] = spineSerialNumber
                                leaf_row.append(rest_of_leaf_data)
                                

                    except KeyError as e:
                        LOGGER.log_noTs("-exception in manager init: {0}".format(e), "red")
                else:
                    try:
                        for data_row in d['device_vars']['Tab0']['data']:
                            if not any(data_row):
                                continue
                            _vars = OrderedDict(zip(d['device_vars']['Tab0']['columns'], data_row))
                            _vars['__link__'] = data_row
                            self.previous_device_vars[_vars['serialNumber']] = _vars
                    except KeyError as e:
                        LOGGER.log_noTs("-exception in manager init: {0}".format(e), "red")
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
                for spineSerialNumber, collection in d['device_vars'].items():
                    # spine = self.current_device_vars.setdefault(spineSerialNumber, {})

                    for leaf_data_row in collection['data']:

                        if not any(leaf_data_row):
                            continue

                        # row[0] -> serialNumber
                        # these will be individual leaf records with spine references
                        leaf = self.current_device_vars.setdefault(leaf_data_row[0], {})
                        # sN and host indexes are known
                        leaf['serialNumber'] = leaf_data_row[0]
                        leaf['hostname'] = leaf_data_row[1]
                        
                        # init rows to list and prevent overwrite and allow appending to
                        leaf_row = leaf.setdefault('data', [])
                        # for multiple leaf records in each spine tab append the data to 'rows' with sN and host stripped out
                        rest_of_leaf_data = dict(zip(collection['columns'][2:], leaf_data_row[2:]))
                        # each row will have a parent spine pointer
                        rest_of_leaf_data['spine'] = spineSerialNumber
                        leaf_row.append(rest_of_leaf_data)
                            

            except KeyError as e:
                LOGGER.log_noTs("-exception in manager init: {0}".format(e), "red")
        else:
            try:
                for data_row in d['device_vars']['Tab0']['data']:
                    if not any(data_row):
                        continue
                    _vars = OrderedDict(zip(d['device_vars']['Tab0']['columns'], data_row))
                    _vars['__link__'] = data_row
                    self.current_device_vars[_vars['serialNumber']] = _vars
            except KeyError as e:
                LOGGER.log_noTs("-exception in manager init: {0}".format(e), "red")

    def stageDeployment(self, d, loadInventory=True,  loadConfiglets=True, loadContainers=False, skipCvp=False):
        LOGGER.log_noTs("-initializing internal state: please wait...", "blue")

        self.COMPILE_FOR = []
        self.DEVICES = {}
        self.SPINES = []
        self.HOST_TO_DEVICE = {}

        CVP.loadInventory() if loadInventory else None
        CVP.loadConfiglets() if loadConfiglets else None
        CVP.loadContainers() if loadContainers else None

        if d == 'current':
            _d = self.current_deployment_var
            device_vars = self.current_device_vars
        elif d == 'last':
            _d = self.last_deployed_var
            device_vars = self.previous_device_vars

        self.CONFIG = {'global': Global_Config.objects.get(name='master').params}

        # fix disjointed keys/values from UI
        variables = {}
        if 'variables' in _d:
            variables = dict(zip(_d['variables']['keys'], _d['variables']['values']))
        
        iterables = {}
        if 'iterables' in _d:
            for tab, collection in _d['iterables'].items():
                tab = tab.replace('#','')
                iterables[tab] = []
                # collection is an object with keys: data, columns
                for row in collection['data']:
                    # skip blank rows
                    if not any(row):
                        continue
                    iterables[tab].append(dict(zip(collection['columns'], row)))   

        if self.mode == 'nested':

            for serialNumber in searchConfig('spines'):
                _cvp_record = CVP.getBySerial(serialNumber)
                self.COMPILE_FOR.append(Switch(params = {'data':[], '__context__':d}, cvpDevice = _cvp_record if _cvp_record else {'serialNumber': serialNumber, 'hostname': 'NOT_IN_CVP'}))

            for serialNumber, data in device_vars.items():

                for row in data['data']:
                    row['spine'] = getBySerial(row['spine'])

                _leaf = Switch(data)
                self.COMPILE_FOR.append(_leaf)

                for row in _leaf.data:
                    row['leaf'] = _leaf

                for spine in self.SPINES:
                    for row in _leaf.data:
                        if row['spine'] == spine:
                            spine.data.append(row)
            
            if not skipCvp:
                for device in self.COMPILE_FOR:
                    if device.loadCVPRecord():
                        device.loadCVPConfiglets()


        else:
            for serialNumber in searchSource('compile_for', _d, []):
                device = Switch({**searchSource(serialNumber, device_vars, {}), '__context__':d})

                if not skipCvp and device.loadCVPRecord():
                    device.loadCVPConfiglets()
                self.COMPILE_FOR.append(device)
            
            # make sure unselected spines are also loaded
            for serialNumber in searchConfig('spines'):
                if not getBySerial(serialNumber):
                    _cvp_record = CVP.getBySerial(serialNumber)
                    Switch(params = {'__context__':d}, cvpDevice = _cvp_record if _cvp_record else {'serialNumber': serialNumber, 'hostname': 'NOT_IN_CVP'})

        self.CONFIG = {**variables, **iterables, **self.CONFIG}
        self.CONFIG['selected_templates'] = searchSource('selected_templates', _d, [])
        self.CONFIG['name'] = self.name
                
    def stageTasks(self):
        LOGGER.log_noTs("-staging builder tasks", "blue")
            
        selected_templates = searchConfig('selected_templates')
        spines = searchConfig('spines')

        if not (spines):
            LOGGER.log('-spines must be defined in the global master config; aborting', "red")
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
            LOGGER.log("-last deployment record does not exist; aborting", "red")
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

            old_compile_for = []
            new_compile_for = []

            for spine_serial_number, collection in self.last_deployed_var['device_vars'].items():
                old_compile_for.append(spine_serial_number)

                for leaf_serial_number in [r[0] for r in collection['data'] if r[0]]:
                    old_compile_for.append(leaf_serial_number)

            for spine_serial_number, collection in self.current_deployment_var['device_vars'].items():
                new_compile_for.append(spine_serial_number)

                for leaf_serial_number in [r[0] for r in collection['data'] if r[0]]:
                    new_compile_for.append(leaf_serial_number)

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
                        # this updates the actual row data in self.last_deployed_var [:-1] removes __link__ from ordered dict
                        # in turn if all is well this updates in the DB
                        link[:] = list(varsDict)[:-1]
                except (CvpApiError) as e:
                    LOGGER.log_noTs("--exception moving containers, please try again; aborting", "red")
                    LOGGER.log_noTs("-exception: {0}".format(e), "red")
                    return False
            
            if toRemove:
                try:
                    if device:
                        LOGGER.log_noTs('--unassigning configlets from {0}'.format(searchSource('hostname', device, 'no_hostname_error')), "green")
                        CVP.cvprac.api.remove_configlets_from_device('builder', device, toRemove)
                    for c in toRemove:
                        LOGGER.log_noTs('--removing configlet {0}'.format(searchSource('name', c, 'no_name_error')), "green")
                        CVP.cvprac.api.delete_configlet(c['name'], c['key'])
                    if serialNumber in removeDevices:
                        self.last_deployed_var['compile_for'].remove(serialNumber)
                        row = list(searchSource(serialNumber, self.previous_device_vars, {}).values())
                        #row = [row for row in self.last_deployed_var['device_vars']['Tab0']['data'] if row[0] == serialNumber][0]
                        self.last_deployed_var['device_vars']['Tab0']['data'].remove(row)
                    
                except (CvpApiError) as e:
                    
                    LOGGER.log_noTs("--exception deleting configlets (979), please try again; aborting", "red")
                    LOGGER.log_noTs("-exception: {0}".format(e), "red")
                    return False

                
            
        
        for id in removeTemplates:
            self.last_deployed_var['selected_templates'].remove(id)
        
        #this makes the last_deployed state true with what was removed during this operation to match current state in case of further exceptions and subsequent attempts to update which will not verify.
        try:    
            self.dbRecord.last_deployed_var = self.last_deployed_var
            self.dbRecord.save()
        except dbError as e:
            LOGGER.log_noTs('--error updating last deployment in DB to reflect removed CVP records (line1016); aborting', "red")
            LOGGER.log_noTs('--please update the DB manually, last deployment should be:','red')
            LOGGER.log_noTs("-exception: {0}".format(e), "red")
            LOGGER.log_noTs(json.dumps(self.last_deployed_var))
            LOGGER.log_noTs('')
            
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
                LOGGER.log_noTs('-error saving current deployment (saveAndDeploy); aborting', "red")
                LOGGER.log_noTs('-following should manually pushed to DB as the current deployment:', "red")
                LOGGER.log_noTs("-exception: {0}".format(e), "red")
                LOGGER.log_noTs(json.dumps(self.current_deployment_var, indent=4))
                LOGGER.log_noTs('')
                return False
            
            # when sync returns it should have removed all configlets and updated last_deployed_var to reflect those removals
            # if it failed on any of the cvprac calls then the configlets might have been partially removed from CVP but not reflected in the DB for last_deployed_vars
            # worst case scenario we can now rerun and remove non existent cvp configlets and finally update the db
            if self._verifyLastDeployment() and self._sync():
                
                try:
                    self.dbRecord.last_deployment = int(time.time())
                    self.dbRecord.last_deployed_var = self.current_deployment_var
                    self.dbRecord.save()
                except dbError as e:
                    LOGGER.log_noTs('-error pushing current deployment vars into history (line1016); aborting', "red")
                    LOGGER.log_noTs('-following should manually pushed to DB as current/last after which \"Sync\" should fix state:', "red")
                    LOGGER.log_noTs("-exception: {0}".format(e), "red")
                    LOGGER.log_noTs(json.dumps(self.current_deployment_var,indent=4))
                    LOGGER.log_noTs('')
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
                LOGGER.log_noTs('-error creating deployment (first deployment); aborting', "red")
                LOGGER.log_noTs("-exception: {0}".format(e), "red")
                LOGGER.log_noTs(json.dumps(self.fromUI,indent=4))
                return False
            
            if self._verifyPreDeployment():
                try:
                    self.dbRecord.last_deployment = int(time.time())
                    self.dbRecord.last_deployed_var = self.current_deployment_var
                    self.dbRecord.save()
                except dbError as e:
                    LOGGER.log_noTs('-error creating deployment (first deployment); aborting', "red")
                    LOGGER.log_noTs("-exception: {0}".format(e), "red")
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
        
def show_diff(text, n_text):
    """
    http://stackoverflow.com/a/788780
    Unify operations between two compared strings seqm is a difflib.
    SequenceMatcher instance whose a & b are strings
    """
    seqm = difflib.SequenceMatcher(None, text, n_text)
    output= []
    previous = ''
    for opcode, a0, a1, b0, b1 in seqm.get_opcodes():
        if opcode == 'equal':
            previous = str(seqm.a[a0:a1])
        elif opcode == 'insert':
            output.append('...' + previous[-33:] + "<font color=red style='background:chartreuse'>^" + seqm.b[b0:b1] + "</font>")
        elif opcode == 'delete':
            output.append('...' + previous[-33:] + "<font color=blue style='background:chartreuse'>^" + seqm.a[a0:a1] + "</font>")
        elif opcode == 'replace':
            # seqm.a[a0:a1] -> seqm.b[b0:b1]
            output.append('...' + previous[-33:] + "<font color=green style='background:chartreuse'>^" + seqm.b[b0:b1] + "</font>")
        else:
            raise RuntimeError("unexpected opcode")
    return ''.join(output)

# get if dict, getattr if else
def searchSource(key, source, default = None):
    _type = type(source)
    if _type is dict or _type is OrderedDict:
        return source.get(key, default)
    elif _type is list:
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
    if not config:
        try:
            config = MANAGER.CONFIG['global'][key]
        except:
            return config

    _type = type(config)
    if _type is list or _type is dict or _type is OrderedDict:
        return config
    
    if config.lower() == 'true':
        return True
    if config.lower() == 'false':
        return False

    return config

def getKeyDefinition(key, source):
    toReturn = searchSource(key, source) or searchConfig(key)
    if toReturn == 'STOP' or not toReturn:
        LOGGER.log_noTs("--{0} not found".format(key), "red")

    return '' if toReturn == 'STOP' else toReturn

def buildValueDict(source, template):
    valueDict = {}
    valueDict['error'] = []
    
    keys = meta.find_undeclared_variables(template)

    for key in keys:
        #check if dict already has defined
        if valueDict.get(key, None):
            continue
        
        defined = getKeyDefinition(key, source)
        if not defined:
            valueDict['error'].append(key)
        else:
            valueDict[key] = defined
    print(valueDict)
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
                        LOGGER.log("Failed to init CVP", "red")
                        LOGGER.log_noTs("-exception: {0}".format(e), "red")
                        continue
                    
                    
                    
                    getattr(MANAGER, action)()
                    
                    send_message(conn, {"status":"success"})




if __name__ == '__main__':
    main()