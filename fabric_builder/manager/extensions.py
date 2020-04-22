from ipaddress import ip_address

class SwitchBase():
    
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
    def spine_lo0_list(self):
        d = self.deployments.objects.filter(name='Loopback')[0]
        spines = self.MANAGER.CONFIG['global']['spines']
        if d and d.last_deployment:
            normalized = [row for row in d.last_deployed_var['device_vars']['Tab0']['data'] if row[0] in spines]
            lo0_index = d.last_deployed_var['device_vars']['Tab0']['columns'].index('loopback0')
            return list(map(lambda row: row[lo0_index], normalized))
        else:
            return []
        
    
    @property
    def spine_lo1_list(self):
        d = self.deployments.objects.filter(name='Loopback')[0]
        spines = self.MANAGER.CONFIG['global']['spines']
        if d and d.last_deployment:
            normalized = [row for row in d.last_deployed_var['device_vars']['Tab0']['data'] if row[0] in spines]
            lo0_index = d.last_deployed_var['device_vars']['Tab0']['columns'].index('loopback0')
            return list(map(lambda row: row[lo0_index], normalized))
        else:
            return []
    
    @property
    def spine_ipv4_list(self):
        d = self.deployments.objects.filter(name='Underlay')[0]
        if d and d.last_deployment:
            found = []
            for collection in d.last_deployed_var['device_vars'].values():
                spineip_index = collection['columns'].index('spine_Ip')
                for row in collection['data']:
                    if row[0] == self.serialNumber:
                        found.append(row[spineip_index])

            return [ip for ip in found if ip]
        else:
            return []

    @property
    def spine_hostname_list(self):
        return [spine.hostname for spine in self.MANAGER.SPINES]