from ipaddress import ip_address

class SwitchBase():

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
    def test(self):
        return "whoaman"
    
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
        template = [t for t in self.MANAGER.TEMPLATES.values() if t.name == "Underlay"][0]
        compiled = []
        for data in self.underlay_inject:
            if self.role == "spine":
                ipAddress = ip_address(data['spine_Ip'])
                args = {
                    "interface" : data['spine_Int'],
                    "address" : ipAddress,
                    "interface_speed" : data['speed'],
                    "description" : "TO-{0}-UNDERLAY Ethernet{1}".format(data['hostname'], data['leaf_Int'])
                }
            elif self.role == 'leaf':
                ipAddress = ip_address(data['spine_Ip'])
                args = {
                    "interface" : data['leaf_Int'],
                    "address" : ipAddress + 1,
                    "interface_speed" : data['speed'],
                    "description" : "TO-{0}-UNDERLAY Ethernet{1}".format(data['spine'].hostname, data['spine_Int'])
                }
            _cc = template.compile(args)
            compiled.append(_cc[0])
        
        return "\n".join(compiled)

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