from ip2geotools.databases.noncommercial import DbIpCity
from datetime import datetime as dt
import json

class IPInfo:
    # Lists should use lowercased strings.
    def __init__(self, api_key='free', log_lookups=True,
                    countries_search_list=[], cities_search_list=[], regions_search_list=[]):
        if log_lookups:
            self.lookups = { }
            self.log_lookups = True
 
        self.api_key=api_key
        self.countries_search_list = countries_search_list
        self.cities_search_list = cities_search_list
        self.regions_search_list = regions_search_list

    def _gen_lookup_id(self, ip_addr):
        return str(ip_addr) + '-' + str(dt.now().isoformat())

    def _log_lookup(self, iplocation, keep=False):
        id = self._gen_lookup_id(iplocation.ip_address)
        lookup = json.loads(iplocation.to_json())
        try:
            lookup['countries_search_list_match'] = iplocation.country.lower() in self.countries_search_list
            lookup['cities_search_list_match'] = iplocation.city.lower() in self.cities_search_list
            lookup['regions_search_list_match'] = iplocation.region.lower() in self.regions_search_list
        except: # Location was not found so the country, city, and region are null (calling .lower() => exception):
            lookup['countries_search_list_match'] = False
            lookup['cities_search_list_match'] = False
            lookup['regions_search_list_match'] = False
        
        log_id = 'None'
        if keep:
            self.lookups[id] = lookup
            log_id = id

        return id, lookup

    def pop_from_log(self, id):
        return self.lookups.pop(id)

    def lookup(self, ip_address):
        iplocation = DbIpCity.get(str(ip_address), api_key=self.api_key)
        return self._log_lookup(iplocation, keep=self.log_lookups)