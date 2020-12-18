from postgres_functions import PostgresFunctions
from random import randint
import json
from ip_generation import IpGeneration


class GenerateDnsEntry:
    postgres = PostgresFunctions()
    ip_gen = IpGeneration()

    def start(self, entry_text, connection):
        print("Start DNS Entry Creation")
        dict_return = dict()
        dict_return['ipv4'] = None
        dict_return['ipv6'] = None
        arr_country_code = []
        result_country_list = self.postgres.get_country_listing(connection)
        country_list = json.loads(result_country_list['data'])
        for country in country_list:
            arr_country_code.append(str(country['country_code']).lower())
        country_to_use = country_list[randint(0, len(result_country_list) - 1)]
        result_ipv4 = self.postgres.get_ipv4_subnet_listing(connection, country_to_use['country_id'])
        result_ipv6 = self.postgres.get_ipv6_subnet_listing(connection, country_to_use['country_id'])

        v4_subnets = json.loads(result_ipv4['data'])
        v6_subnets = json.loads(result_ipv6['data'])
        v4_subnet = v4_subnets[randint(0, len(v4_subnets) - 1)]
        v6_subnet = v6_subnets[randint(0, len(v6_subnets) - 1)]
        ips = self.ip_gen.get_ips(v4_subnet['network'], v6_subnet['network'])
        v4_entry = {"ip": ips['ipv4']}
        v6_entry = {"ip": ips['ipv6']}
        self.postgres.add_dns_entry(connection, entry_text, 1, v4_entry)
        self.postgres.add_dns_entry(connection, entry_text, 28, v6_entry)
        self.postgres.add_to_authority(connection, entry_text)
        dict_return['ipv4'] = ips['ipv4']
        dict_return['ipv6'] = ips['ipv6']
        return dict_return




    # def get_ipv4(self):
