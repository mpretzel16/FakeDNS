from postgres_functions import PostgresFunctions
from random import randint
import json
from ip_generation import IpGeneration


class GenerateDnsEntry:
    postgres = PostgresFunctions()
    ip_gen = IpGeneration()

    def start(self, conn, entry_text):

        #print("Start DNS Entry Creation")
        dict_return = dict()
        dict_return['bool_error'] = False
        dict_return['str_status'] = None
        dict_return['bool_already_exists'] = False
        dict_return['ipv4'] = None
        dict_return['ipv6'] = None
        try:
            dict_result_add_to_authority = self.postgres.add_to_authority(conn, entry_text)
            if dict_result_add_to_authority['bool_error']:
                dict_return['bool_error'] = True
                dict_return['str_status'] = dict_result_add_to_authority['str_status']
                return dict_return
            if dict_result_add_to_authority['bool_inserted']:
                arr_country_code = []
                result_country_list = self.postgres.get_country_listing(conn)
                country_list = json.loads(result_country_list['data'])
                for country in country_list:
                    arr_country_code.append(str(country['country_code']).lower())
                country_to_use = country_list[randint(0, len(result_country_list) - 1)]
                result_ipv4 = self.postgres.get_ipv4_subnet_listing(conn, country_to_use['country_id'])
                result_ipv6 = self.postgres.get_ipv6_subnet_listing(conn, country_to_use['country_id'])

                v4_subnets = json.loads(result_ipv4['data'])
                v6_subnets = json.loads(result_ipv6['data'])
                v4_subnet = v4_subnets[randint(0, len(v4_subnets) - 1)]
                v6_subnet = v6_subnets[randint(0, len(v6_subnets) - 1)]
                ips = self.ip_gen.get_ips(v4_subnet['network'], v6_subnet['network'])
                v4_entry = {"ip": ips['ipv4']}
                v6_entry = {"ip": ips['ipv6']}
                # print('add {}'.format(str(ips)))
                self.postgres.add_dns_entry(conn, entry_text, 1, v4_entry)
                self.postgres.add_dns_entry(conn, entry_text, 28, v6_entry)
                dict_return['ipv4'] = ips['ipv4']
                dict_return['ipv6'] = ips['ipv6']
            elif dict_result_add_to_authority['bool_already_exists']:
                dict_return['bool_already_exists'] = True

        except Exception as e:
            dict_return['bool_error'] = True
            dict_return['str_status'] = str(e)
            print(e)
        return dict_return




    # def get_ipv4(self):
