import ipaddress
import random


class IpGeneration:

    def get_ips(self, str_ipv4_subnet=None, str_ipv6_subnet=None):
        dict_ips = dict()
        dict_ips['bool_error'] = False
        dict_ips['str_status'] = None
        dict_ips['ipv4'] = None
        dict_ips['ipv6'] = None
        try:
            if str_ipv4_subnet is not None:
                dict_result = self.__ip_from_subnet__(str_ipv4_subnet)
                if not dict_result['bool_error']:
                    dict_ips['ipv4'] = dict_result['ip']
                else:
                    dict_ips['bool_error'] = True
                    dict_ips['str_status'] = dict_result['str_status']
                del dict_result
            if str_ipv6_subnet is not None:
                dict_result = self.__ip_from_subnet__(str_ipv6_subnet)
                if not dict_result['bool_error']:
                    dict_ips['ipv6'] = dict_result['ip']
                else:
                    dict_ips['bool_error'] = True
                    dict_ips['str_status'] = dict_result['str_status']
                del dict_result
        except Exception as e:
            dict_ips['bool_error'] = True
            dict_ips['str_status'] = str(e)
        return dict_ips

    @staticmethod
    def __ip_from_subnet__(str_subnet):
        dict_return = dict()
        dict_return['bool_error'] = False
        dict_return['str_status'] = None
        dict_return['ip'] = None
        try:
            ips = ipaddress.ip_network(str_subnet)
            int_total_ips = int(ips.num_addresses) - 1
            int_ip_to_get = random.randint(0, int_total_ips)
            dict_return['ip'] = str(ips[int_ip_to_get])
        except Exception as e:
            dict_return['bool_error'] = True
            dict_return['str_status'] = str(e)
        return dict_return