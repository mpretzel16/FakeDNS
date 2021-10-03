import psycopg2
import sqlalchemy.orm

from random import randint
import json
from ip_generation import IpGeneration
from sqlalchemy import func
from sqlalchemy import or_
from database import Database


class GenerateDnsEntry:
    ip_gen = IpGeneration()
    dict_server_config: dict
    def __init__(self, dict_server_config: dict):
        self.dict_server_config = dict_server_config

    def start(self, db_session: sqlalchemy.orm.Session, entry_text):
        dict_return = dict()
        dict_return['bool_error'] = False
        dict_return['str_status'] = None
        dict_return['bool_already_exists'] = False
        dict_return['bool_invalid_tld'] = False
        dict_return['ipv4'] = None
        dict_return['ipv6'] = None
        try:
            dict_tld_query = self.build_tld_where_statement(entry_text)
            if dict_tld_query['bool_error']:
                dict_return['bool_error'] = True
                dict_return['str_status'] = dict_tld_query['str_status']
                return dict_return
            lookup_tld = db_session.query(func.count(Database.Tld_Listing.domain))\
                .filter(Database.Tld_Listing.domain.in_(dict_tld_query['arr_tld'])).scalar()
            if lookup_tld == 0:
                dict_return['bool_invalid_tld'] = True
                return dict_return
            try:
                dns_authority = Database.Dns_Authoritative()
                dns_authority.bool_authoritative = True
                dns_authority.entry_text = entry_text
                db_session.add(dns_authority)
                db_session.commit()
            except sqlalchemy.exc.IntegrityError as exec:
                if "duplicate" in str(exec).lower():
                    dict_return['bool_already_exists'] = True
                    return dict_return
                else:
                    dict_return['bool_error'] = True
                    dict_return['str_status'] = str(exec)
                    return dict_return
            except Exception as e:
                if "duplicate" in str(e).lower():
                    dict_return['bool_already_exists'] = True
                    return dict_return
                dict_return['bool_error'] = True
                dict_return['str_status'] = str(e)
                return dict_return

            country_to_use: Database.Country_List
            try:
                result_country_list = db_session.query(Database.Country_List).all()
                arr_country_code = []
                for country in result_country_list:
                    country: Database.Country_List = country
                    arr_country_code.append(str(country.country_id).lower())
                country_to_use = result_country_list[randint(0, len(result_country_list) - 1)]
            except Exception as e:
                dict_return['bool_error'] = True
                dict_return['str_status'] = str(e)
                return dict_return
            result_ipv4: Database.IPv4_Networks
            try:
                result_ipv4 = db_session.query(Database.IPv4_Networks)\
                    .filter(Database.IPv4_Networks.country_id == country_to_use.country_id).all()
            except Exception as e:
                dict_return['bool_error'] = True
                dict_return['str_status'] = str(e)
                return dict_return
            result_ipv6: Database.IPv6_Networks
            try:
                result_ipv6 = db_session.query(Database.IPv6_Networks) \
                    .filter(Database.IPv6_Networks.country_id == country_to_use.country_id).all()
            except Exception as e:
                dict_return['bool_error'] = True
                dict_return['str_status'] = str(e)
                return dict_return
            v4_subnet: Database.IPv4_Networks
            v6_subnet: Database.IPv6_Networks
            v4_subnet = result_ipv4[randint(0, len(result_ipv4) - 1)]
            v6_subnet = result_ipv6[randint(0, len(result_ipv6) - 1)]
            ips = self.ip_gen.get_ips(v4_subnet.network, v6_subnet.network)
            v4_entry = {"ip": ips['ipv4']}
            v6_entry = {"ip": ips['ipv6']}
            str_ptr_ipv4 = "{}.in-addr.arpa.".format(self.reverse_ip_address(ips['ipv4']))
            str_ptr_ipv6 = "{}.ip6.arpa.".format(self.reverse_ip_address(ips['ipv6']))
            a_dns_entry = Database.Dns_Entries()
            a_dns_entry.entry_text = entry_text
            a_dns_entry.record_type = 1
            a_dns_entry.record_data = v4_entry
            aaa_dns_entry = Database.Dns_Entries()
            aaa_dns_entry.entry_text = entry_text
            aaa_dns_entry.record_type = 28
            aaa_dns_entry.record_data = v6_entry
            ptr_a_dns_entry = Database.Dns_Entries()
            ptr_a_dns_entry.entry_text = str_ptr_ipv4
            ptr_a_dns_entry.record_type = 12
            ptr_a_dns_entry.record_data = {"domain_name": entry_text}
            ptr_aaa_dns_entry = Database.Dns_Entries()
            ptr_aaa_dns_entry.entry_text = str_ptr_ipv6
            ptr_aaa_dns_entry.record_type = 12
            ptr_aaa_dns_entry.record_data = {"domain_name": entry_text}
            ns_dns_entry = Database.Dns_Entries()
            ns_dns_entry.entry_text = entry_text
            ns_dns_entry.record_type = 2
            ns_dns_entry.record_data = {"server_id": self.dict_server_config['id']}
            db_session.bulk_save_objects([a_dns_entry, aaa_dns_entry, ptr_a_dns_entry, ptr_aaa_dns_entry, ns_dns_entry])
            db_session.commit()
            dict_return['ipv4'] = ips['ipv4']
            dict_return['ipv6'] = ips['ipv6']
        except Exception as e:
            dict_return['bool_error'] = True
            dict_return['str_status'] = str(e)
        return dict_return

    @staticmethod
    def build_tld_where_statement(query_string):
        dict_return = dict()
        dict_return['bool_error'] = False
        dict_return['str_status'] = None
        dict_return['str_where_statement'] = None
        dict_return['arr_tld'] = []
        try:
            arr_splits_search = []
            arr_query_split = query_string.split('.')
            arr_query_split.pop(0)
            arr_query_split.remove("")
            #str_query = ""
            int_current_query = 1
            int_total_query = len(arr_query_split)
            while int_current_query <= int_total_query:
                if int_current_query == 1:
                    str_join = ".".join(arr_query_split)
                    str_join = ".{}.".format(str_join)
                    arr_splits_search.append(str_join)
                    #str_query = "WHERE domain = %s"
                else:
                    arr_query_split.pop(0)
                    str_join = ".".join(arr_query_split)
                    str_join = ".{}.".format(str_join)
                    arr_splits_search.append(str_join)
                    #str_query = "{} OR domain = %s".format(str_query)
                int_current_query += 1
            dict_return['arr_tld'] = arr_splits_search
            dict_return['str_where_statement'] = None
        except Exception as e:
            print(e)
            dict_return['bool_error'] = True
            dict_return['str_status'] = str(e)
        return dict_return

    @staticmethod
    def reverse_ip_address(str_ip_address: str):
        if "." in str_ip_address:
            arr_ip = str_ip_address.split(".")
            arr_ip.reverse()
            str_ip_address = ".".join(arr_ip)
            return str_ip_address
        else:
            arr_ip = str_ip_address.split(":")
            str_ip_address = "".join(arr_ip)
            arr_ip = list(str_ip_address)
            arr_ip.reverse()
            str_ip_address = ".".join(arr_ip)
            return str_ip_address
