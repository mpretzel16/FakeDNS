import os

import toml
from scapy.layers.dns import DNS, DNSQR, DNSRR, dnsqtypes
from socket import AF_INET, SOCK_DGRAM, socket
from traceback import print_exc
from postgres_functions import PostgresFunctions
import threading
import json
import time
from generate_dns_entry import GenerateDnsEntry
from records import A
from records import AAAA
from records import MX
from records import NS
from records import PTR
from records import CNAME
from records import TXT
from database import Database

class DNSServer:
    sock = socket(AF_INET, SOCK_DGRAM)
    conn = None
    arr_processing = []
    a_record: A
    aaaa_record = AAAA()
    mx_record = MX()
    txt_record = TXT()
    cname_record = CNAME()
    ptr_record = PTR()
    ns_record = NS()
    database: Database
    dict_server_config: dict
    def __init__(self):

        self.sock.bind(('0.0.0.0', 53))
        config_path = os.path.join(os.getcwd(), "config.toml")
        config = toml.load(config_path)
        self.database = Database(config['database'])
        if not self.database.bool_connected:
            print("Failed to make Connection to PostgreSQL Server. DNS Server Exiting")
            exit(1)
        self.dict_server_config = config['dnsServer']
        self.a_record = A(self.dict_server_config)

    def handle_request(self, request, address):
        start_time = time.time()
        try:
            dns = DNS(request)
            if dns[DNSQR].qtype == 1: # A Record
                self.a_record.handle_request(dns, self.database, address, self.sock)
            elif dns[DNSQR].qtype == 2: # NS Record
                self.ns_record.handle_request(dns, self.database, address, self.sock)
            elif dns[DNSQR].qtype == 5:
                self.cname_record.handle_request(dns, self.conn)
            elif dns[DNSQR].qtype == 12:
                self.ptr_record.handle_request(dns, self.database, address, self.sock)
            elif dns[DNSQR].qtype == 15:
                self.mx_record.handle_request(dns, self.conn)
            elif dns[DNSQR].qtype == 16:
                self.txt_record.handle_request(dns, self.conn, address, self.sock)
            elif dns[DNSQR].qtype == 28: # AAAA Record
                self.aaaa_record.handle_request(dns, self.database, address, self.sock)


            # # Define Postgres Functions
            # postgres = PostgresFunctions()
            # # Set DNS Request to a variable
            # dns = DNS(request)
            # # Get the ascii string for the DNS request
            # query = str(dns[DNSQR].qname.decode('ascii'))
            # if query.lower().endswith('.pretzel1.'):
            #     response = DNS(id=dns.id, ancount=1, qr=1, rcode=3)
            #     ans4 = DNSRR(rrname=str(query), type=1)
            #     response.an = ans4
            #     self.sock.sendto(bytes(response), addr)
            #     return True
            # # Check if this server is Authoritative for this DNS request (It will be 99% of the time)
            # result_dns_authority = postgres.get_dns_authority(self.conn, str(query))
            # # Error Checking Authoritative status, do not send a result
            # if result_dns_authority['bool_error']:
            #     return False
            # # Put database results into a variable
            # arr_data = json.loads(result_dns_authority['data'])
            # # No data was returned from the SQL query (Non Existent Domain)
            # if len(arr_data) == 0:
            #     # We will only auto create records for A and AAA types
            #     if dns[DNSQR].qtype == 1 or dns[DNSQR].qtype == 28:
            #         # Generate IPs for A and AAA, add to database
            #         dict_result_generate_dns_entry = GenerateDnsEntry().start(self.conn, str(query))
            #         # Error in creating new DNS record, do not send result
            #         if dict_result_generate_dns_entry['bool_error']:
            #             return False
            #         # All set to return generated data
            #         if not dict_result_generate_dns_entry['bool_already_exists']:
            #             response = DNS(id=dns.id, ancount=1, qr=1)
            #             ans4 = DNSRR(rrname=str(query), type=1, rdata=str(dict_result_generate_dns_entry['ipv4']))
            #             response.an = ans4
            #             self.sock.sendto(bytes(response), addr)
            #             return True
            #         else:  # A record was created before this record could be created. Reference new record
            #             if dns[DNSQR].qtype == 1:
            #                 result_dns_entry = postgres.get_dns_record(self.conn, str(query), 1)
            #                 result_data = json.loads(result_dns_entry['data'])
            #                 int_total = len(result_data)
            #                 response = DNS(id=dns.id, ancount=int(int_total), qr=1)
            #                 if int_total > 0:
            #                     int_parsed = 0
            #                     for result in result_data:
            #                         #print(result)
            #                         ans = DNSRR(rrname=str(query), type=1, rdata=str(result['record_data']['ip']))
            #                         if int_parsed == 0:
            #                             response.an = ans
            #                         elif int_parsed < int_total:
            #                             response.an = response.an / ans
            #                         int_parsed += 1
            #                 else:
            #                     ans = DNSRR(rrname=str(query), type=1, rdata=None)
            #                     response.an = ans
            #                 self.sock.sendto(bytes(response), addr)
            #                 return True
            #             if dns[DNSQR].qtype == 28:
            #                 result_dns_entry = postgres.get_dns_record(self.conn, str(query), 28)
            #                 result_data = json.loads(result_dns_entry['data'])
            #                 int_total = len(result_data)
            #                 response = DNS(id=dns.id, ancount=int(int_total), qr=1)
            #                 if int_total > 0:
            #                     int_parsed = 0
            #                     for result in result_data:
            #                         #(result['record_data']['ip'])
            #                         ans = DNSRR(rrname=str(query), type=28, rdata=str(result['record_data']['ip']))
            #                         if int_parsed == 0:
            #                             response.an = ans
            #                         elif int_parsed < int_total:
            #                             response.an = response.an / ans
            #                         int_parsed += 1
            #                 else:
            #                     ans = DNSRR(rrname=str(query), type=1, rdata=None)
            #                     response.an = ans
            #                 self.sock.sendto(bytes(response), addr)
            #                 return True
            # if arr_data[0]['bool_authoritative'] == "1":
            #     if dns[DNSQR].qtype == 1:
            #         result_dns_entry = postgres.get_dns_record(self.conn, str(query), 1)
            #         result_data = json.loads(result_dns_entry['data'])
            #         int_total = len(result_data)
            #         response = DNS(id=dns.id, ancount=int(int_total), qr=1)
            #         if int_total > 0:
            #             int_parsed = 0
            #             for result in result_data:
            #                 #print(result)
            #                 ans = DNSRR(rrname=str(query), type=1, rdata=str(result['record_data']['ip']))
            #                 if int_parsed == 0:
            #                     response.an = ans
            #                 elif int_parsed < int_total:
            #                     response.an = response.an / ans
            #                 int_parsed += 1
            #         else:
            #             ans = DNSRR(rrname=str(query), type=1, rdata=None)
            #             response.an = ans
            #         self.sock.sendto(bytes(response), addr)
            #     if dns[DNSQR].qtype == 28:
            #         result_dns_entry = postgres.get_dns_record(self.conn, str(query), 28)
            #         result_data = json.loads(result_dns_entry['data'])
            #         int_total = len(result_data)
            #         response = DNS(id=dns.id, ancount=int(int_total), qr=1)
            #         if int_total > 0:
            #             int_parsed = 0
            #             for result in result_data:
            #                 #print(result['record_data']['ip'])
            #                 ans = DNSRR(rrname=str(query), type=28, rdata=str(result['record_data']['ip']))
            #                 if int_parsed == 0:
            #                     response.an = ans
            #                 elif int_parsed < int_total:
            #                     response.an = response.an / ans
            #                 int_parsed += 1
            #         else:
            #             ans = DNSRR(rrname=str(query), type=1, rdata=None)
            #             response.an = ans
            #         self.sock.sendto(bytes(response), addr)
            #     if dns[DNSQR].qtype == 12:
            #         result_dns_entry = postgres.get_dns_record(self.conn, str(query), 12)
            #         result_data = json.loads(result_dns_entry['data'])
            #         int_total = len(result_data)
            #         response = DNS(id=dns.id, ancount=int(int_total), qr=1)
            #         #print(int_total)
            #         if int_total > 0:
            #             int_parsed = 0
            #             for result in result_data:
            #                 ans = DNSRR(rrname=str(query), type=12, rdata=str(result['record_data']['domain_name']))
            #                 if int_parsed == 0:
            #                     response.an = ans
            #                 elif int_parsed < int_total:
            #                     response.an = response.an / ans
            #                 int_parsed += 1
            #         else:
            #             ans = DNSRR(rrname=str(query), type=1, rdata=None)
            #             response.an = ans
            #         self.sock.sendto(bytes(response), addr)
            #         #print("--- %s seconds ---" % (time.time() - start_time))
            #     return False
            # #domain, tld, tail = query.rsplit('.', 3)
            # #assert domain == 'example' and tld == 'com' and tail == ''
            # #head = head.split('.', 1)[-1]  # drop leading "prefix." part
        except Exception as e:
            print('')
            print_exc()
            print('garbage from {!r}? data {!r}'.format(addr, request))


    def run_server(self):
        int_cnt = 0
        while True:
            try:
                request, addr = self.sock.recvfrom(4096)
                if request:
                    # print("Count: {}".format(int_cnt))
                    # print("Got Request for '{}' From '{}'".format(str(DNS(request)[DNSQR].qname.decode('ascii')), str(addr)))
                    if threading.active_count() < 2000:
                        x = threading.Thread(target=self.handle_request, args=(request, addr))
                        x.start()
                        #print("Ping")
                    else:
                        print("thread Limit")
                    del request
                    del addr
            except Exception as e:
                print("Server Error: {}".format(e))
        self.sock.close()