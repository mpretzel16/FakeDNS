import os
import toml
from scapy.layers.dns import DNS, DNSQR
from socket import AF_INET, SOCK_DGRAM, socket
from traceback import print_exc
import threading
import time
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
    aaaa_record: AAAA
    mx_record: MX
    txt_record: TXT
    cname_record: CNAME
    ptr_record: PTR
    ns_record: NS
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
        self.aaaa_record = AAAA(self.dict_server_config)
        self.ns_record = NS(self.dict_server_config)
        self.mx_record = MX(self.dict_server_config)
        self.cname_record = CNAME(self.dict_server_config)
        self.txt_record = TXT(self.dict_server_config)
        self.ptr_record = PTR(self.dict_server_config)

    def handle_request(self, request, address):
        start_time = time.time()
        try:
            dns = DNS(request)
            if dns[DNSQR].qtype == 1: # A Record
                self.a_record.handle_request(dns, self.database, address, self.sock)
            elif dns[DNSQR].qtype == 2: # NS Record
                self.ns_record.handle_request(dns, self.database, address, self.sock)
            elif dns[DNSQR].qtype == 5: # CNAME Record
                self.cname_record.handle_request(dns, self.database, address, self.sock)
            elif dns[DNSQR].qtype == 12: # PTR Record
                self.ptr_record.handle_request(dns, self.database, address, self.sock)
            elif dns[DNSQR].qtype == 15: # MX Record
                self.mx_record.handle_request(dns, self.database, address, self.sock)
            elif dns[DNSQR].qtype == 16: # TXT Record
                self.txt_record.handle_request(dns, self.database, address, self.sock)
            elif dns[DNSQR].qtype == 28: # AAAA Record
                self.aaaa_record.handle_request(dns, self.database, address, self.sock)
            elif dns[DNSQR].qtype == 255: # ANY Record
                pass
                #self.aaaa_record.handle_request(dns, self.database, address, self.sock)
            else:
                print(dns[DNSQR].qtype)
                response = DNS(id=dns.id, ancount=0, qr=1)
                self.sock.sendto(bytes(response), address)
        except Exception as e:
            print(e)
            print_exc()
            print('garbage from {!r}? data {!r}'.format(address, request))


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