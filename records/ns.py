from scapy.layers.dns import DNS, DNSQR, DNSRR

from database import Database


class NS:
    dict_server_config: dict
    def __init__(self):
        print("DNS NS(2) Record Class Initialized")
        #self.dict_server_config = dict_server_config

    def handle_request(self, dns_request, database: Database, address, sock):
        query = str(dns_request[DNSQR].qname.decode('ascii')).lower()
        print(query)
        response = DNS(id=dns_request.id, ancount=1, qr=1)
        ans4 = DNSRR(rrname=str(query), type=2, rdata=str("google.org"))
        response.an = ans4
        sock.sendto(bytes(response), address)