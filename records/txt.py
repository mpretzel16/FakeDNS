from scapy.layers.dns import DNS, DNSQR, DNSRR
import json


class TXT:
    def __init__(self):
        print("DNS TXT(16) Record Class Initialized")

    def handle_request(self, dns_request, postgres_connection, addr, socket):
        query = str(dns_request[DNSQR].qname.decode('ascii')).lower()
        print(query)
        #result_dns_entry = self.lookup_dns_entry.get_dns_record(postgres_connection, str(query), 12)
        #result_data = json.loads(result_dns_entry['data'])
        #int_total = len(result_data)
        response = DNS(id=dns_request.id, ancount=1, qr=1)
        ans = DNSRR(rrname=str(query), type=16, rdata='hi', ttl=120)
        response.an = ans
        socket.sendto(bytes(response), addr)