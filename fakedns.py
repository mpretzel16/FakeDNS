from scapy.layers.dns import DNS, DNSQR, DNSRR, dnsqtypes
from socket import AF_INET, SOCK_DGRAM, socket
from traceback import print_exc
from postgres_functions import PostgresFunctions
import threading
import json
import time
from generate_dns_entry import GenerateDnsEntry

sock = socket(AF_INET, SOCK_DGRAM)
sock.bind(('0.0.0.0', 53))
conn = PostgresFunctions().connect()


def handle_request(request, addr):
    start_time = time.time()
    try:
        postgres = PostgresFunctions()
        dns = DNS(request)
        # print(dns[DNSQR].qtype)
        #assert dns.opcode == 0, dns.opcode  # QUERY
        query = str(dns[DNSQR].qname.decode('ascii'))  # test.1.2.3.4.example.com.
        # if query.endswith('.in-addr.arpa.'):
        #     query = query.replace('.in-addr.arpa.', '')
        #     t_query = query.split(".")
        #     t_query.reverse()
        #     query = ".".join(t_query)
        #assert dnsqtypes[dns[DNSQR].qtype] == 'A', dns[DNSQR].qtype
        result_dns_authority = postgres.get_dns_authority(conn, str(query))
        if result_dns_authority['bool_error']:
            print(result_dns_authority['str_status'])
            return False
        arr_data = json.loads(result_dns_authority['data'])
        if len(arr_data) == 0:
            # NON EXISTENT DOMAIN
            if dns[DNSQR].qtype == 1 or dns[DNSQR].qtype == 28:
                print("Add Em")
                test = GenerateDnsEntry().start(str(query), conn)
                response = DNS(id=dns.id, ancount=1, qr=1)
                ans4 = DNSRR(rrname=str(query), type=1, rdata=str(test['ipv4']))
                response.an = ans4
                sock.sendto(bytes(response), addr)
            return False
        if arr_data[0]['bool_authoritative'] == "1":
            if dns[DNSQR].qtype == 1:
                result_dns_entry = postgres.get_dns_record(conn, str(query), 1)
                result_data = json.loads(result_dns_entry['data'])
                int_total = len(result_data)
                response = DNS(id=dns.id, ancount=int(int_total), qr=1)
                if int_total > 0:
                    int_parsed = 0
                    for result in result_data:
                        print(result)
                        ans = DNSRR(rrname=str(query), type=1, rdata=str(result['record_data']['ip']))
                        if int_parsed == 0:
                            response.an = ans
                        elif int_parsed < int_total:
                            response.an = response.an / ans
                        int_parsed += 1
                else:
                    ans = DNSRR(rrname=str(query), type=1, rdata=None)
                    response.an = ans
                sock.sendto(bytes(response), addr)
            if dns[DNSQR].qtype == 28:
                result_dns_entry = postgres.get_dns_record(conn, str(query), 28)
                result_data = json.loads(result_dns_entry['data'])
                int_total = len(result_data)
                response = DNS(id=dns.id, ancount=int(int_total), qr=1)
                if int_total > 0:
                    int_parsed = 0
                    for result in result_data:
                        print(result['record_data']['ip'])
                        ans = DNSRR(rrname=str(query), type=28, rdata=str(result['record_data']['ip']))
                        if int_parsed == 0:
                            response.an = ans
                        elif int_parsed < int_total:
                            response.an = response.an / ans
                        int_parsed += 1
                else:
                    ans = DNSRR(rrname=str(query), type=1, rdata=None)
                    response.an = ans
                sock.sendto(bytes(response), addr)
            if dns[DNSQR].qtype == 12:
                result_dns_entry = postgres.get_dns_record(conn, str(query), 12)
                result_data = json.loads(result_dns_entry['data'])
                int_total = len(result_data)
                response = DNS(id=dns.id, ancount=int(int_total), qr=1)
                print(int_total)
                if int_total > 0:
                    int_parsed = 0
                    for result in result_data:
                        ans = DNSRR(rrname=str(query), type=12, rdata=str(result['record_data']['domain_name']))
                        if int_parsed == 0:
                            response.an = ans
                        elif int_parsed < int_total:
                            response.an = response.an / ans
                        int_parsed += 1
                else:
                    ans = DNSRR(rrname=str(query), type=1, rdata=None)
                    response.an = ans
                sock.sendto(bytes(response), addr)
                print("--- %s seconds ---" % (time.time() - start_time))
            return False
        #domain, tld, tail = query.rsplit('.', 3)
        #assert domain == 'example' and tld == 'com' and tail == ''
        #head = head.split('.', 1)[-1]  # drop leading "prefix." part
    except Exception as e:
        print('')
        print_exc()
        print('garbage from {!r}? data {!r}'.format(addr, request))


while True:
    request, addr = sock.recvfrom(4096)
    if request:
        print("Got Request for '{}' From '{}'".format(str(DNS(request)[DNSQR].qname.decode('ascii')), str(addr)))
        x = threading.Thread(target=handle_request, args=(request, addr))
        x.start()
