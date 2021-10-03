import sqlalchemy.orm
from scapy.layers.dns import DNS, DNSQR, DNSRR
from database import Database


class AAAA:
    dns_entry = None
    generate_dns_entry = None

    def __init__(self):
        print("DNS AAAA(28) Record Class Initialized")

    def handle_request(self, dns_request, database: Database, address, sock):
        if database.bool_sqlite:
            database = Database()
        db_session: sqlalchemy.orm.Session
        db_session = database.Session()
        query = str(dns_request[DNSQR].qname.decode('ascii')).lower()
        dict_result_lookup_dns_authority = db_session.query(Database.Dns_Authoritative) \
            .filter(Database.Dns_Authoritative.entry_text == query).first()
        if dict_result_lookup_dns_authority is None:
            response = DNS(id=dns_request.id, ancount=0, qr=1)
            ans = DNSRR(rrname=str(query), type=1, rdata=None)
            response.an = ans
            sock.sendto(bytes(response), address)
        # Authoritative Information Returned, Continue
        else:
            # # DNS server is the Authoritative server.
            if dict_result_lookup_dns_authority.bool_authoritative:
                result_get_dns_record = db_session.query(Database.Dns_Entries.record_data)\
                    .filter(Database.Dns_Entries.entry_text == query).filter(Database.Dns_Entries.record_type == 28).all()
                int_total = len(result_get_dns_record)
                response = DNS(id=dns_request.id, ancount=int(int_total), qr=1)
                if int_total > 0:
                    int_parsed = 0
                    for result in result_get_dns_record:
                        result: Database.Dns_Entries = result
                        ans = DNSRR(rrname=str(query), type=28, rdata=str(result.record_data['ip']))
                        if int_parsed == 0:
                            response.an = ans
                        elif int_parsed < int_total:
                            response.an = response.an / ans
                        int_parsed += 1
                else:
                    ans = DNSRR(rrname=str(query), type=1, rdata=None)
                    response.an = ans
                sock.sendto(bytes(response), address)
            # DNS server is NOT the Authoritative server, Forward the request
            else:
                print("Non Authoritative, pass on")




