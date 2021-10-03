import sqlalchemy.orm
from scapy.layers.dns import DNS, DNSQR, DNSRR
from generate_dns_entry import GenerateDnsEntry
from time import sleep
from database import Database


class A:
    dns_entry = None
    generate_dns_entry = None
    dict_server_config: dict
    def __init__(self, dict_server_config: dict):
        print("DNS A(1) Record Class Initialized")
        self.dict_server_config = dict_server_config
        self.generate_dns_entry = GenerateDnsEntry(self.dict_server_config)

    def handle_request(self, dns_request, database: Database, address, sock):
        db_session: sqlalchemy.orm.Session = database.Session()
        query = str(dns_request[DNSQR].qname.decode('ascii')).lower()
        query_exists = db_session.query(Database.Dns_Entries).filter(database.Dns_Entries.entry_text == query).first()
        # Entry Exists in database
        if query_exists:
            query_authority:Database.Dns_Entries = db_session.query(Database.Dns_Entries) \
                .filter(database.Dns_Entries.entry_text == query) \
                .filter(database.Dns_Entries.record_type == 2).all()
            if query_authority:
                if query_authority[0].record_data['domain_name'] == self.dict_server_config['hostname']:
                    # Look at me, I am the authoritative server now
                    dns_query = db_session.query(Database.Dns_Entries)\
                        .filter(Database.Dns_Entries.entry_text == query)\
                        .filter(Database.Dns_Entries.record_type == 1).all()
                    int_total = len(dns_query)
                    response = DNS(id=dns_request.id, ancount=int(int_total), qr=1)
                    if int_total > 0:
                        int_parsed = 0
                        for result in dns_query:
                            result: Database.Dns_Entries = result
                            ans = DNSRR(rrname=str(query), type=1, rdata=str(result.record_data['ip']))
                            if int_parsed == 0:
                                response.an = ans
                            elif int_parsed < int_total:
                                response.an = response.an / ans
                            int_parsed += 1
                    else:
                        ans = DNSRR(rrname=str(query), type=1, rdata=None)
                        response.an = ans
                    sock.sendto(bytes(response), address)
                else:
                    # I am not the authoritative server, pass it on.
                    pass
            else:
                raise Exception("DNS Entry Corrupt")
        else: # DNS Entry does NOT exist in the Database. Generate it.
            result_generate_dns_entry = self.generate_dns_entry.start(db_session, query)
            if result_generate_dns_entry['bool_error']:
                print(result_generate_dns_entry['str_status'])
            if result_generate_dns_entry['bool_invalid_tld']:
                response = DNS(id=dns_request.id, ancount=1, qr=1, rcode=3)
                ans4 = DNSRR(rrname=str(query), type=1)
                response.an = ans4
                sock.sendto(bytes(response), address)
            else:
                if result_generate_dns_entry['bool_already_exists']:
                    sleep(100)
                    return self.handle_request(dns_request, database, address, sock)
                else:
                    response = DNS(id=dns_request.id, ancount=1, qr=1)
                    ans4 = DNSRR(rrname=str(query), type=1, rdata=str(result_generate_dns_entry['ipv4']))
                    response.an = ans4
                    sock.sendto(bytes(response), address)
                    return True

