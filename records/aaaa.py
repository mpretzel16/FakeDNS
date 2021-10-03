import sys
import sqlalchemy.orm
from scapy.layers.dns import DNS, DNSQR, DNSRR
from database import Database


class AAAA:
    dns_entry = None
    dict_server_config: dict
    db_session: sqlalchemy.orm.Session
    def __init__(self, dict_server_config: dict):
        print("DNS AAAA(28) Record Class Initialized")
        self.dict_server_config = dict_server_config

    def handle_request(self, dns_request, database: Database, address, sock):
        query = None
        try:
            query = str(dns_request[DNSQR].qname.decode('ascii')).lower()
            self.db_session = database.Session()
            query_exists = self.db_session.query(Database.DNSEntry).filter(database.DNSEntry.entry_text == query).first()
            # Entry Exists in database
            if query_exists:
                query_authority: Database.DNSEntry = self.db_session.query(Database.DNSEntry) \
                    .filter(database.DNSEntry.entry_text == query) \
                    .filter(database.DNSEntry.record_type == 2).all()
                if query_authority:
                    if query_authority[0].record_data['domain_name'] == self.dict_server_config['hostname']:
                        # Look at me, I am the authoritative server now
                        dns_query = self.db_session.query(Database.DNSEntry) \
                            .filter(Database.DNSEntry.entry_text == query) \
                            .filter(Database.DNSEntry.record_type == 28).all()
                        int_total = len(dns_query)
                        response = DNS(id=dns_request.id, ancount=int(int_total), qr=1)
                        if int_total > 0:
                            int_parsed = 0
                            for result in dns_query:
                                result: Database.DNSEntry = result
                                ans = DNSRR(rrname=str(query), type=28, rdata=str(result.record_data['ip']))
                                if int_parsed == 0:
                                    response.an = ans
                                elif int_parsed < int_total:
                                    response.an = response.an / ans
                                int_parsed += 1
                        else:
                            ans = DNSRR(rrname=str(query), type=28, rdata=None)
                            response.an = ans
                        sock.sendto(bytes(response), address)
                    else:
                        # I am not the authoritative server, pass it on.
                        pass
                else:
                    raise Exception("DNS Entry Corrupt")
            else:  # DNS Entry does NOT exist in the Database. Generate it.
                response = DNS(id=dns_request.id, ancount=0, qr=1, rcode=2)
                sock.sendto(bytes(response), address)
        except Exception as e:
            if query is None:
                query = "ERROR IN QUERY"
            print("Error with AAAA. IP:{}|Request URL:{}|Error: {}".format(address, query, str(e)), file=sys.stderr)
            response = DNS(id=dns_request.id, ancount=0, qr=1, rcode=2)
            sock.sendto(bytes(response), address)
        finally:
            if self.db_session.is_active:
                self.db_session.close()



