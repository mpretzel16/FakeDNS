import sqlalchemy.orm
from scapy.layers.dns import DNS, DNSQR, DNSRR
from database import Database


class NS:
    dict_server_config: dict
    def __init__(self, dict_server_config:dict):
        print("DNS NS(2) Record Class Initialized")
        self.dict_server_config = dict_server_config

    def handle_request(self, dns_request, database: Database, address, sock):
        db_session: sqlalchemy.orm.Session = database.Session()
        query = str(dns_request[DNSQR].qname.decode('ascii')).lower()
        query_exists = db_session.query(Database.Dns_Entries).filter(database.Dns_Entries.entry_text == query).first()
        # Entry Exists in database
        if query_exists:
            query_authority = db_session.query(Database.Dns_Entries) \
                .filter(database.Dns_Entries.entry_text == query) \
                .filter(database.Dns_Entries.record_type == 2).all()
            if query_authority:
                int_total = len(query_authority)
                response = DNS(id=dns_request.id, ancount=int(int_total), qr=2)
                if int_total > 0:
                    int_parsed = 0
                    for result in query_authority:
                        result: Database.Dns_Entries = result
                        ans = DNSRR(rrname=str(query), type=2, rdata=str(result.record_data['domain_name']))
                        if int_parsed == 0:
                            response.an = ans
                        elif int_parsed < int_total:
                            response.an = response.an / ans
                        int_parsed += 1
                else:
                    ans = DNSRR(rrname=str(query), type=2, rdata=None)
                    response.an = ans
                sock.sendto(bytes(response), address)
        else:  # DNS Entry does NOT exist in the Database. Generate it.
            response = DNS(id=dns_request.id, ancount=0, qr=1)
            ans = DNSRR(rrname=str(query), type=2, rdata=None)
            response.an = ans
            sock.sendto(bytes(response), address)