import sqlalchemy.orm
from scapy.layers.dns import DNS, DNSQR, DNSRRMX
from database import Database


class MX:
    dict_server_config: dict
    def __init__(self, dict_server_config: dict):
        print("DNS MX(15) Record Class Initialized")
        self.dict_server_config = dict_server_config

    def handle_request(self, dns_request, database: Database, address, sock):
        db_session: sqlalchemy.orm.Session = database.Session()
        query = str(dns_request[DNSQR].qname.decode('ascii')).lower()
        query_exists = db_session.query(Database.Dns_Entries).filter(database.Dns_Entries.entry_text == query).first()
        # Entry Exists in database
        if query_exists:
            query_authority: Database.Dns_Entries = db_session.query(Database.Dns_Entries) \
                .filter(database.Dns_Entries.entry_text == query) \
                .filter(database.Dns_Entries.record_type == 2).all()
            if query_authority:
                if query_authority[0].record_data['domain_name'] == self.dict_server_config['hostname']:
                    # Look at me, I am the authoritative server now
                    dns_query = db_session.query(Database.Dns_Entries) \
                        .filter(Database.Dns_Entries.entry_text == query) \
                        .filter(Database.Dns_Entries.record_type == 15).all()
                    int_total = len(dns_query)
                    response = DNS(id=dns_request.id, ancount=int(int_total), qr=1)
                    if int_total > 0:
                        int_parsed = 0
                        for result in dns_query:
                            result: Database.Dns_Entries = result
                            ans = DNSRRMX(rrname=str(query), type=15,
                                          preference=result.record_data['preference'],
                                          exchange=result.record_data['exchange'])
                            if int_parsed == 0:
                                response.an = ans
                            elif int_parsed < int_total:
                                response.an = response.an / ans
                            int_parsed += 1
                    sock.sendto(bytes(response), address)
                else:
                    # I am not the authoritative server, pass it on.
                    pass
            else:
                raise Exception("DNS Entry Corrupt")
        else:  # DNS Entry does NOT exist in the Database. Generate it.
            response = DNS(id=dns_request.id, ancount=0, qr=1)
            sock.sendto(bytes(response), address)
