import sqlalchemy.orm
from scapy.layers.dns import DNS, DNSQR, DNSRR
from database import Database


class PTR:
    def __init__(self):
        print("DNS PTR(12) Record Class Initialized")

    @staticmethod
    def handle_request(dns_request, database: Database, address, sock):
        try:
            db_session: sqlalchemy.orm.Session
            db_session = database.Session()
            query = str(dns_request[DNSQR].qname.decode('ascii')).lower()
            result_dns_entry = db_session.query(Database.Dns_Entries).filter(Database.Dns_Entries.entry_text == query)\
                .filter(Database.Dns_Entries.record_type == 12).all()
            int_total = len(result_dns_entry)
            response = DNS(id=dns_request.id, ancount=int(int_total), qr=1)
            if int_total > 0:
                int_parsed = 0
                for result in result_dns_entry:
                    result: Database.Dns_Entries = result
                    ans = DNSRR(rrname=str(query), type=12, rdata=str(result.record_data['domain_name']))
                    if int_parsed == 0:
                        response.an = ans
                    elif int_parsed < int_total:
                        response.an = response.an / ans
                    int_parsed += 1
            else:
                ans = DNSRR(rrname=str(query), type=1, rdata=None)
                response.an = ans
            sock.sendto(bytes(response), address)
        except Exception as e:
            print(e)
            return False
