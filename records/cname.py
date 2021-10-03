class CNAME:
    def __init__(self):
        print("DNS CNAME(5) Record Class Initialized")

    def handle_request(self, dns_request, postgres_connection):
        print(dns_request)