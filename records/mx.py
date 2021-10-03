class MX:
    def __init__(self):
        print("DNS MX(15) Record Class Initialized")

    def handle_request(self, dns_request, postgres_connection):
        print(dns_request)