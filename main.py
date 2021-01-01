import argparse
import sys
from update_networks import UpdateNetworks
from dns_server import DNSServer
from update_tld import UpdateTLD
from rich.console import Console
console = Console()


def setup_args():
    parser = argparse.ArgumentParser(description='Fake DNS Server (Pretzel Bytes LLC)')
    # Argument to Run the DNS Server
    parser.add_argument("--run", type=bool, nargs='?', const=True, default=False,
                        help="Runs the Fake DNS Server (Can specify --run or --run True)")
    # Argument to Update the Networks and Country Listing
    parser.add_argument("--update-networks", type=bool, default=False, nargs='?', const=True,
                        help="Update Networks and Countries Database Tables (Requires --update-networks-file)")
    # Argument for the 'GeoLite2-Country-CSV_*.zip' File Location
    parser.add_argument("--update-networks-file", type=str, required='--update-networks' in sys.argv,
                        help="File for Network and Country Update. "
                             "--update-networks-file /root/GeoLite2-Country-CSV_20201215.zip"
                             "Download 'GeoLite2 Country: CSV Format' from www.maxmind.com")
    # Argument to Update the legitimate TLD Listing
    parser.add_argument("--update-tld", type=bool, default=False, nargs='?', const=True,
                        help="Updates the Valid TLDs a Client Can Request (Requires either '--update-tld-file' "
                             "or '--update-tld-online')")
    # Argument for the 'tlds-alpha-by-domain.txt' File Location
    parser.add_argument("--update-tld-file", type=str, required=('--update-tld' in sys.argv) and
                                                                ('--update-tld-online' not in sys.argv),
                        help="Download (https://data.iana.org/TLD/tlds-alpha-by-domain.txt) and point this argument to"
                             "the downloaded file.")
    # Argument to Update TLD online (No Need to Download a File)
    parser.add_argument("--update-tld-online", type=bool, default=False, nargs='?', const=True,
                        required=('--update-tld' in sys.argv) and ('--update-tld-file' not in sys.argv),
                        help="Runs the TLD update direct form (https://data.iana.org/TLD/tlds-alpha-by-domain.txt)")

    return parser.parse_args()


if __name__ == "__main__":
    console.rule('[blue]FakeDNS Server - Pretzel Bytes LLC')
    console.print('[bold]--NOT TO BE USED AS AN INTERNET ATTACHED DNS SERVER, THIS IS FOR TRAINING DNS GENERATION ONLY')
    args = setup_args()
    print(args)
    if args.update_networks:
        UpdateNetworks().start_update(args.update_networks_file)
    if args.update_tld:
        UpdateTLD().start_update(args.update_tld_online, args.update_tld_file)
    if args.run:
        dns_server = DNSServer()
        dns_server.run_server()
