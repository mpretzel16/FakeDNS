from zipfile import ZipFile
import tempfile
from shutil import rmtree
import os
import pandas as pd
import psycopg2
from psycopg2 import Error
import math
from rich.progress import Progress
from rich.console import Console
from database import Database



class UpdateNetworks:
    progress = Progress(transient=False)
    console = Console()
    database: Database
    db_session: None
    def ingest_ipv4(self, str_ipv4_file, task):
        ipv4 = pd.read_csv(str_ipv4_file, skiprows=1)
        try:
            # Connect to an existing database
            # connection = psycopg2.connect(user="postgres",
            #                               password="*KC8wjb*$",
            #                               host="10.16.120.40",
            #                               port="5432",
            #                               database="fakeDNS")

            int_total_ips = len(ipv4)
            self.progress.update(task, total=float(int_total_ips))
            int_ingested_ips = 0
            for index, row in ipv4.iterrows():
                try:
                    int_ingested_ips += 1
                    if math.isnan(row[1]):
                        row[1] = 0
                    else:
                        row[1] = int(row[1])
                    self.progress.update(task, completed=float(int_ingested_ips - 1))
                    search = self.db_session.query(self.database.IPv4_Networks)\
                        .filter(self.database.IPv4_Networks.network == str(row[0])).first()
                    if not search:
                        ipv4_network = self.database.IPv4_Networks()
                        ipv4_network.network = str(row[0])
                        ipv4_network.country_id = int(row[1])
                        self.db_session.add(ipv4_network)
                    else:
                        search: Database.IPv4_Networks = search
                        search.country_id = int(row[1])

                except (Exception, Error) as error:
                    print(error)
            self.progress.update(task, completed=float(int_ingested_ips))
        except (Exception, Error) as error:
            print("Postgres Error: {}".format(error))
        finally:
            self.db_session.commit()


    def ingest_ipv6(self, str_ipv6_file, task):
        ipv6 = pd.read_csv(str_ipv6_file, skiprows=1)
        try:
            int_total_ips = len(ipv6)
            self.progress.update(task, total=float(int_total_ips))
            int_ingested_ips = 0
            for index, row in ipv6.iterrows():
                try:
                    int_ingested_ips += 1
                    if math.isnan(row[1]):
                        row[1] = 0
                    else:
                        row[1] = int(row[1])
                    self.progress.update(task, completed=float(int_ingested_ips - 1))
                    search = self.db_session.query(self.database.IPv6_Networks) \
                        .filter(self.database.IPv6_Networks.network == str(row[0])).first()
                    if not search:
                        ipv6_network = self.database.IPv6_Networks()
                        ipv6_network.network = str(row[0])
                        ipv6_network.country_id = int(row[1])
                        self.db_session.add(ipv6_network)
                    else:
                        search: Database.IPv6_Networks = search
                        search.country_id = int(row[1])
                    #self.db_session.commit()
                except (Exception, Error) as error:
                    print(error)
            self.progress.update(task, completed=float(int_ingested_ips))
        except (Exception, Error) as error:
            print("Postgres Error: {}".format(error))
        finally:
            self.db_session.commit()

    def ingest_country(self, str_country_file, task):
        try:
            country = pd.read_csv(str_country_file, skiprows=1)
            int_total_countries = len(country)
            self.progress.update(task, total=float(int_total_countries))
            int_ingested_countries = 0
            for index, row in country.iterrows():
                try:
                    int_ingested_countries += 1
                    if math.isnan(row[0]):
                        row[0] = 0
                    else:
                        row[0] = int(row[0])
                    self.progress.update(task, completed=float(int_ingested_countries - 1))

                    search = self.db_session.query(self.database.Country_List) \
                        .filter(self.database.Country_List.country_id == int(row[0])).first()
                    if not search:
                        country = self.database.Country_List()
                        country.country_id = int(row[0])
                        country.country_code = str(row[4])
                        country.country_name = str(row[5])
                        self.db_session.add(country)
                    else:
                        search: Database.Country_List = search
                        search.country_code = str(row[4])
                        search.country_name = str(row[5])
                except (Exception, Error) as error:
                    print(error)
            self.progress.update(task, completed=float(int_ingested_countries))
        except (Exception, Error) as error:
            print("Postgres Error: {}".format(error))
        finally:
            self.db_session.commit()

    def start_update(self, str_update_zip_file):
        self.database = Database()
        self.db_session = self.database.Session()
        self.console.print('[purple]Network and Country Update[bold] Starting')
        tmp = tempfile.mkdtemp()
        with ZipFile(str_update_zip_file, 'r') as zipObj:
            zipObj.extractall(tmp)
        str_csv_directory = os.path.join(tmp, os.listdir(tmp)[0])
        lst_csv_files = os.listdir(str_csv_directory)
        with self.progress:
            ipv4_task = self.progress.add_task("Updating IPv4", total=100)
            ipv6_task = self.progress.add_task("Updating IPv6", total=100)
            country_task = self.progress.add_task("Updating Countries", total=100)
            for csv in lst_csv_files:
                if csv == "GeoLite2-Country-Blocks-IPv4.csv":
                    self.ingest_ipv4(os.path.join(str_csv_directory, csv), ipv4_task)
                if csv == "GeoLite2-Country-Blocks-IPv6.csv":
                    self.ingest_ipv6(os.path.join(str_csv_directory, csv), ipv6_task)
                if csv == "GeoLite2-Country-Locations-en.csv":
                    self.ingest_country(os.path.join(str_csv_directory, csv), country_task)
        self.db_session.close()
        rmtree(tmp, ignore_errors=True)
        self.console.print('[purple]Network and Country Update [bold]Complete')
