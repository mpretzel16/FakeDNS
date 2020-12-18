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


class UpdateNetworks:
    progress = Progress(transient=False)
    console = Console()

    def ingest_ipv4(self, str_ipv4_file, task):
        ipv4 = pd.read_csv(str_ipv4_file, skiprows=1)
        try:
            # Connect to an existing database
            connection = psycopg2.connect(user="postgres",
                                          password="*KC8wjb*$",
                                          host="10.16.120.40",
                                          port="5432",
                                          database="fakeDNS")

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
                    # print("{:,}/{:,} | Ingesting: {} | {}".format(int_ingested_ips, int_total_ips, row[0], row[1]))
                    record_to_insert = (str(row[0]), int(row[1]), int(row[1]))
                    cursor = connection.cursor()
                    insert_update_query = """
                    INSERT INTO public."IPv4_networks" (network, country_id)
            VALUES (%s, %s)
            ON CONFLICT (network) DO UPDATE SET country_id = %s;
                    """
                    cursor.execute(insert_update_query, record_to_insert)
                    cursor.close()
                except (Exception, Error) as error:
                    print(error)
            connection.commit()
            self.progress.update(task, completed=float(int_ingested_ips))
        except (Exception, Error) as error:
            print("Postgres Error: {}".format(error))
        finally:
            if connection:
                connection.close()

    def ingest_ipv6(self, str_ipv6_file, task):
        ipv6 = pd.read_csv(str_ipv6_file, skiprows=1)
        try:
            # Connect to an existing database
            connection = psycopg2.connect(user="postgres",
                                          password="*KC8wjb*$",
                                          host="10.16.120.40",
                                          port="5432",
                                          database="fakeDNS")

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
                    # print("{:,}/{:,} | Ingesting: {} | {}".format(int_ingested_ips, int_total_ips, row[0], row[1]))
                    self.progress.update(task, completed=float(int_ingested_ips - 1))
                    record_to_insert = (str(row[0]), int(row[1]), int(row[1]))
                    cursor = connection.cursor()
                    insert_update_query = """
                    INSERT INTO public."IPv6_networks" (network, country_id)
            VALUES (%s, %s)
            ON CONFLICT (network) DO UPDATE SET country_id = %s;
                    """
                    cursor.execute(insert_update_query, record_to_insert)
                    cursor.close()
                except (Exception, Error) as error:
                    print(error)
            connection.commit()
            self.progress.update(task, completed=float(int_ingested_ips))
        except (Exception, Error) as error:
            print("Postgres Error: {}".format(error))
        finally:
            if connection:
                connection.close()

    def ingest_country(self, str_country_file, task):
        country = pd.read_csv(str_country_file, skiprows=1)
        try:
            # Connect to an existing database
            connection = psycopg2.connect(user="postgres",
                                          password="*KC8wjb*$",
                                          host="10.16.120.40",
                                          port="5432",
                                          database="fakeDNS")

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
                    # test = cc_tld.query('country=="' + str(row[5]) + '"')[['domain']]
                    # print(test['domain'])
                    record_to_insert = (int(row[0]), str(row[4]), str(row[5]), str(row[4]), str(row[5]))
                    cursor = connection.cursor()
                    insert_update_query = """
                    INSERT INTO public."country_list" (country_id, country_code, country_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (country_id) DO UPDATE SET country_code = %s, country_name = %s;
                    """
                    cursor.execute(insert_update_query, record_to_insert)
                    cursor.close()
                except (Exception, Error) as error:
                    print(error)
            connection.commit()
            self.progress.update(task, completed=float(int_ingested_countries))
        except (Exception, Error) as error:
            print("Postgres Error: {}".format(error))
        finally:
            if connection:
                connection.close()

    def start_update(self, str_update_zip_file):
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
        rmtree(tmp, ignore_errors=True)
        self.console.print('[purple]Network and Country Update [bold]Complete')
