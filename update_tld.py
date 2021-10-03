from rich.progress import Progress
from rich.console import Console
import tempfile
from shutil import rmtree
import requests
from os import path
import pandas as pd
import psycopg2
from psycopg2 import Error

from database import Database


class UpdateTLD:
    progress = Progress(transient=False)
    console = Console()
    str_tld_download_url = "https://raw.githubusercontent.com/datasets/top-level-domain-names/master/top-level-domain-names.csv"
    database: Database
    db_session: None

    def start_update(self, bool_online_update, str_file_update):
        self.database = Database()
        self.db_session = self.database.Session()
        self.console.print('[red]Top Level Domain Update[bold] Starting')
        if bool_online_update:
            self.console.print('[red]-Preforming Online TLD Update')
            dict_return_start_internet_update = self.start_internet_update()
            if not dict_return_start_internet_update['bool_error']:
                with self.progress:
                    db_update_task = self.progress.add_task("Updating Top Level Domains", total=100)
                    self.database_update(dict_return_start_internet_update['str_file_path'], db_update_task)
        self.db_session.close()

    def start_internet_update(self):
        dict_return = dict()
        dict_return['bool_error'] = False
        dict_return['str_error_text'] = None
        dict_return['str_file_path'] = None
        try:
            self.console.print('--Downloading Update File')
            tmp = tempfile.mkdtemp()
            tmp = path.join(tmp, 'tld.txt')
            r = requests.get(self.str_tld_download_url)
            if r.status_code == 200:
                with open(tmp, 'wb') as f:
                    f.write(r.content)
                dict_return['str_file_path'] = tmp
            else:
                self.console.log("Error Downloading TLD File. Http Status Code: {}".format(r.status_code))
                dict_return['bool_error'] = True
                dict_return['str_error_text'] = "Error Downloading TLD File. Http Status Code: {}".format(r.status_code)
            # rmtree(tmp, ignore_errors=True)
        except Exception as e:
            dict_return['bool_error'] = True
            dict_return['str_error_text'] = str(e)
            print(e)
        return dict_return

    def database_update(self, str_file_path, task):
        try:
            tld_listing = pd.read_csv(str_file_path, skiprows=1)
            int_total_tld = len(tld_listing)
            self.progress.update(task, total=float(int_total_tld))
            int_ingested_tld = 0
            for index, row in tld_listing.iterrows():
                int_ingested_tld += 1
                search = self.db_session.query(self.database.Tld_Listing) \
                    .filter(self.database.Tld_Listing.domain == str(row[0] + ".")).first()
                if not search:
                    tld = self.database.Tld_Listing()
                    tld.domain = str(row[0] + ".")
                    tld.type = str(row[1])
                    tld.org = str(row[2])
                    self.db_session.add(tld)
                else:
                    search: Database.Tld_Listing = search
                    search.type = str(row[1])
                    search.org = str(row[2])
            self.progress.update(task, completed=float(int_total_tld))
        except Exception as e:
            print(e)
        finally:
            self.db_session.commit()

