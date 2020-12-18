from rich.progress import Progress
from rich.console import Console
import tempfile
from shutil import rmtree
import requests
from os import path


class UpdateTLD:
    console = Console()
    str_tld_download_url = "https://data.iana.org/TLD/tlds-alpha-by-domain.txt"

    def start_update(self, bool_online_update, str_file_update):
        self.console.print('[red]Top Level Domain Update[bold] Starting')
        if bool_online_update:
            self.start_internet_update()

    def start_internet_update(self):
        self.console.print('[red]-Preforming Online Update')
        tmp = tempfile.mkdtemp()
        print(tmp)
        r = requests.get(self.str_tld_download_url)
        if r.status_code == 200:
            with open(path.join(tmp, 'tld.txt'), 'wb') as f:
                f.write(r.content)
        else:
            self.console.log("Error Downloading TLD File. Http Status Code: {}".format(r.status_code))
        rmtree(tmp, ignore_errors=True)