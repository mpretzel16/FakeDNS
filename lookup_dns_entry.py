import traceback

from database.database import Database
import json


class LookupDNSEntry:

    @staticmethod
    def get_dns_record(database: Database, entry_text, record_type):
        dict_return = dict()
        dict_return['bool_error'] = False
        dict_return['str_status'] = None
        dict_return['data'] = []
        try:
            # entry_text = str(entry_text).lower()
            # cursor = conn.cursor(cursor_factory=RealDictCursor)
            # lookup_query = "SELECT record_data FROM public.dns_entries WHERE entry_text = %s AND record_type = %s;"
            # record_to_find = (str(entry_text), int(record_type))
            # cursor.execute(lookup_query, record_to_find)
            # dict_return['data'] = json.dumps(cursor.fetchall(), indent=2)
            # cursor.close()
            pass
        except Exception as e:
            print("General Error: {}".format(e))
            dict_return['str_status'] = e
        return dict_return

    @staticmethod
    def get_dns_authority(db_session, entry_text):
        dict_return = dict()
        dict_return['bool_error'] = False
        dict_return['str_status'] = None
        dict_return['data'] = []
        cursor = None
        try:
            entry_text = str(entry_text).lower()
            search = db_session.query(Database.Dns_Authoritative) \
                .filter(Database.Dns_Authoritative.entry_text == entry_text).first()
            #print(search)
            dict_return['data'] = search
        except Exception as e:
            traceback.print_exc()
            print("General Error1: {}".format(e))
            dict_return['str_status'] = e
        return dict_return
