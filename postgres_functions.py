import psycopg2
from psycopg2 import Error
from psycopg2.extras import RealDictCursor
import json


class PostgresFunctions:

    @staticmethod
    def connect():
        connection = None
        try:
            # Connect to an existing database
            connection = psycopg2.connect(user="postgres",
                                          password="*KC8wjb*$",
                                          host="10.16.120.40",
                                          port="5432",
                                          database="fakeDNS")
            connection.autocommit = True
        except Exception as e:
            print(e)
        return connection

    @staticmethod
    def get_dns_record(conn, entry_text, record_type):
        dict_return = dict()
        dict_return['bool_error'] = False
        dict_return['str_status'] = None
        dict_return['data'] = []
        cursor = None
        try:
            entry_text = str(entry_text).lower()
            # print("Lookup: {}".format(entry_text))
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            lookup_query = "SELECT record_data FROM public.dns_entries WHERE entry_text = %s AND record_type = %s;"
            record_to_find = (str(entry_text), int(record_type))
            cursor.execute(lookup_query, record_to_find)
            dict_return['data'] = json.dumps(cursor.fetchall(), indent=2)
            cursor.close()
        except Error as e:
            if cursor is not None:
                cursor.close()
            print("SQL Error: {}".format(e))
            dict_return['bool_error'] = True
            dict_return['str_status'] = e
        except Exception as e:
            if cursor is not None:
                cursor.close()
            print("General Error: {}".format(e))
            dict_return['str_status'] = e
        return dict_return

    @staticmethod
    def get_dns_authority(conn, entry_text):
        dict_return = dict()
        dict_return['bool_error'] = False
        dict_return['str_status'] = None
        dict_return['data'] = []
        cursor = None
        try:
            entry_text = str(entry_text).lower()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            lookup_query = "SELECT bool_authoritative, authoritative_data FROM public.dns_authoritative " \
                           "WHERE entry_text = %s;"
            record_to_find = (str(entry_text),)
            cursor.execute(lookup_query, record_to_find)
            dict_return['data'] = json.dumps(cursor.fetchall(), indent=2)
            cursor.close()
        except Error as e:
            if cursor is not None:
                cursor.close()
            print("SQL Error: {}".format(e))
            dict_return['bool_error'] = True
            dict_return['str_status'] = e
        except Exception as e:
            if cursor is not None:
                cursor.close()
            print("General Error: {}".format(e))
            dict_return['str_status'] = e
        return dict_return

    @staticmethod
    def add_to_authority(conn, entry_text):
        dict_return = dict()
        dict_return['bool_error'] = False
        dict_return['str_status'] = None
        dict_return['bool_inserted'] = False
        dict_return['bool_already_exists'] = False
        cursor = None
        try:
            entry_text = str(entry_text).lower()
            record_to_insert = (str(entry_text),)
            cursor = conn.cursor()
            insert_update_query = """
                           INSERT INTO public.dns_authoritative(entry_text, bool_authoritative) 
                           VALUES (%s, '1');
                            """
            cursor.execute(insert_update_query, record_to_insert)
            if cursor.rowcount == 1:
                dict_return['bool_inserted'] = True
            cursor.close()
        except Error as e:
            if cursor is not None:
                cursor.close()
            if "duplicate key value violates unique constraint" in str(e):
                dict_return['bool_already_exists'] = True
                return dict_return
            print("SQL Error: {}".format(e))
            dict_return['bool_error'] = True
            dict_return['str_status'] = e
        except Exception as e:
            if cursor is not None:
                cursor.close()
            print("General Error: {}".format(e))
            dict_return['str_status'] = e
        return dict_return

    @staticmethod
    def add_dns_entry(conn, entry_text, record_type, record_data):
        dict_return = dict()
        dict_return['bool_error'] = False
        dict_return['str_status'] = None
        cursor = None
        try:
            entry_text = str(entry_text).lower()
            record_to_insert = (str(entry_text), int(record_type), json.dumps(record_data))
            cursor = conn.cursor()
            insert_update_query = """
                               INSERT INTO public.dns_entries(entry_text, record_type, record_data) VALUES (%s, %s, %s);
                                """
            cursor.execute(insert_update_query, record_to_insert)
            cursor.close()
        except Error as e:
            if cursor is not None:
                cursor.close()
            print("SQL Error: {}".format(e))
            dict_return['bool_error'] = True
            dict_return['str_status'] = e
        except Exception as e:
            if cursor is not None:
                cursor.close()
            print("General Error: {}".format(e))
            dict_return['str_status'] = e
        return dict_return

    @staticmethod
    def get_country_listing(conn):
        dict_return = dict()
        dict_return['bool_error'] = False
        dict_return['str_status'] = None
        dict_return['data'] = []
        cursor = None
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            lookup_query = "SELECT country_id, country_code, country_name FROM public.country_list;;"
            cursor.execute(lookup_query)
            dict_return['data'] = json.dumps(cursor.fetchall(), indent=2)
            cursor.close()
        except Error as e:
            if cursor is not None:
                cursor.close()
            print("SQL Error: {}".format(e))
            dict_return['bool_error'] = True
            dict_return['str_status'] = e
        except Exception as e:
            if cursor is not None:
                cursor.close()
            print("General Error: {}".format(e))
            dict_return['str_status'] = e
        return dict_return

    @staticmethod
    def get_ipv4_subnet_listing(conn, country_id):
        dict_return = dict()
        dict_return['bool_error'] = False
        dict_return['str_status'] = None
        dict_return['data'] = []
        cursor = None
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            record_to_search = (str(country_id),)
            lookup_query = """SELECT network FROM public."IPv4_networks" WHERE country_id = %s;"""
            cursor.execute(lookup_query, record_to_search)
            dict_return['data'] = json.dumps(cursor.fetchall(), indent=2)
            cursor.close()
        except Error as e:
            if cursor is not None:
                cursor.close()
            print("SQL Error: {}".format(e))
            dict_return['bool_error'] = True
            dict_return['str_status'] = e
        except Exception as e:
            if cursor is not None:
                cursor.close()
            print("General Error: {}".format(e))
            dict_return['str_status'] = e
        return dict_return

    @staticmethod
    def get_ipv6_subnet_listing(conn, country_id):
        dict_return = dict()
        dict_return['bool_error'] = False
        dict_return['str_status'] = None
        dict_return['data'] = []
        cursor = None
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            record_to_search = (str(country_id),)
            lookup_query = """SELECT network FROM public."IPv6_networks" WHERE country_id = %s;"""
            cursor.execute(lookup_query, record_to_search)
            dict_return['data'] = json.dumps(cursor.fetchall(), indent=2)
            cursor.close()
        except Error as e:
            if cursor is not None:
                cursor.close()
            print("SQL Error: {}".format(e))
            dict_return['bool_error'] = True
            dict_return['str_status'] = e
        except Exception as e:
            if cursor is not None:
                cursor.close()
            print("General Error: {}".format(e))
            dict_return['str_status'] = e
        return dict_return
