import psycopg2

class BmsDbConnection:

    def get_db_connection():
        conn = psycopg2.connect("dbname=bms_datas_analysis host='localhost' user='postgres' password='iSGM@1234'")
        return conn