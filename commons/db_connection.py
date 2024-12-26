import psycopg2

class DbConnection:

    def get_db_connection():
        conn = psycopg2.connect("dbname=datas_analysis host='localhost' user='postgres' password='iSGM@1234'")
        return conn