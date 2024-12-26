import sys, psycopg2, psycopg2.sql as sql
import psycopg2.extras
from flask import jsonify
import json
import pandas as pd

class Connector:
    def __init__(self, user, password, host, port, dbname, table, primarykey):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dbname = dbname
        self.table = table
        self.primarykey = primarykey
    
    def connect(self):
        try:
            connection = psycopg2.connect(
                user = self.user,
                password = self.password,
                host = self.host,
                port = self.port,
                dbname = self.dbname
                )
            cursor = connection.cursor()
            print(
                '------------------------------------------------------------'
                '\n-# PostgreSQL connection & transaction is ACTIVE\n'
                )
        except(Exception, psycopg2.Error) as error :
            print(error, error.pgcode, error.pgerror, sep = '\n')
            sys.exit()
        else:
            self._connection = connection
            self._cursor = cursor
            self._counter = 0

    def _check_connection(self):
        try:
            self._connection
        except AttributeError:
            print('ERROR: NOT Connected to Database')
            sys.exit()
    
    def _execute(self, query, Placeholder_value = None):
        self._check_connection()
        if Placeholder_value == None or None in Placeholder_value:
            self._cursor.execute(query)
            print( '-# ' + query.as_string(self._connection) + ';\n' )
        else:
            self._cursor.execute(query, Placeholder_value)
            print( '-# ' + query.as_string(self._connection) % Placeholder_value + ';\n' )

    def commit(self):
        self._check_connection()
        self._connection.commit()
        print('-# COMMIT '+ str(self._counter) +' changes\n')
        self._counter = 0
        
    def close(self, commit = False):
        self._check_connection()
        if commit:
            self.commit()
        else:
            self._cursor.close()
            self._connection.close()
        if self._counter > 0:
            print(
                '-# '+ str(self._counter) +' changes NOT commited  CLOSE connection\n'
                '------------------------------------------------------------\n'
            )
        else:
            print(
                '-# CLOSE connection\n'
                '------------------------------------------------------------\n'
            )

    # get by value
    def select(self, columns, primaryKey_value = None):
        if primaryKey_value == None:
            select_query = sql.SQL("SELECT {} FROM {}").format(
                sql.SQL(',').join(map(sql.Identifier, columns)),
                sql.Identifier(self.table)
            )
            self._execute( select_query )
        else:
            select_query = sql.SQL("SELECT {} FROM {} WHERE {} = {}").format(
                sql.SQL(',').join(map(sql.Identifier, columns)),
                sql.Identifier(self.table),
                sql.Identifier(self.primarykey),
                sql.Placeholder()
            )
            self._execute( select_query, ( primaryKey_value,))
        try:
            selected = self._cursor.fetchall()
        except psycopg2.ProgrammingError as error:
            selected = '# ERROR: ' + str(error)
        else:
            print('-# ' + str(selected) + '\n')
            return selected
            # cursor.execute("Select * from budgets")
            # print(cursor.fetchall())

    # get all data
    def select_all(self, primaryKey_value = None):
        if primaryKey_value == None:
            select_query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(self.table))
            self._execute( select_query )
        else:
            select_query = sql.SQL("SELECT * FROM {} WHERE {} = {}").format(
                sql.Identifier(self.table),
                sql.Identifier(self.primarykey),
                sql.Placeholder()
            )
            self._execute( select_query, ( primaryKey_value,))
        try:
            selected = self._cursor.fetchall()
        except psycopg2.ProgrammingError as error:
            selected = '# ERROR: ' + str(error)
        else:
            print(jsonify(selected))
            df = pd.DataFrame(selected)
            result = df.to_json(orient="records")
            parsed = json.loads(result)
            print(json.dumps(json.dumps(parsed, indent=4)))
            # print('-# ' + str(jsonify(selected)) + '\n')
            return selected
        
    def get_tables(self):
        # cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        self._cursor.execute("""SELECT table_schema, table_name
                        FROM information_schema.tables
                        WHERE table_schema != 'pg_catalog'
                        AND table_schema != 'information_schema'
                        AND table_type='BASE TABLE'
                        ORDER BY table_schema, table_name""")

        tables = self._cursor.fetchall()

        for row in tables:
            print("row",row)
            print("{}.{}".format(row.index(0), row.index(1)))

        self._cursor.close()

        return tables
