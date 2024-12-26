import psycopg2
import psycopg2.extras
import sys, psycopg2, psycopg2.sql as sql

# get table
def get_tables(connection):
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("""SELECT table_schema, table_name
                      FROM information_schema.tables
                      WHERE table_schema != 'pg_catalog'
                      AND table_schema != 'information_schema'
                      AND table_type='BASE TABLE'
                      ORDER BY table_schema, table_name""")
    tables1 = cursor.fetchall()

    cursor.execute("""SELECT u.view_name, u.table_schema
                      FROM information_schema.view_table_usage u
                      WHERE u.table_schema != 'pg_catalog'
                      AND u.table_schema != 'information_schema'""")

    tables2 = cursor.fetchall()

    cursor.close()

    return tables1+tables2

# print table list
def print_tables(tables):

    """
    Prints the list created by get_tables
    """

    for row in tables:

        print("{}.{}".format(row["table_schema"], row["table_name"]))
# CREATE TABLE public.history
# (
#     id bigint SERIAL NOT NULL PRIMARY KEY,
#     query_str text NOT NULL,
#     json_obj jsonb NOT NULL,
#     PRIMARY KEY (id)
#         INCLUDE(id)
#         DEFERRABLE
# );
def insert(connection, table_name, **column_value):
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    insert_query  = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join( map( sql.Identifier, column_value.keys() ) ),
        sql.SQL(', ').join(sql.Placeholder() * len(column_value.values()))
    )
    record_to_insert = tuple(column_value.values())
    cursor.execute(insert_query, record_to_insert )
    # cursor.counter += 1

def update(connection, table_name, column, column_value,primarykey, primaryKey_value):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        update_query  = sql.SQL("UPDATE {} SET {} = {} WHERE {} = {}").format(
            sql.Identifier(table_name),
            sql.Identifier(column),
            sql.Placeholder(),
            sql.Identifier(primarykey),
            sql.Placeholder()
        )
        cursor.execute(update_query, ( column_value, primaryKey_value))
        # self._counter += 1

def update_multiple_columns(connection, table_name, columns, columns_value,primarykey, primaryKey_value):
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    update_query  = sql.SQL("UPDATE {} SET ({}) = ({}) WHERE {} = {}").format(
        sql.Identifier(table_name),
        sql.SQL(',').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(columns_value)),
        sql.Identifier(primarykey),
        sql.Placeholder()
    )
    Placeholder_value = list(columns_value)
    Placeholder_value.append(primaryKey_value)
    Placeholder_value = tuple(Placeholder_value)
    cursor.execute(update_query, Placeholder_value)
    # self._counter += 1

def select_by_quary_name(connection,table_name,primarykey, primaryvalue = None):
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if primaryvalue == None:
        select_query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
        cursor.execute( select_query )
    else:
        select_query = sql.SQL("SELECT * FROM {} WHERE {} = {}").format(
        sql.Identifier(table_name),
        sql.Identifier(primarykey),
        sql.Placeholder()
        )
        cursor.execute( select_query, ( primaryvalue,))
    try:
        selected = cursor.fetchall()
    except psycopg2.ProgrammingError as error:
        selected = '# ERROR: ' + str(error)
    else:
        print('-# ' + str(selected) + '\n')
        return selected

def delete(connection, table, primaryKey, primaryKey_value):
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    delete_query  = sql.SQL("DELETE FROM {} WHERE {} = {}").format(
        sql.Identifier(table),
        sql.Identifier(primaryKey),
        sql.Placeholder()
    )
    print('-# ' + str(delete_query) + '\n')
    cursor.execute(delete_query, ( primaryKey_value,))
    # self._counter += 1

# get table columns
def get_columns(connection, table_schema, table_name):

    """
    Creates and returns a list of dictionaries for the specified
    schema.table in the database connected to.
    """

    where_dict = {"table_schema": table_schema, "table_name": table_name}

    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("""SELECT column_name, ordinal_position, is_nullable, data_type, character_maximum_length
                      FROM information_schema.columns
                      WHERE table_schema = %(table_schema)s
                      AND table_name   = %(table_name)s
                      ORDER BY ordinal_position""",
                      where_dict)

    columns = cursor.fetchall()

    cursor.close()

    return columns


def print_columns(columns):

    """
    Prints the list created by get_columns.
    """

    for row in columns:

        print("Column Name:              {}".format(row["column_name"]))
        print("Ordinal Position:         {}".format(row["ordinal_position"]))
        print("Is Nullable:              {}".format(row["is_nullable"]))
        print("Data Type:                {}".format(row["data_type"]))
        print("Character Maximum Length: {}\n".format(row["character_maximum_length"]))

def get_tree(connection):

    """
    Uses get_tables and get_columns to create a tree-like data
    structure of tables and columns.

    It is not a true tree but a list of dictionaries containing
    tables, each dictionary having a second dictionary
    containing column information.
    """

    tree = get_tables(connection)

    for table in tree:

        table["columns"] = get_columns(connection, table["table_schema"], table["table_name"])

    return tree


def print_tree(tree):

    """
    Prints the tree created by get_tree
    """

    for table in tree:

        print("{}.{}".format(table["table_schema"], table["table_name"]))

        for column in table["columns"]:

            print(" |-{} ({})".format(column["column_name"], column["data_type"]))

def commit(self):
        self._check_connection()
        self._connection.commit()
        print('-# COMMIT '+ str(self._counter) +' changes\n')
        self._counter = 0

def _check_connection(self):
        try:
            self._connection
        except AttributeError:
            print('ERROR: NOT Connected to Database')
            sys.exit()