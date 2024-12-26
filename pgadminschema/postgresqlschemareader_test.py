
import psycopg2
import postgresqlschemareader

def main():

    """
    Test and demonstrate the functions of the postgresqlschemareader module.
    """

    print("--------------------------")
    print("| Read PostgreSQL Schema |")
    print("--------------------------\n")

    try:

        conn = psycopg2.connect("dbname=bms_datas_analysis host='localhost' user='postgres' password='iSGM@1234'")

        tables = postgresqlschemareader.get_tables(conn)
        print("BMS Table List\n===================")
        postgresqlschemareader.print_tables(tables)

        columns = postgresqlschemareader.get_columns(conn, "public", "budgets")
        print("\nColumns in budgets Table\n=======================")
        postgresqlschemareader.print_columns(columns)

        tree = postgresqlschemareader.get_tree(conn)
        print("Database python\n=====================")
        postgresqlschemareader.print_tree(tree)

        conn.close()

    except psycopg2.Error as e:

        print(type(e))

        print(e)

main()