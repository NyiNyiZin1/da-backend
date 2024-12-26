from flask import Flask, jsonify
from flask_restful import Resource, Api
import psycopg2.extras
import commons.db_connection as DbConnection
from pgadminschema import postgresqlschemareader

app = Flask(__name__)

@app.get('/api/v01/budgets')
def get_users():
    conn = DbConnection.DbConnection.get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM budgets")
    budgets = cur.fetchall()
    cur.close()
    conn.close()
    resBudgets = {}
    resBudgets["totalCount"] = len(budgets)
    resBudgets["budgetsList"] = budgets
    return jsonify(resBudgets)

@app.get('/api/v01/schema')
def get_schema():
    conn = DbConnection.get_db_connection()
    tables = postgresqlschemareader.get_tables(conn)
    schemaList = []
    for table in tables:
        columnList = []
        columns = postgresqlschemareader.get_columns(conn, "public", table["table_name"])
        for column in columns:
            if(('id' in column['column_name'].lower())):
                continue
            elif(('created' in column['column_name'].lower()) or ('updated' in column['column_name'].lower())):
                continue
            else:
                columnData = {
                    column["column_name"] : inputTypeConverter(column["data_type"])
                    #column["column_name"] : column
                }
            columnList.append(columnData)
        schemadata = {
            table["table_name"] : columnList
        }
        schemaList.append(schemadata)
    return jsonify(schemaList)

def inputTypeConverter(colName):
    if('int' in colName.lower()):
        return 'number'
    elif('double' in colName.lower()):
        return 'number'
    elif('text' in colName.lower()):
        return 'text'
    elif('char' in colName.lower()):
        return 'text'
    elif('date' in colName.lower()):
        return 'date'
    else:
        return 'text'

if __name__ == '__main__':
    app.run(debug=True)