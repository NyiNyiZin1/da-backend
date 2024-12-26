from flask import Flask, jsonify, request
import json
from flask_restful import Resource, Api
from flask_cors import CORS
import psycopg2.extras
from commons.bms_db_connection import BmsDbConnection
from commons.db_connection import DbConnection
from pgadminschema import postgresqlschemareader

app = Flask(__name__)
CORS(app)

@app.get('/api/v01/schema')
def get_schema():
    bms_conn = BmsDbConnection.get_db_connection()
    tables = postgresqlschemareader.get_tables(bms_conn)
    schemaList = []
    for table in tables:
        columnList = []
        if "view_name" in table:
            columns = postgresqlschemareader.get_columns(bms_conn, "public", table["view_name"])
        else:
            columns = postgresqlschemareader.get_columns(bms_conn, "public", table["table_name"])
        for column in columns:
            columnList.append({
                "label" : column['column_name'],
                "type" : inputTypeConverter(column["data_type"])
            })
        if "view_name" in table:
            schemadata = {
                table["view_name"] : columnList
            }
        else:
            schemadata = {
                table["table_name"] : columnList
            }
        schemaList.append(schemadata)
    return jsonify(schemaList)

@app.post('/api/v01/request')
def get_data():
    bms_conn = BmsDbConnection.get_db_connection()
    db_conn = DbConnection.get_db_connection()
    history_data = postgresqlschemareader.select_by_quary_name(db_conn,"data_history","query_name",primaryvalue = request.json['query_name'])
    if len(history_data) == 0:
        # save query and json object
        postgresqlschemareader.insert(db_conn,"data_history",
        query_str = request.json['query'],
        db_table_name = request.json['table_name'],
        query_name = request.json['query_name']
        )
        db_conn.commit()
    # get data by query
    cur = bms_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    print(request.json['query'])
    cur.execute(request.json['query'])
    obj = cur.fetchall()
    metadata = {}
    attributes_names = []
    measures_names = []
    columnList = []
    dataList = []
    columns = postgresqlschemareader.get_columns(bms_conn, "public", request.json["table_name"])
    for column in columns:
        if(('id' in column['column_name'].lower())):
            continue
        elif(('created' in column['column_name'].lower()) or ('updated' in column['column_name'].lower())):
            continue
        else:
            columnList.append(column["column_name"])
            if(inputTypeConverter(column["data_type"]) == 'number' or inputTypeConverter(column["data_type"]) == 'decimal'):
                measures_names.append(column["column_name"])
            else:
                attributes_names.append(column["column_name"])
    
    colList = []
    measuresList = set(())
    attributesList = set(())

    for data in obj:
        values = []
        for column in data.keys():
            if column in measures_names:
                measuresList.add(column)
            elif column in attributes_names:
                attributesList.add(column)
            if column not in colList:
                colList.append(column)
            if column in colList:
                try:
                    values.append(float('{0:.2f}'.format(data[column])))
                except ValueError:
                    values.append(data[column])
        dataList.append(values)
        
    metadata["available_data_row_count"] = len(dataList)
    metadata["data_rows"] = dataList
    metadata["attributes_names"] = list(attributesList)
    metadata["measures_names"] =  list(measuresList)
    metadata["column_names"] = colList
    cur.close()
    bms_conn.close()
    return {"metadata":metadata}

@app.route('/api/v01/budgets', methods=['GET','POST'])
def get_data_list():
    bms_conn = BmsDbConnection.get_db_connection()
    cur = bms_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    i=1
    response = []
    for req in request.json:
        print(req['query'+str(i)])
        cur.execute(req['query'+str(i)])
        budgets = cur.fetchall()
        req['query'+str(i)] = {}
        req['query'+str(i)]["totalCount"] = len(budgets)
        req['query'+str(i)]["budgetsList"] = budgets
        response.append(req['query'+str(i)])
        i+=1

    cur.close()
    bms_conn.close()
    # return (request.get_json(force=True))
    # return jsonify(resBudgets)
    return response

@app.get('/api/v01/queries')
def get_queries_list():
    db_conn = DbConnection.get_db_connection()
    history_data = postgresqlschemareader.select_by_quary_name(db_conn,"data_history","query_name")
    db_conn.commit()
    return history_data

@app.post('/api/v01/dashboard')
def save_dashboard():
    db_conn = DbConnection.get_db_connection()
    dashboard_data = postgresqlschemareader.select_by_quary_name(db_conn,"dashboard_data","query_name",primaryvalue = request.json['query_name'])
    if len(dashboard_data) == 0:
        # save query and json object
        postgresqlschemareader.insert(db_conn,"dashboard_data",
        query_name = request.json['query_name'],
        data = request.json['data']
        )
    else:
        postgresqlschemareader.update(db_conn,"dashboard_data","data",request.json['data'],"query_name",request.json['query_name'])
    db_conn.commit()
    return json.dumps({"result":True}), 201, {"ContentType":"application/json"} 

@app.get('/api/v01/dashboard')
def get_data_query_id():
    db_conn = DbConnection.get_db_connection()
    name = request.args.get('query_name')
    historyData = {}
    history_data = postgresqlschemareader.select_by_quary_name(db_conn,"dashboard_data","query_name",request.args.get('query_name'))
    if len(history_data) != 0:
        historyData["id"] = history_data[0]['id']
        historyData["data"] = history_data[0]['data']
        historyData["query_name"] = history_data[0]['query_name']
    return historyData

@app.delete('/api/v01/querie')
def delete():
    db_conn = DbConnection.get_db_connection()
    name = request.args.get('query_name')
    print("result",name)
    postgresqlschemareader.delete(db_conn,"data_history","query_name",primaryKey_value=request.args.get('query_name'))
    postgresqlschemareader.delete(db_conn,"dashboard_data","query_name",primaryKey_value=request.args.get('query_name'))
    db_conn.commit()
    return json.dumps({"result":True}), 200, {"ContentType":"application/json"} 

def inputTypeConverter(colType):
    if('int' in colType.lower()):
        return 'number'
    elif('double' in colType.lower()):
        return 'decimal'
    elif('text' in colType.lower()):
        return 'text'
    elif('char' in colType.lower()):
        return 'text'
    elif('date' in colType.lower()):
        return 'date'
    elif('timestamp' in colType.lower()):
        return 'datetime'
    else:
        return 'text'
    
def isShow(dataKey, columnName):
    if(dataKey in columnName):
        return dataKey
    else:
        return None

if __name__ == '__main__':
    app.run(debug=True)