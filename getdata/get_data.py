from connector import Connector

table = Connector(
    user = 'postgres',
    password = 'iSGM@1234',
    host = '127.0.0.1',
    port = '5432',
    dbname = 'bms_datas_analysis',
    table = 'budgets',
    primarykey = 'fiscal_year_id'
)

table.connect()

# need to commit after insert data
table.commit()

# table.select(
#     columns = ['fiscal_year_id'],
#     primaryKey_value = '8'
# )

table.select_all()

table.close("commit")