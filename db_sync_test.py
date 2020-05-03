import requests
import json
import pymysql

import runtime
import metadata


def get_sql_strings(data, table_label) :

    table_name = "t_" + table_label
    unique_col_name = table_label.upper() + "_UNIQUE_INDEX"

    id_range = [rec[unique_col_name] for rec in data]
    id_range_string = ",".join(str(x) for x in id_range)

    delete_sql = ""
    delete_sql += "DELETE FROM " + table_name + " WHERE " + unique_col_name + " IN (" + id_range_string + ")"

    insert_sql = ""
    insert_sql += "INSERT INTO " + table_name + " ("
    row = data[0]
    for column in row :
        insert_sql += column + ","
    insert_sql = insert_sql[:-1]
    insert_sql += ") VALUES "        
    for row in data :
        insert_sql += "("
        for column in row :
            if row[column] == None :
                str_value = 'NULL' + ","
            else :
                str_value = "'" + str(row[column]) + "',"
            insert_sql += str_value
        insert_sql = insert_sql[:-1]
        insert_sql += "),"
    insert_sql = insert_sql[:-1]
    return {'delete_sql' : delete_sql , 'insert_sql' : insert_sql}
    

config = metadata.Configure()
env = config.get()
print(env['STORE_PATH'])

table_label = "channel"
id_range = []

id_range_string = "-9999"
if len(id_range) > 0 :
    id_range_string = ",".join(str(x) for x in id_range)

raw_data = requests.post("http://" + "109.74.8.89" + "/common/get_db_rows.php", {'tablelabel': table_label, 'idrange': id_range_string})
json_data = raw_data.json()
data = json_data['returnstring']

with open(env['STORE_PATH'] + table_label + ".json", "w", encoding = "utf-8") as f:
    json.dump(data, f, ensure_ascii = False, indent = 4)

sql_strings = get_sql_strings(data, table_label)
print("sql_strings['delete_sql']", sql_strings['delete_sql'])
print("sql_strings['insert_sql']", sql_strings['insert_sql'])


try:
    conn = pymysql.connect(host = env['STORE_DATABASE_HOST'], user = env['STORE_DATABASE_USER'], passwd = env['STORE_DATABASE_PASSWD'], db = env['STORE_DATABASE_DB'], autocommit = True)
except (pymysql.err.OperationalError, pymysql.err.Error) as e:
    runtime.logging.exception(e)

insert_result = -1

try:
    
    with conn.cursor() as cursor :

        try:
            cursor.execute(sql_strings['delete_sql'])
        except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
            runtime.logging.exception(e)

    with conn.cursor() as cursor :

        try:
            cursor.execute(sql_strings['insert_sql'])
        except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
            runtime.logging.exception(e)
        insert_result = cursor.rowcount

except (pymysql.err.OperationalError, pymysql.err.Error) as e:

    runtime.logging.exception(e)
