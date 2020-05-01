import requests
import json
import pymysql

import runtime
import metadata



config = metadata.Configure()
env = config.get()

table_label = "uplink_process"
id_range = [1,3]

id_range_string = "-9999"
id_range_string = ",".join(str(x) for x in id_range)

print("id_range_string", id_range_string)

raw_data = requests.post("http://" + "109.74.8.89" + "/common/get_db_rows.php", {'tablelabel': table_label, 'idrange': id_range_string})
json_data = raw_data.json()
print("json_data", json_data)
data = json_data['returnstring']
print("type(data)", type(data))
print("len(data)", len(data))
print(data[0])

table_name = "t_" + table_label
unique_col_name = table_label.upper() + "_UNIQUE_INDEX"

#select_sql = "SELECT " + unique_col_name + " FROM " + table_name + " WHERE " + unique_col_name + " IN (" + id_range_string + ")"
#print(select_sql)

delete_sql = "DELETE FROM " + table_name + " WHERE " + unique_col_name + " IN (" + id_range_string + ")"
print(delete_sql)

insert_sql = "INSERT INTO " + table_name + " ("
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
print(insert_sql)


try:
    conn = pymysql.connect(host = env['STORE_DATABASE_HOST'], user = env['STORE_DATABASE_USER'], passwd = env['STORE_DATABASE_PASSWD'], db = env['STORE_DATABASE_DB'], autocommit = True)
except (pymysql.err.OperationalError, pymysql.err.Error) as e:
    runtime.logging.exception(e)


insert_result = -1

try:
    
    with conn.cursor() as cursor :

        try:
            cursor.execute(delete_sql)
        except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
            runtime.logging.exception(e)

    with conn.cursor() as cursor :

        try:
            cursor.execute(insert_sql)
        except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
            runtime.logging.exception(e)
        insert_result = cursor.rowcount

except (pymysql.err.OperationalError, pymysql.err.Error) as e:

    runtime.logging.exception(e)
