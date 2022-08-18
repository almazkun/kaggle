import utils

file = "datasets/amex-default-prediction/train_data.csv"
db_file = "data.sqlite3"
table_name = "from_csv"

col_names = utils.get_line(file, 0).split(",")
col_types = ["TEXT"] * len(col_names)

conn = utils.connect_to_sqlite(db_file)

utils.create_table(conn, table_name, col_names, col_types)

length = utils.get_length(file)

print("Inserting data into table...", length)

col_names = utils.get_line(file, 0)

for i in range(1, length + 1):
    line = utils.get_line(file, i)
    utils.insert_into_table(conn, table_name, col_names, line)

conn.close()
print("Done")
