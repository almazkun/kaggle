import sqlite3


def yield_line(file, line_number):
    return (x for i, x in enumerate(open(file)) if i == line_number)


def get_line(file, line_number):
    return next(yield_line(file, line_number))


def get_length(file):
    return sum(1 for _ in open(file))


def merge_2_files_line_by_line(file_1, file_2, output_file):
    num_lines = get_length(file_1)

    for i in range(num_lines):
        line_1 = get_line(file_1, i)
        line_2 = get_line(file_2, i)
        with open(output_file, 'a') as f:
            f.write(line_1 + "," + line_2 + "\n")

    print("Merged file saved to {}".format(output_file))


def connect_to_sqlite(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception as e:
        print(e)

    return conn

def create_table(conn, table_name, col_names, col_types):
    c = conn.cursor()
    col_name_col_type = ",".join([f"{col_name} {col_type}" for col_name, col_type in zip(col_names, col_types)])
    
    command = f"CREATE TABLE if not Exists {table_name} ({col_name_col_type})"
    c.execute(command)
    conn.commit()
    return c

def insert_into_table(conn, table_name, col_names, col_values):
    c = conn.cursor()
    bindings = ",".join(["?"] * len(col_values.split(",")))
    sql_command = f''' INSERT INTO {table_name} ({col_names}) VALUES({bindings}) '''
    c.execute(sql_command, col_values.split(","))
    conn.commit()
    return c