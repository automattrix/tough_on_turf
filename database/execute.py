from database import connect


def run_sql(db_path, sql, params):
    con = connect.connect_db(db_path=db_path)
    cur = con.cursor()
    group_sql = sql
    parameters = params
    cur.execute(group_sql, parameters)
    con.close()
