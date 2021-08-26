import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_schema_queries

def create_schemas(cur,conn):
    """
        Description: This function create the schemas which will hold the staging and the model tables.

        Arguments:
            cur: the cursor object.
            conn: the connection object.

        Returns:
            None
    """
    for query in create_schema_queries:
        cur.execute(query)
        conn.commit()


def drop_tables(cur, conn):
    """
        Description: This function drop all the tables in the staging and model schemas.

        Arguments:
            cur: the cursor object.
            conn: the connection object.

        Returns:
            None
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Description: This function create the tables in the staging and model schemas.

        Arguments:
            cur: the cursor object.
            conn: the connection object.

        Returns:
            None
    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    create_schemas(cur,conn)

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()