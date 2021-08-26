import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        Description: This function run the copy command to to stage the files in the tables in the staging schema.

        Arguments:
            cur: the cursor object.
            conn: the connection object.

        Returns:
            None
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        Description: This function populate the tables in the model schema from the tables in the staging schema.

        Arguments:
            cur: the cursor object.
            conn: the connection object.

        Returns:
            None
    """

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()