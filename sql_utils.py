import psycopg2
import sys

HOST = "192.168.72.156"
DB = "speedlimits_20181126"
USER = "worker"
PASSWORD = "worker"

def query(select_statement):
    response = None
    con = None
    try:
        #con = psycopg2.connect(host='localhost', database="smk", user="postgres", password="")
        con = psycopg2.connect(host=HOST, database=DB, user=USER,
                password=PASSWORD)
        cur = con.cursor()
        cur.execute(select_statement)
        response = cur.fetchall()
    except (psycopg2.DatabaseError) as e:
        print(e)
        if con:
            con.rollback()
            sys.exit(1)
    finally:
        if con:
            con.close()
    #print(response)
    return response

def act(action):
    response = None
    con = None
    try:
        con = psycopg2.connect(host='localhost', database="smk", user="postgres", password="")
        cur = con.cursor()
        cur.execute(action)
        con.commit()
    except (psycopg2.DatabaseError) as e:
        print(e)
        if con:
            con.rollback()
            sys.exit(1)
    finally:
        if con:
            con.close()
