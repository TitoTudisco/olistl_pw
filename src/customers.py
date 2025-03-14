import psycopg
from dotenv import load_dotenv
import os
# 3 metodi ETL per CUSTOMER
import src.common as common

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")


def extract():
    print ("questo è il metodo EXTRACT dei clienti")
    df = common.readFile()
    return df

def transform(df):
    print ("questo è il metodo TRANSFORM dei clienti")
    df = common.drop_duplicates(df)
    df= common.check_nulls(df, ["customer_id"])
    df = common.format_string(df, ["region", "city"])
    df = common.formatcap(df)
   # common.saveProcessed(df)
    print(df)
    return df

def load(df):
    print("Questo è il metodo LOAD dei clienti")
    #debug print(df)


    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = """
            CREATE TABLE  customers (
            pk_customer VARCHAR PRIMARY KEY,
            region VARCHAR,
            city VARCHAR,
            cap VARCHAR
            );
            """

            try:
                cur.execute(sql) # Inserimento report nel database
            except psycopg.errors.DuplicateTable as ex:
                conn.commit()
                print(ex)
                domanda = input("Vuoi cancellare la tabella? SI NO  ")
                if domanda == "SI":
                    sqldelete = """DROP TABLE customers"""
                    cur.execute(sqldelete)
                    conn.commit()
                    print("Ricreo la tabella customers")
                    cur.execute(sql)

            sql = """
            INSERT INTO customers
            (pk_customer, region, city, cap)
            VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING; 
            """


            common.caricamento_barra(df,cur,sql)

            conn.commit()
#integrazione città / regione
def complete_city_region () :
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port)as conn:
        with conn.cursor() as cur:
            sql = """
            SELECT * 
            FROM customers
            WHERE city = 'Nan' or region = 'Nan';
            """

            cur.execute(sql)
            for record in cur:
                print(record)


def main():
    print("questo è il metodo MAIN dei clienti")
    df = extract()
    df=transform(df)
    load(df)


#per usare questo file come fosse un modulo
#I metodi definiti sopra vanno importati per poter essere utilizzati.

# __name__ =

if __name__ == "__main__": # indica ciò che viene eseguito quando eseguo direttamente
    main()