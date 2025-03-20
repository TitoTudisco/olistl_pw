import datetime

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
    df= common.check_nulls(df, ["customer_id","region","city","cap"])
    df = common.format_string(df, ["region", "city"])
    df = common.format_cap(df)
   # common.saveProcessed(df)
    return df

def load(df):
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    print("Questo è il metodo LOAD dei clienti")
    #debug print(df)


    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = """
            CREATE TABLE  customers (
            pk_customer VARCHAR PRIMARY KEY,
            region VARCHAR,
            city VARCHAR,
            cap VARCHAR,
            last_updated TIMESTAMP
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

            #se ci sono doppioni li modifica
            sql = """ 
                  INSERT INTO customers
                  (pk_customer, region, city, cap, last_updated)
                  VALUES (%s, %s, %s, %s, %s)
                  ON CONFLICT (pk_customer) DO UPDATE 
                  SET (region, city, cap, last_updated) = (EXCLUDED.region, EXCLUDED.city, EXCLUDED.cap, EXCLUDED.last_updated);
            """

            common.caricamento_barra(df,cur,sql)

            conn.commit()
#integrazione città / regione
def complete_city_region():
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            #regione
            sql = f"""      
                UPDATE customers AS c1 
                SET region = c2.region,
                last_updated = '{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")}'
                FROM customers AS c2
                WHERE c1.cap = c2.cap
                AND c1.cap <> 'NaN'
                AND c2.cap <> 'NaN'
                AND c1.region = 'NaN'
                AND c2.region <> 'NaN'
                RETURNING *
                ; """
            #città


            cur.execute(sql)
            print("Record con regione aggiornata\n")
            for record in cur :
                print(record)
            #for record in result:
               # print(record)
            #print(cur.rowcount())

            sql = f"""      
                           UPDATE customers AS c1 
                           SET city = c2.city,
                           last_updated = '{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")}'
                           FROM customers AS c2
                           WHERE c1.cap = c2.cap
                           AND c1.cap <> 'NaN'
                           AND c2.cap <> 'NaN'
                           AND c1.region = 'NaN'
                           AND c2.region <> 'NaN'
                           RETURNING *
                           ; """
            # città

            cur.execute(sql)
            print("Record con città  aggiornata\n")
            for record in cur:
                print(record)

def main():
    print("questo è il metodo MAIN dei clienti")
    df = extract()
    df=transform(df)
    print("Dati trasformati")
    print(df, end="\n\n") #vedere una riga sola
    load(df)


#per usare questo file come fosse un modulo
#I metodi definiti sopra vanno importati per poter essere utilizzati.

# __name__ =

if __name__ == "__main__": # indica ciò che viene eseguito quando eseguo direttamente
    main()