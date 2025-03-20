import psycopg
from dotenv import load_dotenv
import os
# 3 metodi ETL per CUSTOMER
import src.common as common
import datetime

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

# 3 metodi ETL per PRODUCTS

def extract():
    print("questo è il metodo EXTRACT dei prodotti")
    df = common.readFile()
    return df


def transform(df):
    print("questo è il metodo TRANSFORM dei prodotti")
    df = common.drop_duplicates(df)
    df = common.check_nulls(df, ["product_id","category","product_name_lenght",
                                 "product_description_lenght","product_photos_qty"])

    df = common.format_string(df, ["category"])
    # common.saveProcessed(df)
    return df


def load_products(df):
    # Aggiunge una colonna con il timestamp di aggiornamento
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    print("Questo è il metodo LOAD dei prodotti")

    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            # Crea la tabella products con le colonne corrispondenti
            sql = """
            CREATE TABLE products (
                product_id VARCHAR PRIMARY KEY,
                category VARCHAR,
                product_name_lenght FLOAT,
                product_description_lenght FLOAT,
                product_photos_qty FLOAT,
                last_updated TIMESTAMP
            );
            """
            try:
                cur.execute(sql)
            except psycopg.errors.DuplicateTable as ex:
                conn.commit()
                print(ex)
                domanda = input("Vuoi cancellare la tabella? SI NO  ")
                if domanda.upper() == "SI":
                    sqldelete = "DROP TABLE products"
                    cur.execute(sqldelete)
                    conn.commit()
                    print("Ricreo la tabella products")
                    cur.execute(sql)

            # Inserisce i dati nel database, aggiornando in caso di conflitto sulla chiave primaria (product_id)
            sql = """
            INSERT INTO products
                (product_id, category, product_name_lenght, product_description_lenght, product_photos_qty, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE
            SET (category, product_name_lenght, product_description_lenght, product_photos_qty, last_updated) = 
                (EXCLUDED.category, EXCLUDED.product_name_lenght, EXCLUDED.product_description_lenght, EXCLUDED.product_photos_qty, EXCLUDED.last_updated);
            """

            # Utilizza la funzione di caricamento con barra di avanzamento per inserire riga per riga
            common.caricamento_barra(df, cur, sql)
            conn.commit()


def main():
    print("questo è il metodo MAIN dei prodotti")
    df = extract()
    df=transform(df)
    print("Dati trasformati")
    print(df, end="\n\n") #vedere una riga sola
    load_products(df)


# éer usare questo file come fosse un modulo
if __name__ == "__main__": # indica ciò che viene eseguito quando eseguo direttamente
    main()