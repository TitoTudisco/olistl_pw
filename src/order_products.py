import psycopg
import datetime
from dotenv import load_dotenv
import os
import src.common as common

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")


# Metodo EXTRACT per order_products
def extract():
    print("questo è il metodo EXTRACT degli order_products")
    df = common.readFile()
    return df


# Metodo TRANSFORM per order_products
def transform(df):
    print("questo è il metodo TRANSFORM degli order_products")
    df = common.drop_duplicates(df)
    df = common.check_nulls(df, ["order_id", "order_item", "product_id", "seller_id", "price", "freight"])
    # Eventuali ulteriori trasformazioni possono essere aggiunte qui
    return df


# Metodo LOAD per order_products
def load_order_products(df):
    # Aggiunge la colonna last_updated con il timestamp corrente
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    print("Questo è il metodo LOAD degli order_products")

    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            # Crea la tabella order_products
            sql = """
            CREATE TABLE order_products (
                order_id VARCHAR,
                order_item INTEGER,
                product_id VARCHAR,
                seller_id VARCHAR,
                price FLOAT,
                freight FLOAT,
                last_updated TIMESTAMP,
                PRIMARY KEY (order_id, order_item)
            );
            """
            try:
                cur.execute(sql)
            except psycopg.errors.DuplicateTable as ex:
                conn.commit()
                print(ex)
                domanda = input("Vuoi cancellare la tabella? SI NO  ")
                if domanda.upper() == "SI":
                    sqldelete = "DROP TABLE order_products"
                    cur.execute(sqldelete)
                    conn.commit()
                    print("Ricreo la tabella order_products")
                    cur.execute(sql)

            # Inserimento dei record con gestione dei conflitti
            sql = """
            INSERT INTO order_products
                (order_id, order_item, product_id, seller_id, price, freight, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id, order_item) DO UPDATE
            SET (product_id, seller_id, price, freight, last_updated) =
                (EXCLUDED.product_id, EXCLUDED.seller_id, EXCLUDED.price, EXCLUDED.freight, EXCLUDED.last_updated);
            """

            # Utilizza la barra di avanzamento per inserire riga per riga
            common.caricamento_barra(df, cur, sql)
            conn.commit()


def main():
    print("questo è il metodo MAIN degli order_products")
    df = extract()
    df = transform(df)
    print("Dati trasformati:")
    print(df, end="\n\n")
    load_order_products(df)


# Esecuzione dello script se chiamato direttamente
if __name__ == "__main__":
    main()