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


# Metodo EXTRACT per orders
def extract():
    print("questo è il metodo EXTRACT degli orders")
    df = common.readFile()
    return df


# Metodo TRANSFORM per orders
def transform(df):
    print("questo è il metodo TRANSFORM degli orders")
    # Rimuove duplicati
    df = common.drop_duplicates(df)
    # Verifica che le colonne essenziali non abbiano valori nulli
    df = common.check_nulls(df, ["order_id", "customer_id", "order_status",
                                 "order_purchase_timestamp", "order_delivered_customer_date",
                                 "order_estimated_delivery_date"])
    # Eventuali altre trasformazioni possono essere aggiunte qui
    df = common.format_string(df, ["order_status"])
    return df


# Metodo LOAD per orders
def load_orders(df):
    # Aggiunge la colonna last_updated con il timestamp corrente
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    print("Questo è il metodo LOAD degli orders")

    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            # Creazione della tabella orders
            sql = """
            CREATE TABLE orders (
                order_id VARCHAR PRIMARY KEY,
                customer_id VARCHAR,
                order_status VARCHAR,
                order_purchase_timestamp TIMESTAMP,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP,
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
                    sqldelete = "DROP TABLE orders"
                    cur.execute(sqldelete)
                    conn.commit()
                    print("Ricreo la tabella orders")
                    cur.execute(sql)

            # Inserimento dei record con gestione dei conflitti sulla chiave primaria (order_id)
            sql = """
            INSERT INTO orders
                (order_id, customer_id, order_status, order_purchase_timestamp, order_delivered_customer_date, order_estimated_delivery_date, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE
            SET (customer_id, order_status, order_purchase_timestamp, order_delivered_customer_date, order_estimated_delivery_date, last_updated) =
                (EXCLUDED.customer_id, EXCLUDED.order_status, EXCLUDED.order_purchase_timestamp, EXCLUDED.order_delivered_customer_date, EXCLUDED.order_estimated_delivery_date, EXCLUDED.last_updated);
            """

            # Inserimento riga per riga con visualizzazione barra di avanzamento
            common.caricamento_barra(df, cur, sql)
            conn.commit()


def main():
    print("questo è il metodo MAIN degli orders")
    df = extract()
    df = transform(df)
    print("Dati trasformati:")
    print(df, end="\n\n")
    load_orders(df)


# Esecuzione del main se il file viene eseguito direttamente
if __name__ == "__main__":
    main()
