import pandas as pd
import datetime
import os
import psycopg
from dotenv import load_dotenv

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")



#analogo al metodo extract, fare in modo che legga diversi tipi di file
def readFile():
    isValid = False
    df = pd.DataFrame()
    while not isValid:
        path = input("Inserire il path del file:\n").strip()
        try:
            path_list = path.split(".")

            if path_list[-1] == "csv" or path_list[-1] == "txt":
                df = pd.read_csv(path)
            elif path_list[-1] == "xlsx" or path_list[-1] == "xls":
                df = pd.read_excel(path)
            else:
                df = pd.read_json(path)
        except FileNotFoundError as ex:
            print(ex)
        except OSError as ex:
            print(ex)
        else:
            print("Path inserito correttamente")
            isValid = True
    else:
        return df

# metodi per
def caricamento_barra(df,cur,sql):
    print(f"Caricamento in corso... \n{str(len(df))} righe da inserire.")
    print("┌──────────────────────────────────────────────────┐")
    print("│",end="")
    perc_int = 2
    for index, row in df.iterrows():
        perc = float("%.2f" % ((index + 1) / len(df) * 100))
        if perc >= perc_int:
            print("█",end="")
            #print(perc,end="")
            perc_int += 2
        cur.execute(sql, row.to_list())
    print("│ 100% Completato!")
    print("└──────────────────────────────────────────────────┘")

def format_cap (df):
    # Converte in stringa e riempie con zeri fino a 5 cifre
    #if "cap" in df.columns:
        #debug df ["cap"].fillna(value="00000"
        #df["cap"] = np.where(df["cap"] == "nan", [0], df["cap"])
        #df["cap"].astype(str).str.zfill(5, inplace=True)
    df["cap"] = df["cap"].fillna(0).astype(int).astype(str).str.zfill(5)
    print("Questo è il format_cap")
    return df

def format_string(df, cols):
    print(df.info)
    for col in cols:
        df[col] = df[col].str.strip()
        df[col] = df[col].str.replace("[0-9]", "", regex=True)
        df[col] = df[col].str.replace("[\\[\\]$&+:;=?@#|<>.^*(/_)%!]", "", regex=True)
        df[col] = df[col].str.replace(r"\s+", " ", regex=True)
    return df

#no duplicati
def drop_duplicates (df):
    print(df.duplicated())
    df.drop_duplicates()
    return df

# Gestione valori nulli
def check_nulls(df, subset = ""):
    print(subset)
    subset = df.columns.tolist()[0] if not subset else subset
    print(subset)
    print(f"Valori nulli per colonna:\n{df.isnull().sum()}\n")
    df.dropna(subset=subset, inplace=True, ignore_index=True)
    print(df)
    return df
def fillNulls(df):
    #gestione del tipo di valore da aggiornare
    df.fillna(value="nd", axis=0, inplace=True)
    return df

#salvare file con date
def saveProcessed(df):
    name = input("Qual'è il nome del file? ").strip().lower()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    # print(datetime.datetime.now)
    file_name = name +  "_processed" + "_datetime" + timestamp + ".csv"
    print(file_name)
    if __name__ == "__main__" :
        directory_name = "../data/processed/"
    else:
        directory_name = "data/processed/"
    df.to_csv(directory_name + file_name, index = False)

def format_region():
    print("Formattazione dei nomi delle regioni per PowerBI")
    nome_tabella = input("inserisci il nome della tabella da modificare ").strip().lower()
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = f"""
            UPDATE {nome_tabella} 
            SET region = 'Emilia-Romagna'
            WHERE region = 'Emilia Romagna'
            RETURNING *
            """
            cur.execute(sql)
            print("Record con regione aggiornata \n")
            for record in cur:
                print(record)

            sql = f"""
            UPDATE {nome_tabella} 
            SET region = 'Friuli-VeneziaGiulia'
            WHERE region = 'Friuli Venezia Giulia'
            RETURNING *
            """
            cur.execute(sql)
            print("Record con regione aggiornata \n")
            for record in cur:
                print(record)

            sql = f"""
            UPDATE {nome_tabella} 
            SET region = 'Trentino-AltoAdige'
            WHERE region = 'Trentino Alto Adige'
            RETURNING *
            """
            cur.execute(sql)
            print("Record con regione aggiornata \n")
            for record in cur:
                print(record)

            sql = f"""
            UPDATE {nome_tabella} 
            SET region = 'Valled''Aosta'
            WHERE region = 'Valle d''Aosta'
            RETURNING *
            """
            cur.execute(sql)
            print("Record con regione aggiornata \n")
            for record in cur:
                print(record)


#testing
if __name__ == "__main__" :
    #df=readFile()
    #print("Visualizza i dati prima di format cap")
    #df = format_string(df,["region", "city"])
    #print(df)
    #df = format_cap(df)
    #print("Visualizza dati dopo format cap")
    #checkNulls(df,["customer_id"])
    #readFile()
    #saveProcessed([],"pippo")
    format_region()
#volendo si può fare in modo che: ricevere il path del file, copiare il file nella cartella data\raw (aggiungere data
# e ora per rendere il file univoco)



