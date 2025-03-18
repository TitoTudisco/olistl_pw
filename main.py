import src.customers as customers
import src.products as products

if __name__ == "__main__":
    risposta = "-1"
    while risposta != "0":
        risposta = input("""Che cosa vuoi fare?
        1 = esegui ETL dei clienti
        2= esegui integrazione dati regione e città 
        0= esci dal programma """)
        if risposta == "1":
            df_customer = customers.extract()
            df_customer = customers.transform(df_customer)
            customers.load(df_customer)
        elif risposta == "2" :
            customers.complete_city_region()
        else:
            risposta = "0"

    #df_customers = customers.extract()
    #df_customer =customers.transform(df_customers)
    #customers.load(df_customers)
    #products.extract()
    #products.transform()
    #products.load()
    #customers.complete_city_region()
