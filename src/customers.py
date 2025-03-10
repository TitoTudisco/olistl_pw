# 3 metodi ETL per CUSTOMER

def extract():
    print ("questo è il metodo EXTRACT dei customers")

def transform():
    print ("questo è il metodo TRANSFORM dei customers")

def load():
    print("questo è il metodo LOAD dei customers")

def main():
    print("questo è il metodo MAIN dei customers")
    extract()
    transform()
    load()


#per usare questo file come fosse un modulo
#I metodi definiti sopra vanno importati
if __name__ == "__main__": # indica ciò che viene eseguito quando eseguo direttamente
    main()