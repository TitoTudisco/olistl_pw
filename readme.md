

## TABEllE
**customers**
- pk_customer VARCHAR
- region VARCHAR
- city VARCHAR
- cap (VARCHAR  SI PUò ELIMINARE / formattare con gli 00 davanti)
- 

**categories**
- pk_category SERIAL
- name VARCHAR _(in inglese)_
- 

**products**
- pk_product VARCHAR
- fk_categoryINTEGER
- name_length INTEGER (errore ortografico)
- description_length INTEGER
- imgs_length INTEGER

**orders**
- pk_order VARCHAR
- fk_customer VARCHAR
- status VARCHAR
- purchase_timestamp TIMESTAMP
- delivered_timestamp TIMESTAMP
- estimated_date DATE

**sellers**(al 10/3 non lo abbiamo)
- pk_seller VARCHAR
- region VARCHAR
- 
**orders_products**
- pk_order_product SERIAL
- fk_order VARCHAR
- fk_product VARCHAR
- fk_seller VARCHAR
- price FLOAT
- freight FLOAT (=costo di trasporto)

## To do opzionale
in common py
- copia del file in input alla cartella raw
- (fare in modo che il nome del file sia univoco)
- prima di fare il load creare database da Python
- Controllo di validità input 
- Upper per si/NO  
- Controllo user password eliminazione tabella 
- Aggiunta colonna inserimento (data inserimento database)
