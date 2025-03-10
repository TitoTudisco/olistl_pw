## TITOLO
# Esempio
Testo normale 

* PIPPO

## TABEllE
**customers**
- pk_customer VARCHAR
- region VARCHAR
- city VARCHAR
- cap (VARCHAR  SI PUò ELIMINAREformattare con gli 00 davanti)

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
