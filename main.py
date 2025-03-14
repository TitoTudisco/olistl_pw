import src.customers as customers
import src.products as products

if __name__ == "__main__":
    df_customers= customers.extract()
    df_customer=customers.transform(df_customers)
    customers.load(df_customers)
    #products.extract()
    #products.transform()
    #products.load()
    customers.complete_city_region()
