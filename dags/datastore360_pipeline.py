from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd
import numpy as np 

RAW_FILE = "/opt/airflow/data/raw/store-data-6aa6d7a3f171f140353680(1).csv"
PROCESSED_FILE = "/opt/airflow/data/processed/datastore360_clean.csv"




@dag(
    dag_id="datastore360_pipeline",
    start_date=datetime(2026, 9, 20),
    schedule=None,
    catchup=False,
)



def datastore360_pipeline()  :
    @task
    def extractj_data_set():

        dataset = pd.read_csv(RAW_FILE)
        
        output = "/opt/airflow/data/processed/01_extracted.csv"

        dataset.to_csv(output, index=False)
    
        return output
    
    
    
    @task
    def make_inputs_with_one_format(path) : 
        dataset = pd.read_csv(path)
        
        columns = ["City", "Country", "State", "Region", "Category", "Sub-Category"]
        
        for column in columns :   
            
            dataset[column] = dataset[column].str.strip().str.title()
         
         
        output = "/opt/airflow/data/processed/02_extracted.csv"
        
        dataset.to_csv(output, index=False)
            
        return output  
    
    
    
    @task
    def remove_duplicates(path) : 
        
        dataset = pd.read_csv(path)
        
        duplicates = dataset.duplicated()
        ids = [id for id , boolean in enumerate(duplicates) if boolean == True]
        dataset.drop(ids, inplace=True)
        dataset.reset_index(drop=True, inplace=True)
        
        output = "/opt/airflow/data/processed/03_extracted.csv"
        
        dataset.to_csv(output, index=False)
            
        return output   
        
    @task 
    def fix_dates(path) : 
        
        dataset = pd.read_csv(path)
        
        dataset["Order Date"] = pd.to_datetime(
            dataset["Order Date"],
            format="mixed"
        )

        dataset["Ship Date"] = pd.to_datetime(
            dataset["Ship Date"],
            format="mixed"
        )
        
        
        dataset["shipping_days"] = (dataset["Ship Date"] - dataset["Order Date"]).dt.days

        
        dataset["Ship Mode"] = dataset["Ship Mode"].fillna(
            dataset["Ship Mode"].mode()[0]
        )
        
        valid = dataset["shipping_days"] >= 0
        
        
        
        mode_days = (
            dataset[valid]
            .groupby("Ship Mode")["shipping_days"]
            .agg(lambda x: x.mode().iloc[0])
        )
        
        condition = (
            dataset["Ship Date"].isna() |
            (dataset["Ship Date"] < dataset["Order Date"])
            )
        
        
        dataset.loc[condition, "Ship Date"] = (
            dataset.loc[condition, "Order Date"]
            + pd.to_timedelta(
                dataset.loc[condition, "Ship Mode"].map(mode_days),
                unit="D"
            )
        )
        
        
        dataset["shipping_days"] = ( dataset["Ship Date"] - dataset["Order Date"] ).dt.days
        
        
        output = "/opt/airflow/data/processed/04_extracted.csv"
        
        dataset.to_csv(output, index=False)
            
        return output  
    @task 
    def update_nan_customer_name(path) : 
        
        dataset = pd.read_csv(path)
        
        dataset["Customer Name"] = dataset["Customer Name"].str.strip().str.title()
     
        
        dataset["Customer Name"] = dataset["Customer Name"].fillna(
            dataset.groupby("Customer ID")["Customer Name"]
            .transform("first")
        )
        
        dataset["Customer Name"] = dataset["Customer Name"].fillna(
             dataset["Customer ID"]
        )  
        
        output = "/opt/airflow/data/processed/05_extracted.csv"
        
        dataset.to_csv(output, index=False)
            
        return output  
        
    
    @task 
    def update_nan_postal_code(path) : 
        
        dataset = pd.read_csv(path)
        
        city_postal_codes = dataset.groupby("City")["Postal Code"].agg(
            lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan
        )
        
        state_postal_codes = dataset.groupby("State")["Postal Code"].agg(
            lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan
        )
        
        dataset["Postal Code"] = dataset["Postal Code"].fillna(
            dataset["City"].map(city_postal_codes)
        )
        
        
        dataset["Postal Code"] = dataset["Postal Code"].fillna(
            dataset["State"].map(state_postal_codes)
        )   
        
        output = "/opt/airflow/data/processed/06_extracted.csv"
        
        dataset.to_csv(output, index=False)
            
        return output  
        
        
        
    @task 
    def update_quantity(path) :
        
        dataset = pd.read_csv(path)
        
        product_modes = dataset.groupby("Product ID")["Quantity"].agg( lambda x: x.mode()[0] if not x.mode().empty else x)

        product_modes = product_modes.loc[product_modes.notna()] 

        
        dataset["Quantity"] = dataset["Quantity"].fillna(
            dataset["Product ID"].map(product_modes)
        )
        
        sub_categories_modes = dataset.groupby("Sub-Category")["Quantity"].agg( lambda x: x.mode())

        
        dataset["Quantity"] = dataset["Quantity"].fillna(
            dataset["Sub-Category"].map(sub_categories_modes)
        )
        
        
        mask = dataset["Quantity"].apply(
            lambda x: isinstance(x, np.ndarray) or (isinstance(x, (int, float, np.number)) and x < 0)
        )
        
        dataset.drop(dataset[mask].index, inplace=True)

        
        output = "/opt/airflow/data/processed/07_extracted.csv"
        
        dataset.to_csv(output, index=False)
            
        return output  
        
    
    @task 
    def update_and_clean_sales(path) : 
         
        dataset = pd.read_csv(path)
        
        dataset['Row ID'] = np.arange(0, len(dataset), 1)
        
      
        dataset["Unit_price"] = dataset["Sales"] / (dataset["Quantity"] * (1 - dataset["Discount"]))



        product_names = dataset.loc[dataset["Sales"] == 1131924.0 , "Product Name"]
        
        
       
        
        for column in product_names : 
            
            sales_of_column  = dataset[(dataset["Product Name"] == column) & (dataset["Sales"] != 1131924.0)]["Unit_price"]
             
            dataset.loc[(dataset["Product Name"] == column) & (dataset["Sales"] == 1131924.0), "Unit_price"] = sales_of_column.mean()
            
            row = dataset.loc[(dataset["Product Name"] == column) & (dataset["Sales"] == 1131924.0)]
            
            dataset.loc[(dataset["Product Name"] == column) & (dataset["Sales"] == 1131924.0) , "Sales"] = row["Unit_price"] * row["Quantity"] * (1 - row["Discount"])
            
             
        sales_nan_rows_names = dataset.loc[dataset["Sales"].isna(), "Product Name"] 
        
        
        for column in sales_nan_rows_names : 
             
            sales_of_column  = dataset[(dataset["Product Name"] == column) & (pd.notna(dataset["Sales"]))][["Unit_price", "Sales"]]
            
            
            dataset.loc[(dataset["Product Name"] == column) & (pd.isna(dataset["Unit_price"])), "Unit_price"] = sales_of_column["Unit_price"].mean()
            
            
            dataset.loc[(dataset["Product Name"] == column) & (pd.isna(dataset["Sales"])), "Sales"] = sales_of_column["Sales"].mean()
                
          
            
        unique_products_with_nan_values = dataset.loc[dataset["Sales"].isna(), :]


        dataset.drop(index=unique_products_with_nan_values.index, inplace=True)


         
        output = "/opt/airflow/data/processed/08_extracted.csv"
        
        dataset.to_csv(output, index=False)
            
        return output  
    @task 
    def hash_sensitive_data(path) : 
        
        import hashlib
        
        
        dataset = pd.read_csv(path)
        
        for Customer_Name in dataset["Customer Name"] : 
          hash_object = hashlib.sha256(Customer_Name.encode()).hexdigest()
          dataset.loc[dataset["Customer Name"] == Customer_Name, "Customer Name"] = hash_object
         
        
        output = "/opt/airflow/data/processed/09_extracted.csv"
        
        dataset.to_csv(output, index=False)
            
        return output  
    
    @task 
    def save_to_procced_data(path) : 
        dataset = pd.read_csv(path)
        
        dataset.to_csv(PROCESSED_FILE, index=False)
        
        return PROCESSED_FILE
    
    
    
    step1 = extractj_data_set()
    step2 = make_inputs_with_one_format(step1)
    step3 = remove_duplicates(step2)
    step4 = fix_dates(step3)
    step5 = update_nan_customer_name(step4)
    step6 = update_nan_postal_code(step5)
    step7 = update_quantity(step6)
    step8 = update_and_clean_sales(step7)
    step9 = hash_sensitive_data(step8)
    step10 = save_to_procced_data(step9)
    
    


datastore360_pipeline()

