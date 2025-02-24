from sqlalchemy import create_engine, text, inspect
from datetime import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time
import re
import os

class Saver:
    USERNAME = os.getenv('username')
    PASSWORD = os.getenv('password')
    HOST = os.getenv('host')
    DATABASE_NAME = os.getenv('database_name')
    PORT = os.getenv('port')
    def __init__(self):
        self.engine = self.initialize_engine()

    def initialize_engine(self):
        connection_url = f'mysql://{Saver.USERNAME}:{Saver.PASSWORD}@{Saver.HOST}:{Saver.PORT}/{Saver.DATABASE_NAME}'
        engine = create_engine(connection_url)
        return engine

    def get_information(self):
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database
        }
    
    def to_database_seller(self, input_df, table_name):
        input_df['source'] = 'lazada_upload'
        input_columns = input_df.columns.tolist()
        input_df.drop_duplicates(subset=input_columns, inplace=True)
        input_df['created'] = datetime.now()
        input_df['updated'] = datetime.now()
        print('Start getting the index for table ecommerce_seller')
        input_df['country'] = input_df['used_id'].apply(lambda row: row.split('.')[0])
        input_df['marketplace'] = input_df['used_id'].apply(lambda row: row.split('.')[1])
        while True:
            try:
                with self.engine.connect() as connection:
                    marketplaces_queries = text("SELECT used_id, id FROM user_management_marketplace")
                    marketplaces_results = connection.execute(marketplaces_queries)
                    marketplaces_list = marketplaces_results.fetchall()

                    countries_queries = text("SELECT used_id, id FROM user_management_country")
                    countries_results = connection.execute(countries_queries)
                    countries_list = countries_results.fetchall()

                    connection.close()
                    break
            except Exception as e:
                print(f'Error occured getting the data as {e}, retrying ...')
        marketplaces_df = pd.DataFrame(marketplaces_list)
        countries_df = pd.DataFrame(countries_list)
        marketplaces_map = marketplaces_df.set_index('used_id')['id'].to_dict()
        countries_map = countries_df.set_index('used_id')['id'].to_dict()
        input_df['fk_marketplace_id'] = input_df['marketplace'].apply(lambda row: marketplaces_map.get(row))
        input_df['fk_country_id'] = input_df['country'].apply(lambda row: countries_map.get(row))
        input_df = input_df.drop(columns=['country', 'marketplace'])
        inspector = inspect(self.engine)
        table_list = inspector.get_table_names()
        current_list = input_df['used_id'].values.tolist()
        formatted_used_id_list = ", ".join([f"'{used_id}'" for used_id in current_list])
        if table_name in table_list: 
            update_queries = []
            insert_queries = []
            query = f"SELECT used_id, id from ecommerce_seller WHERE used_id IN ({formatted_used_id_list})"
            validate_used_id_df = self.execute_query(query, 'retrieve')
            if len(validate_used_id_df) > 0:
                validate_used_id_list = validate_used_id_df['used_id'].values.tolist()
                existed_df = input_df.loc[input_df['used_id'].isin(validate_used_id_list), :]
                self.existed_df = existed_df
                nonexisted_df = input_df.loc[~input_df['used_id'].isin(validate_used_id_list), :]
                for index, row in existed_df.iterrows():
                    try:
                        seller_center_code = row['seller_center_code']
                    except:
                        seller_center_code = None
                    try:
                        token_refresh_latest = row['token_refresh_latest']
                    except:
                        token_refresh_latest = None
                    try: 
                        slug = row['slug']
                    except: 
                        slug = None
                    try: 
                        url = row['url']
                    except: 
                        url = None
                    try: 
                        seller_type = row['seller_type']
                    except: 
                        seller_type = None
                    seller_center_code_value = 'NULL' if seller_center_code is None else f"'{seller_center_code}'"
                    token_refresh_latest_value = 'NULL' if token_refresh_latest is None else f"'{token_refresh_latest}'"
                    slug_value = 'NULL' if slug is None else f"'{slug}'"
                    url_value = 'NULL' if url is None else f"'{url}'"
                    seller_type_value = 'NULL' if seller_type is None else f"'{seller_type}'"
                    query = f"""
                    UPDATE {table_name}
                    SET 
                        id_marketplace = '{row['id_marketplace']}',
                        seller_center_code = {seller_center_code_value},
                        name = '{row['name']}',
                        slug = {slug_value},
                        url = {url_value},
                        seller_type = {seller_type_value},
                        token_refresh_latest = {token_refresh_latest_value},
                        source = '{row['source']}',
                        updated = '{datetime.now()}',
                        fk_country_id = '{row['fk_country_id']}',
                        fk_marketplace_id = '{row['fk_marketplace_id']}'
                    WHERE used_id = '{row['used_id']}'
                    """
                    update_queries.append(query)
                for index, row in nonexisted_df.iterrows():
                    try:
                        seller_center_code = row['seller_center_code']
                    except:
                        seller_center_code = None
                    try:
                        token_refresh_latest = row['token_refresh_latest']
                    except:
                        token_refresh_latest = None
                    try: 
                        slug = row['slug']
                    except: 
                        slug = None
                    try: 
                        url = row['url']
                    except: 
                        url = None
                    try: 
                        seller_type = row['seller_type']
                    except: 
                        seller_type = None
                    seller_center_code_value = 'NULL' if seller_center_code is None else f"'{seller_center_code}'"
                    token_refresh_latest_value = 'NULL' if token_refresh_latest is None else f"'{token_refresh_latest}'"
                    slug_value = 'NULL' if slug is None else f"'{slug}'"
                    url_value = 'NULL' if url is None else f"'{url}'"
                    seller_type_value = 'NULL' if seller_type is None else f"'{seller_type}'"
                    query = f"""
                    INSERT INTO {table_name} (
                        id_marketplace,
                        seller_center_code,
                        name,
                        slug,
                        url,
                        seller_type,
                        token_refresh_latest,
                        source,
                        created,
                        updated,
                        fk_country_id,
                        fk_marketplace_id,
                        used_id 
                    )
                    VALUES (
                        '{row['id_marketplace']}',
                        {seller_center_code_value},
                        '{row['name']}',
                        {slug_value},
                        {url_value},
                        {seller_type_value},
                        {token_refresh_latest_value},
                        '{row['source']}',
                        '{row['created']}',
                        '{row['updated']}',
                        {row['fk_country_id']},
                        {row['fk_marketplace_id']},
                        '{row['used_id']}'
                    )
                    """
                    insert_queries.append(query)
                combined_update_query = ";\n".join(update_queries)
                combined_insert_query = ";\n".join(insert_queries)
                with ThreadPoolExecutor(max_workers=2) as executors:
                    if len(combined_update_query) > 0:
                        executors.submit(self.execute_query, combined_update_query, 'update')
                    if len(combined_insert_query) > 0:
                        executors.submit(self.execute_query, combined_insert_query, 'update')
                print(f'Saved table {table_name} to database {Saver.DATABASE_NAME}')  
            else:
                input_df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False) 
                print(f'Saved table {table_name} to database {Saver.DATABASE_NAME}')

    def to_database_sku(self, input_df, table_name):
        input_df['source'] = 'lazada_upload'
        input_columns_list = input_df.columns.tolist()
        input_df.drop_duplicates(subset=input_columns_list, inplace=True)
        matching_pattern = r'(VN\.LAZ\.\d+)\.'
        input_df['seller_used_id'] = input_df['used_id'].apply(lambda used_id: re.search(matching_pattern, used_id).group(1))
        unique_seller_list = input_df['seller_used_id'].unique().tolist()
        while True:
            try:
                with self.engine.connect() as connection:
                    formatted_where_seller_list = ", ".join([f"'{used_id}'" for used_id in unique_seller_list])
                    seller_queries = text(f"SELECT used_id, id FROM ecommerce_seller where used_id in ({formatted_where_seller_list})")
                    seller_results = connection.execute(seller_queries)
                    seller_list = seller_results.fetchall()

                    connection.close()
                    break
            except Exception as e:
                print(f'Error occured during the connection as {e}, retrying ...')
        seller_df = pd.DataFrame(seller_list)
        seller_map = seller_df.set_index('used_id')['id'].to_dict()
        input_df['fk_seller_id'] = input_df['seller_used_id'].apply(lambda seller_used_id: seller_map.get(seller_used_id))
        input_df.drop(columns=['seller_used_id'], inplace=True)
        input_df['created'] = datetime.now()
        input_df['updated'] = datetime.now()
        inspector = inspect(self.engine)
        table_list = inspector.get_table_names()
        current_list = input_df['used_id'].values.tolist()
        formatted_used_id_list = ", ".join([f"'{used_id}'" for used_id in current_list])
        if table_name in table_list: 
            update_queries = []
            insert_queries = []
            string_query = f"SELECT used_id, id from ecommerce_sku where used_id IN ({formatted_used_id_list})"
            validate_used_id_df = self.execute_query(string_query, 'retrieve')
            if len(validate_used_id_df) > 0:
                validate_used_id_list = validate_used_id_df['used_id'].values.tolist()
                existed_df = input_df.loc[input_df['used_id'].isin(validate_used_id_list), :]
                nonexisted_df = input_df.loc[~input_df['used_id'].isin(validate_used_id_list), :]
                for index, row in existed_df.iterrows():
                    try:
                        category_raw = row['category_raw']
                    except:
                        category_raw = None
                    try:
                        brand_raw = row['brand_raw']
                    except:
                        brand_raw = None
                    try:
                        barcode = row['barcode']
                    except:
                        barcode = None
                    try:
                        variation_name = row['variation_name']
                    except:
                        variation_name = None
                    try:
                        img_url = row['img_url']
                    except:
                        img_url = None
                    try:
                        url = row['url']
                    except:
                        url = None
                    try:
                        spu_id_marketplace_seller = row['spu_id_marketplace_seller']
                    except:
                        spu_id_marketplace_seller = None
                    category_raw_value = 'NULL' if category_raw is None else f"'{category_raw}'"
                    brand_raw_value = 'NULL' if brand_raw is None else f"'{brand_raw}'"
                    barcode_value = 'NULL' if barcode is None else f"'{barcode}'"
                    variation_name_value = 'NULL' if variation_name is None else f"'{variation_name}'"
                    img_url_value = 'NULL' if img_url is None else f"'{img_url}'"     
                    url_value = 'NULL' if url is None else f"'{url}'"        
                    spu_id_marketplace_seller_value = 'NULL' if spu_id_marketplace_seller is None else f"'{spu_id_marketplace_seller}'"                  
                    query = f"""
                    UPDATE {table_name}
                    SET 
                        category_raw = {category_raw_value},
                        brand_raw = {brand_raw_value},
                        variation_name = {variation_name_value},
                        img_url = {img_url_value},
                        url = {url_value},
                        spu_used_id = '{row['spu_used_id']}',
                        spu_id_marketplace = '{row['spu_id_marketplace']}',
                        spu_id_marketplace_seller = {spu_id_marketplace_seller_value},
                        used_id = '{row['used_id']}',
                        barcode = {barcode_value},
                        name = '{row['name']}',
                        retail_price = {row['retail_price']},
                        selling_price = {row['selling_price']},
                        source = '{row['source']}',
                        updated = '{row['updated']}',
                        fk_seller_id = {row['fk_seller_id']}
                    WHERE used_id = '{row['used_id']}'
                    """
                    update_queries.append(query)
                for index, row in nonexisted_df.iterrows():
                    try:
                        category_raw = row['category_raw']
                    except:
                        category_raw = None
                    try:
                        brand_raw = row['brand_raw']
                    except:
                        brand_raw = None
                    try:
                        barcode = row['barcode']
                    except:
                        barcode = None
                    try:
                        variation_name = row['variation_name']
                    except:
                        variation_name = None
                    try:
                        img_url = row['img_url']
                    except:
                        img_url = None
                    try:
                        url = row['url']
                    except:
                        url = None
                    try:
                        spu_id_marketplace_seller = row['spu_id_marketplace_seller']
                    except:
                        spu_id_marketplace_seller = None
                    category_raw_value = 'NULL' if category_raw is None else f"'{category_raw}'"
                    brand_raw_value = 'NULL' if brand_raw is None else f"'{brand_raw}'"
                    barcode_value = 'NULL' if barcode is None else f"'{barcode}'"
                    variation_name_value = 'NULL' if variation_name is None else f"'{variation_name}'"
                    img_url_value = 'NULL' if img_url is None else f"'{img_url}'"  
                    url_value = 'NULL' if url is None else f"'{url}'"        
                    spu_id_marketplace_seller_value = 'NULL' if spu_id_marketplace_seller is None else f"'{spu_id_marketplace_seller}'"      
                    query = f"""
                    INSERT INTO {table_name} (
                        category_raw,
                        brand_raw,
                        variation_name,
                        img_url,
                        url,
                        spu_used_id,
                        spu_id_marketplace,
                        spu_id_marketplace_seller,
                        used_id,
                        barcode,
                        name,
                        retail_price,
                        selling_price,
                        source,
                        updated,
                        created,
                        fk_seller_id
                    )
                    VALUES (
                        {category_raw_value},
                        {brand_raw_value},
                        {variation_name_value},
                        {img_url_value},
                        {url_value},
                        '{row['spu_used_id']}',
                        '{row['spu_id_marketplace']}',
                        {spu_id_marketplace_seller_value},
                        '{row['used_id']}',
                        '{barcode_value}',
                        '{row['name']}',
                        '{row['retail_price']}',
                        '{row['selling_price']}',
                        '{row['source']}',
                        '{row['updated']}',
                        '{row['created']}',
                        {row['fk_seller_id']}
                    )
                    """
                    insert_queries.append(query)
                with ThreadPoolExecutor(max_workers=4) as executors:
                    if len(update_queries) > 0:
                        for i in range(0, len(update_queries), 4000):
                            batch = ";\n".join(update_queries[i:i + 4000])
                            executors.submit(self.execute_query, batch, 'update')
                    if len(insert_queries) > 0:
                        for i in range(0, len(insert_queries), 4000):
                            batch = ";\n".join(insert_queries[i:i + 4000])
                            executors.submit(self.execute_query, batch, 'update')
                print(f'Saved table {table_name} to database {Saver.DATABASE_NAME}')  
            else:
                input_df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)
                print(f'Saved table {table_name} to database {Saver.DATABASE_NAME}')

    def to_database_sales(self, input_df, table_name):
        inspector = inspect(self.engine)
        table_list = inspector.get_table_names()
        if table_name in table_list:
            input_df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False) 
            print(f'Saved table {table_name} to database {self.database}')  
        else: 
            print(f'Table {table_name} does not existed in database {self.database}')
  
    def execute_query(self,
                      string_query:str,
                      query_type:list[str]=['retrieve', 'update']):
        while True:
            try:
                query = text(string_query)
                with self.engine.connect() as connection:
                    print('Database connection created')
                    if query_type == 'retrieve':
                        matching_pattern = r'from\s+(\w+)(?:\s+)?.*'
                        table_name = re.search(matching_pattern, string_query).group(1)
                        rows = connection.execute(query)
                        results = rows.fetchall()
                        print(f'Fetched {len(results)} rows from table {table_name}')
                        return pd.DataFrame(results)
                    else:
                        matching_pattern = r'(?:INSERT INTO|insert into|UPDATE|update)\s+(.*)(?:\s*)?'
                        table_name = re.search(matching_pattern, string_query).group(1)
                        connection.execute(query)
                        connection.commit()
                        print(f'Updated data for table {table_name}')
                connection.close()
                break
            except Exception as e:
                with open('error.txt', 'w') as file:
                    file.write(str(e))
                print(f'Error occured during the query: {e}')
                time.sleep(10)
                continue

    def close_engine(self):
        self.engine.dispose()
        print(f'Closed engine connection to the database')

if __name__ == '__main__':
    pass

    

    