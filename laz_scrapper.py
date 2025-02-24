from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
from urllib.parse import quote
from curl_cffi import requests
from saver import Saver

class Scrapper:
    MAX_RETRIES = 1
    COOKIES_PATH = '/home/ubuntu/cookies.json'
    def __init__(self, 
                 search_word: str,
                 scrapped_pages: int):
        self.scrapped_pages = scrapped_pages
        self.search_word = search_word
        self.encoded_query_string = self.get_encoded_string()
        self.seller_result_list = []
        self.skus_result_list = []
        self.Saver = Saver()

    def get_encoded_string(self):
        encoded_query_string = quote(self.search_word)
        return encoded_query_string
    
    def get_product(self):
        for page in range(1, self.scrapped_pages+1):
            print(f'Start scrapping the search word {self.search_word}, current page is {page}')
            current_retry = 0
            while current_retry < Scrapper.MAX_RETRIES:
                try:
                    scrapped_url = f"https://www.lazada.vn/catalog/?ajax=true&isFirstRequest=true&page={page}&q={self.encoded_query_string}"
                    response = requests.request("GET", impersonate="chrome100", url=scrapped_url, cookies=Scrapper.COOKIES_PATH)
                    print(f'Made requests to {scrapped_url}, the status code is at {response.status_code}')
                    list_items = response.json().get('mods').get('listItems')
                    for product_node in list_items:
                        seller_id = product_node['sellerId']
                        seller_name = product_node['sellerName']
                        seller_used_id = '.'.join(['VN.LAZ', seller_id])
                        seller_json_node = {
                            'used_id': seller_used_id,
                            'id_marketplace': seller_id,
                            'name': seller_name,
                        }
                        self.seller_result_list.append(seller_json_node)
                        spu_id_marketplace = product_node['itemId']
                        sku_id_marketplace = product_node['skuId']
                        spu_used_id = '.'.join(['VN.LAZ', seller_id, spu_id_marketplace])
                        sku_used_id = '.'.join(['VN.LAZ', seller_id, spu_id_marketplace, sku_id_marketplace])
                        product_name = product_node['name']
                        product_image = product_node['image']
                        marketplace_code = product_node['cheapest_sku']
                        try:
                            product_retail_price = product_node['originalPrice']
                        except:
                            product_retail_price = product_node['price']
                        product_selling_price = product_node['price']
                        product_price_show = product_node['priceShow']
                        product_rating_score = product_node['ratingScore']
                        product_review = product_node['review']
                        product_url = product_node['itemUrl'][2:]
                        try:
                            product_units_sold = product_node['itemSoldCntShow']
                        except: 
                            product_units_sold = '0 sold'
                        json_node = {
                            'category_raw': self.search_word,
                            'spu_id_marketplace': spu_id_marketplace,
                            'sku_id_marketplace': sku_id_marketplace,
                            'spu_used_id': spu_used_id,
                            'used_id': sku_used_id, 
                            'marketplace_code': marketplace_code, 
                            'name': product_name,
                            'img_url': product_image, 
                            'url': product_url, 
                            'retail_price': product_retail_price,
                            'selling_price': product_selling_price, 
                            'content_videos': product_price_show,
                            'content_imgs': product_units_sold,
                            'highlights': product_review,
                            'old_content': product_rating_score,
                        }
                        self.skus_result_list.append(json_node)
                    break
                except Exception as e:
                    print(f'Error occured as {e}, retrying ...')
                    current_retry += 1
        seller_df = pd.DataFrame(self.seller_result_list)
        skus_df = pd.DataFrame(self.skus_result_list)
        self.Saver.to_database_seller(seller_df, 'ecommerce_seller')
        self.Saver.to_database_sku(skus_df, 'ecommerce_sku')

if __name__ == "__main__":
    scrapper = Scrapper('Chuột máy tính', 5)
    scrapper.get_product()

    
