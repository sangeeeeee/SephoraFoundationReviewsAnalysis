import json
import csv
from progressbar import progressbar
import requests
import math
import pandas as pd


class SephoraDataCollector:
    def __init__(self):
        
        """
        
        initialize object with urls
        
        """
        
        self.brands_url = "https://www.sephora.com/api/catalog/brands?/currentPage=0&pageSize=999999999&content=true&includeRegionsMap=true"
        
        #url to return all products in foundation category
        self.product_url = 'https://www.sephora.com/api/catalog/categories/cat60004/products?currentPage={current_page}&pageSize=999999999&content=true&includeRegionsMap=true'
        
        #url to return all reviews for specific product
        self.review_url = 'https://api.bazaarvoice.com/data/reviews.json?Filter=ProductId%3A{product_id}&Sort=Helpfulness%3Adesc&Limit=100&Offset={offset}&Include=Products%2CComments&Stats=Reviews&passkey=rwbw526r2e7spptqd2qzbkp7&apiversion=5.4'

    def fetch_and_write_data(self, file_path='sephora_review_db.csv'):
        
        """
        
        retrieves reviews and writes to csv
        
        """
        
        #get all product ids to iterate over
        product_ids = self._fetch_all_product_ids()
        
        #column names in csv
        key_fields = ['brand', 'name', 'brand_id', 'brand_image_url', 'product_id', 'product_image_url', 'rating', 'skin_type', 'eye_color', 'skin_concerns', 'incentivized_review',
                      'skin_tone', 'age', 'beauty_insider', 'user_name', 'review_text','price', 'recommended', 'first_submission_date', 'last_submission_date', 'location', 'description']
        
        #create csv file
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=key_fields)
            writer.writeheader()
            
            #iterate over every product id to get all reviews and write to csv
            for p in progressbar(product_ids, prefix='fetching reviews :: '):
                batch_review = self._fetch_all_reviews(p)
                for batch in batch_review:
                    writer.writerow(batch)
                    
    
    def fetch_and_print_tags(self, file_path = 'Sephora_Foundation_Tags.csv'):
        
        """
        
        returns all tags for foundation in reviews
        
        """
        
        product_ids = self._fetch_all_product_ids()
        brands = self._fetch_all_brand_names()
        products = self._fetch_all_prod_names()
        
        df = pd.DataFrame()
        for p in progressbar(product_ids, prefix= 'fetching tags :: '):
            tags = self._fetch_tags(p)
            tag_df = pd.DataFrame(tags)
            tag_df['brand'] = brands[product_ids.index(p)]
            tag_df['product'] = products[product_ids.index(p)]
            tag_df['total_review_count'] = self._fetch(self.review_url.format(**{'offset': 0, 'product_id': p}))['TotalResults']
            df = pd.concat([df, tag_df])
        
        df.to_csv(file_path, index=False)
    
    
    
    def _fetch_all_product_ids(self):
        
        """
        returns all product ids from url
        
        """
        
        #set current_page to 0 in url 
        sub = {'current_page': 0}
        
        #read json
        data = self._fetch(self.product_url.format(**sub))
        
        #extract productId from dictionary
        all_data = [d['productId'] for d in data['products']]
        
        return all_data
    
    def _fetch_all_prices(self):
        
        """
        returns all prices for each product
        
        """
        
        #set current_page to 0 in url 
        sub = {'current_page': 0}
        
        #read json
        data = self._fetch(self.product_url.format(**sub))
        
        #extract prices from dictionary
        prices = [d['currentSku']['listPrice'] for d in data['products']]
        
        return prices
         
        
    
    def _fetch_all_brand_names(self):
        
        """
        
        returns all brand names for each product
        
        """
        
        #set current_page to 0 in url
        sub = {'current_page': 0}
        
        #read json
        data = self._fetch(self.product_url.format(**sub))
        
        #extract brand names from dictionary
        brand_names = [d['brandName'] for d in data['products']]

        return brand_names
    
    def _fetch_all_prod_names(self):
        
        """
        
        returns name of each product in list
        
        """
        
        #set current_page to 0 in url
        sub = {'current_page': 0}
        
        #read json
        data = self._fetch(self.product_url.format(**sub))
        
        #extract product names from dictionary
        prod_names = [d['displayName'] for d in data['products']]

        return prod_names
            

    def _fetch_all_reviews(self, product_id):
        
        """
        
        reads in url and product_id
        
        returns necessary information for every review for product_id
        
        """
        
        
        
        #get list of prices, brands, and product names in order of iteration
        price_dat = self._fetch_all_prices()
        brand_dat = self._fetch_all_brand_names()
        prod_dat = self._fetch_all_prod_names()
        
        #get all product ids for iteration
        prod_ids = self._fetch_all_product_ids()
        
        #read json of first page of review information for product_id
        dat = self._fetch(self.review_url.format(**{'offset': 0, 'product_id': product_id}))
        
        #initialize empty list
        all_reviews = []
        
        #limit: max is 100 - means that only 100 reviews per page
        #offset: how many reviews you want to skip over before seeing limit number of reviews
        #iterate over offset in url by getting total number of reviews from json and dividing by 100
        for offset in range(0,dat['TotalResults'],100):
            
            #get data for specific offset value and product_id
            data = self._fetch(self.review_url.format(**{'offset': offset, 'product_id': product_id}))
            
            brand_img_dat = self._fetch(self.brands_url)
            
            brand_img_dct = dict(zip([i.get('brandId') for i in brand_img_dat['brands']], [i.get('logo') for i in brand_img_dat['brands']]))
            
            #this table remains constant for specific product_id but still needs to be appended to row
            table1 = {'brand': brand_dat[prod_ids.index(product_id)],
                      'name': prod_dat[prod_ids.index(product_id)],
                      'price': price_dat[prod_ids.index(product_id)]}
            
            
            #this table is review specific information
            for review in data['Results']:
                table2 = {'brand_id': self.nget(data, '', 'Includes', 'Products',review.get('ProductId', ''), 'BrandExternalId'), 
                          'brand_image_url': brand_img_dct[self.nget(data, '', 'Includes', 'Products',review.get('ProductId', ''), 'BrandExternalId')],
                          'product_id': product_id,
                          'product_image_url': self.nget(data, '', 'Includes', 'Products', review.get('ProductId', ''), 'ImageUrl'),
                          'user_name': review.get('UserNickname', ''),
                          'rating': review.get('Rating', ''),
                          'review_text': review.get('ReviewText', ''),
                          'skin_type': self.nget(review, '', 'ContextDataValues', 'skinType', 'Value'),
                          'eye_color': self.nget(review, '', 'ContextDataValues', 'eyeColor', 'Value'),
                          'skin_concerns': self.nget(review, '', 'ContextDataValues', 'skinConcerns', 'Value'),
                          'incentivized_review': self.nget(review, '', 'ContextDataValues', 'IncentivizedReview', 'Value'),
                          'skin_tone': self.nget(review, '', 'ContextDataValues', 'skinTone', 'Value'),
                          'age': self.nget(review, '', 'ContextDataValues', 'age', 'Value'),
                          'beauty_insider': self.nget(review, '', 'ContextDataValues', 'beautyInsider', 'Value'),
                          'recommended': self.nget(review, '', 'IsRecommended'),
                          'first_submission_date': self.nget(review, '', 'SubmissionTime'),
                          'last_submission_date': self.nget(review, '', 'LastModificationTime'),
                          'location': self.nget(review, '', 'UserLocation'),
                          'description': self.nget(data, '', 'Includes', 'Products',review.get('ProductId', ''), 'Description')}
                
           
                
                #concatenate and append both to make one row in csv
                all_reviews.append({**table1, **table2})
        
        return all_reviews[:dat['TotalResults']]
    
    def _fetch_tags(self, product_id):
        
        """return all tags and tag counts for product id"""
        
        #get JSON for product Id
        data = self._fetch(self.review_url.format(**{'offset': 0, 'product_id': product_id}))
        
        if 'Products' in data['Includes'].keys():
            return self.nget(data, None, 'Includes', 'Products', list(data['Includes']['Products'].keys())[0], 'ReviewStatistics', 'TagDistribution', 'Pro', 'Values')
        
        else:
            return None
    
    @staticmethod
    def _fetch(url):
        
        """
        
        returns json dictionary from url
        
        """
        r = requests.get(url)
        return json.loads(r.content)

    @staticmethod
    def nget(dct, default=None, *keys):
        
        """
        returns specified values from dictionary
        
        """
        
        for key in keys:
            try:
                dct = dct[key]
            except KeyError:
                return default
        return dct


if __name__ == '__main__':
    s = SephoraDataCollector()
    s.fetch_and_write_data()
    s.fetch_and_print_tags()