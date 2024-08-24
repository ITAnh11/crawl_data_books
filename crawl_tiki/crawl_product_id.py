import requests
import time
import random
import pandas as pd
import csv

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'vi,fr-FR;q=0.9,fr;q=0.8,en-',
    'Referer': 'https://tiki.vn/nha-sach-tiki/c8322',
    'x-guest-token': 'MXUIz3NfBeCRGcYnPs8SvK6uaD4TrQ5H',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}

params = {
    'limit': '40',
    'include': 'advertisement',
    'aggregations': '2',
    'version': 'home-persionalized',
    'trackity_id': '91e6ed3e-0f2b-f181-4074-099c24c7fc69',
    'category': '316',
    'page': '1',
    'urlKey': 'sach-truyen-tieng-viet'
}

# Hàm khởi tạo csv.DictWriter và ghi tiêu đề
def init_csv_writer(file_path, fieldnames):
    file = open(file_path, mode='w', newline='', encoding='utf-8')
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    return writer, file

# Hàm ghi từng bản ghi vào file CSV
def write_record_to_csv(writer, record):
    writer.writerow({
        'id': record.get('id'),
        'spid': record.get('impression_info')[0].get('metadata').get('spid'),
    })

# Khởi tạo writer và file
fieldnames = ['id', 'spid']
writer, file = init_csv_writer('data/product_id.csv', fieldnames)
set_product_id = set()

with open('data/categories.csv', mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
            
    for row in reader:
        if row['is_leaf'] == 'False':
            continue
        params['category'] = row['id']
        params['urlKey'] = row['url_key']
        i = 0
        
        while True:
            i += 1
            params['page'] = i
            no_data = False
            print('requesting page', i)
            response = requests.get('https://tiki.vn/api/personalish/v1/blocks/listings', headers=headers, params=params)
            if response.status_code == 200:
                print('request success!!!')
                records = response.json().get('data')
                if len(records) == 0:
                    break
                for record in records:
                    if record.get('id') in set_product_id:
                        continue
                    set_product_id.add(record.get('id'))
                    write_record_to_csv(writer, record)
            time.sleep(1)
