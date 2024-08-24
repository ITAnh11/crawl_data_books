import requests
import time
import random
import pandas as pd
import csv

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
        'is_leaf': record.get('is_leaf'),
        'name': record.get('name'),
        'url_key': record.get('url_key'),
        'parent_id': record.get('parent_id'),
        'level': record.get('level'),
        'meta_description': record.get('meta_description'),
        'meta_title': record.get('meta_title'),
    })
    
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'vi,fr-FR;q=0.9,fr;q=0.8,en-',
    'Referer': 'https://tiki.vn/nha-sach-tiki/c8322',
    'x-guest-token': 'MXUIz3NfBeCRGcYnPs8SvK6uaD4TrQ5H',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}

params_en = {
    'include': 'children',
    'parent_id': '320'
}

params_vn = {
    'include': 'children',
    'parent_id': '316'
}

# Khởi tạo writer và file
fieldnames = ['id', 'is_leaf', 'name', 'url_key', 'parent_id', 'level', 'meta_description', 'meta_title']
writer, file = init_csv_writer('categories.csv', fieldnames)

write_record_to_csv(writer, {
    'id': 320, 
    'is_leaf': False, 
    'name': 'Sách Tiếng Anh', 
    'url_key': 'sach-tieng-anh', 
    'parent_id': 0, 
    'level': 3, 
    'meta_description': '', 
    'meta_title': ''
    })
write_record_to_csv(writer, {
    'id': 316, 
    'is_leaf': False, 
    'name': 'Sách Truyện Tiếng Việt', 
    'url_key': 'sach-truyen-tieng-viet', 
    'parent_id': 0, 
    'level': 3, 
    'meta_description': '', 
    'meta_title': ''
    })

def dsf(params):
    response = requests.get('https://tiki.vn/api/v2/categories', headers=headers, params=params)
    if response.status_code == 200:
        print('request success!!!')
        records = response.json().get('data')
        if len(records) == 0:
            return
        for record in records:
            write_record_to_csv(writer, record)
            print(record.get('name'))
            dsf({'include': 'children', 'parent_id': record.get('id')})
    time.sleep(1)

dsf(params_en)
dsf(params_vn)

file.close()