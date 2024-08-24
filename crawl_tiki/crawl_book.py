import requests
import time
import random
import pandas as pd
import csv
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define the stop flag
stop_event = threading.Event()

# Configure logging
logging.basicConfig(filename='error_log.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'vi,fr-FR;q=0.9,fr;q=0.8,en-',
    'Referer': 'https://tiki.vn/nha-sach-tiki/c8322',
    'x-guest-token': 'MXUIz3NfBeCRGcYnPs8SvK6uaD4TrQ5H',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}

params_book = {
    'platform': 'web',
    'spid': '275025209',
    'version': 3
}

params_review = {
    'limit': 20,
    'include': 'comments,contribute_info,attribute_vote_summary',
    'sort': 'score|desc,id|desc,stars|all',
    'page': 1,
    'spid': 262977990,
    'product_id': 262977989,
    'seller_id': 2953
}


# Khởi tạo writer và file
field_books = ["id", "name", "url_path",
               "current_price", "original_price",
               "rating_average", "review_count", 
               "thumbnail_url", "description", 
               "quantity_sold",
               "specifications"]

field_images_books = ["book_id", "base_url"]

field_books_authors = ["book_id", "author_id"]

field_authors = ["id", "name", "slug"]

field_books_categories = ["book_id", "category_id"]

field_stock_item = ["book_id", "quantity"]

field_failed = ["id", "spid"]

field_user = ["id", "name", "email", "phone_number", "password", "address", "role", "created_at", "updated_at"]

field_review = ["id", "book_id", "user_id", "rating", "content", "created_at", "updated_at"]

field_images_review = ["review_id", "image_url"]

# Hàm khởi tạo csv.DictWriter và ghi tiêu đề
def init_csv_writer(file_path, fieldnames):
    file = open(file_path, mode='w', newline='', encoding='utf-8')
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    return writer, file

def create_list_csv_file(number, name, fieldnames):
    list_writer = []
    list_file = []
    for i in range(number):
        writer, file = init_csv_writer(f'data/{name}_{i}.csv', fieldnames)
        list_writer.append(writer)
        list_file.append(file)
    return list_writer, list_file

writers_books, files_books = create_list_csv_file(8, 'books', field_books)
writers_images_books, files_images_books = create_list_csv_file(8, 'images_books', field_images_books)
writers_books_authors, files_books_authors = create_list_csv_file(8, 'books_authors', field_books_authors)
writers_authors, files_authors = create_list_csv_file(8, 'authors', field_authors)
writers_books_categories, files_books_categories = create_list_csv_file(8, 'books_categories', field_books_categories)
writers_stock_item, files_stock_item = create_list_csv_file(8, 'stock_item', field_stock_item)
writers_failed, files_failed = create_list_csv_file(8, 'failed', field_failed)
writes_users, files_users = create_list_csv_file(8, 'users', field_user)
writers_reviews, files_reviews = create_list_csv_file(8, 'reviews', field_review)
writers_images_review, files_images_review = create_list_csv_file(8, 'images_review', field_images_review)

def write_record_to_book_csv(writer, record):
    try:
        specifications = record.get('specifications')
        writer.writerow({
        'id': record.get('id'),
        'name': record.get('name'),
        'url_path': record.get('url_path'),
        'current_price': record.get('price'),
        'original_price': record.get('list_price'),
        'rating_average': 0 if record.get('rating_average') is None else record.get('rating_average'),
        'review_count': 0 if record.get('review_count') is None else record.get('review_count'),
        'thumbnail_url': record.get('thumbnail_url'),
        'quantity_sold':0 if record.get('all_time_quantity_sold') is None else record.get('all_time_quantity_sold'),
        'description': "" if record.get('description') is None else record.get('description'),
        'specifications': None if specifications is None else specifications,
        })
    except Exception as e:
        logging.error(f'Failed to write book_id: {record.get("id")} {e}')

def write_record_to_images_books_csv(writer, record):
    try:
        images = record.get('images')
        if images is None:
            return
        for image in images:
            writer.writerow({
                'book_id': record.get('id'),
                'base_url': image.get('base_url')
            })
    except Exception as e:
        logging.error(f'Failed to write images for book_id: {record.get("id")} {e}')

def write_record_to_books_authors_csv(writer, record):
    try:
        authors = record.get('authors')
        if authors is None:
            return
        for author in authors:
            writer.writerow({
                'book_id': record.get('id'),
                'author_id': author.get('id')
            })
    except Exception as e:
        logging.error(f'Failed to write authors for book_id: {record.get("id")}     {e}')

set_author_id = set()

def write_record_to_authors_csv(writer, record):
    try:
        authors = record.get('authors')
        if authors is None:
            return
        for author in authors:
            if author.get('id') in set_author_id:
                continue
            set_author_id.add(author.get('id'))
            writer.writerow({
                'id': author.get('id'),
                'name': author.get('name'),
                'slug': author.get('slug')
            })
    except Exception as e:
        logging.error(f'Failed to write authors for book_id: {record.get("id")} {e}')

def write_record_to_books_categories_csv(writer, record):
    try:
        breadcrumbs = record.get('breadcrumbs')
        if breadcrumbs is None:
            return
        for breadcrumb in breadcrumbs:
            if breadcrumb.get('category_id') == 8322 or breadcrumb.get('category_id') == 0:
                continue
            writer.writerow({
                'book_id': record.get('id'),
                'category_id': breadcrumb.get('category_id')
            })
    except Exception as e:
        logging.error(f'Failed to write books_categories for book_id: {record.get("id")}    {e}')

def write_record_to_stock_item_csv(writer, record):
    try:
        stock_item = record.get('stock_item')
        writer.writerow({
            'book_id': record.get('id'),
            'quantity': 0 if stock_item is None else stock_item.get('qty')
        })
    except Exception as e:
        logging.error(f'Failed to write stock_item for book_id: {record.get("id")} {e}')


def write_record_to_user_csv(writer, list_record):
    for record in list_record:
        user = record.get('created_by')
        try:
            writer.writerow({
                'id': user.get('id'),
                'name': user.get('name'),
                'email': f'{user.get('id')}@gmail.com',
                'phone_number': f'0987654321',
                'password': '12345678',
                'address': "viet nam",
                'role': 'user',
                'created_at': user.get('created_time'),
                'updated_at': user.get('created_time')
            })
        except Exception as e:
            logging.error(f'Failed to write user_id: {record.get("id")} {e}')

def write_record_to_review_csv(writer, list_record):
    if list_record is None:
        return
    for record in list_record:
        try:
            writer.writerow({
                'id': record.get('id'),
                'book_id': record.get('product_id'),
                'user_id': record.get('customer_id'),
                'rating': record.get('rating'),
                'content': record.get('content'),
                'created_at': record.get('created_by').get('created_time'),
                'updated_at': record.get('created_by').get('created_time')
            })
        except Exception as e:
            logging.error(f'Failed to write review_id: {record.get("id")} {e}')
            
def write_record_to_images_review_csv(writer, list_record):
    for record in list_record:
        images = record.get('images')
        if images is None:
            continue
        for image in images:
            try:
                writer.writerow({
                    'review_id': record.get('id'),
                    'image_url': image.get('full_path')
                })
            except Exception as e:
                logging.error(f'Failed to write images for review_id: {record.get("id")} {e}')
    

def crawl_users_reviews(product_id, spid, seller_id, headers, params, writers_users, writers_reviews, writers_images_review):
    params['spid'] = spid
    params['product_id'] = product_id
    params['seller_id'] = seller_id
    
    url = f'https://tiki.vn/api/v2/reviews'
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            try:
                data = response.json().get('data')
                write_record_to_user_csv(writers_users, data)
                write_record_to_review_csv(writers_reviews, data)
                write_record_to_images_review_csv(writers_images_review, data)
            except Exception as e:
                writers_failed[0].writerow({'id': product_id, 'spid': spid})
                logging.error(f'Failed to fetch data for product_id: {product_id} {e}')
        else:
            writers_failed[0].writerow({'id': product_id, 'spid': spid})
            logging.error(f'Failed to fetch data for product_id: {product_id} {response.status_code}')
    except Exception as e:
        writers_failed[0].writerow({'id': product_id, 'spid': spid})
        logging.error(f'Failed to fetch data for product_id: {product_id} {e}')
    
# Define the function to handle each request
def process_row(row, headers, params, number):
    product_id = row['id']
    params['spid'] = row['spid']
    url = f'https://tiki.vn/api/v2/products/{product_id}'
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            try:
                data = response.json()
                write_record_to_book_csv(writers_books[number], data)
                write_record_to_images_books_csv(writers_images_books[number], data)
                write_record_to_books_authors_csv(writers_books_authors[number], data)
                write_record_to_authors_csv(writers_authors[number], data)
                write_record_to_books_categories_csv(writers_books_categories[number], data)
                write_record_to_stock_item_csv(writers_stock_item[number], data)
                crawl_users_reviews(product_id, 
                                    row['spid'], 
                                    data.get('current_seller').get('id'), 
                                    headers, 
                                    params_review, 
                                    writes_users[number], 
                                    writers_reviews[number], 
                                    writers_images_review[number])

                print(f'Fetched data for product_id: {product_id}')
            except Exception as e:
                writers_failed[number].writerow({'id': product_id, 'spid': row['spid']})
                # logging.error(f'Failed to fetch data for product_id: {product_id} {e}')
        else:
            writers_failed[number].writerow({'id': product_id, 'spid': row['spid']})
            logging.error(f'Failed to fetch data for product_id: {product_id} {response.status_code}')
        
        time.sleep(random.uniform(0.2, 0.4))
    except Exception as e:
        writers_failed[number].writerow({'id': product_id, 'spid': row['spid']})
        logging.error(f'Failed to fetch data for product_id: {product_id} {e}')
        
# Open the CSV file and read the rows
with open('data/product_id.csv', mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    rows = list(reader)
    
# for row in rows:
#     process_row(row, headers, params)

batch_size = 8

with ThreadPoolExecutor(max_workers=8) as executor:
    try:
        for i in range(112312, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            print(f'Processing batch {i} - {i + batch_size}')
            futures = [executor.submit(process_row, row, headers, params_book, number) for number, row in enumerate(batch)]
            for future in as_completed(futures):
                if stop_event.is_set():
                    break
                try:
                    future.result()
                except Exception as e:
                    logging.error(f'Error processing row: {e}')
            if stop_event.is_set():
                break
    except KeyboardInterrupt:
        stop_event.set()
        logging.info('Stopping threads...')
        # Wait for all futures to complete
        for future in futures:
            future.cancel()

print('All threads have been stopped.')

# process_row(rows[60197], headers, params_book, 0)

def close_files(files):
    for file in files:
        file.close()

def close_writers(writers):
    for writer in writers:
        writer.close()

close_files(files_books)
close_files(files_images_books)
close_files(files_books_authors)
close_files(files_authors)
close_files(files_books_categories)
close_files(files_stock_item)
close_files(files_failed)
close_files(files_users)
close_files(files_reviews)
close_files(files_images_review)

close_writers(writers_books)
close_writers(writers_images_books)
close_writers(writers_books_authors)
close_writers(writers_authors)
close_writers(writers_books_categories)
close_writers(writers_stock_item)
close_writers(writers_failed)
close_writers(writes_users)
close_writers(writers_reviews)
close_writers(writers_images_review)
