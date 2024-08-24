# crawl_data_books

## install library
    pip install -r requirements.txt

## crawl on tiki
-   Step 1: Crawl categories
    ```bash
        python crawl_categories.py
    ```
-   Step 2: Crawl product_id
    ```bash
        python crawl_product_id.py
    ```
-   Step 3: Crawl books, users, reviews
    ```bash
        python crawl_book.py
    ```