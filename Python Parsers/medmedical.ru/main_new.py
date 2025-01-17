import requests
import time
from bs4 import BeautifulSoup
import mysql.connector

class ProductScraper:
    def __init__(self, base_urls):
        self.base_urls = base_urls  # Список базовых URL для парсинга
        self.domain = "https://medmedical.ru"  # Укажите домен вашего сайта
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
            'Accept': '*/*',
            'X-Requested-With': 'XMLHttpRequest',
        }
        self.products = []

        # Настройки базы данных
        self.db_config = {
            "host": "localhost",
            "user": "root",
            "password": "",
            "database": "parsers"
        }

    def fetch_page(self, base_url, page_number, stoped=0):
        """Fetch a single page by its number."""
        url = f"{base_url}?PAGEN_1={page_number}&ajax_get=Y&AJAX_REQUEST=Y&bitrix_include_areas=N"
        try:
            response = requests.get(url, headers=self.headers, timeout=7)
            if response.status_code == 404:
                return None
            return response.text
        except:
            if stoped == 5:
                return False
            time.sleep(5)
            stoped_n = stoped+1
            print('restart: ', stoped_n)
            return self.fetch_page(base_url, page_number, stoped_n)


    def parse_page(self, html):
        """Parse the HTML content of a page to extract product information."""
        soup = BeautifulSoup(html, 'html.parser')
        items = soup.select(".catalog_item_wrapp")
        current_page_products = []

        for item in items:
            product_id = item.get("id", "").split("_")[-1]
            link_tag = item.select_one(".item-title a")
            relative_link = link_tag["href"] if link_tag else ""
            link = self.domain + relative_link  # Полный URL
            name = link_tag.text.strip() if link_tag else ""

            article_tag = item.select_one(".article_block .muted")
            article = article_tag.text.strip().replace("Арт.:", "") if article_tag else ""

            stock_tag = item.select_one(".item-stock .value")
            stock = stock_tag.text.strip() if stock_tag else ""

            brand_tag = item.select_one(".product-brand span")
            brand = brand_tag.text.strip() if brand_tag else ""

            image_tag = item.select_one(".image_wrapper_block img")
            image_link = self.domain + image_tag.get("data-src", "") if image_tag else ""

            full_price = item.find('div', class_='cost').text.strip() if item.find('div', class_='cost') else ""

            # Заглушки для недостающих данных
            description = ""
            breadcrumbs = ""
            country_of_origin = ""

            current_page_products.append({
                "id": product_id,
                "name": name,
                "link": link,
                "article": article,
                "stock": stock,
                "brand": brand,
                "image_link": image_link,
                "description": description,
                "breadcrumbs": breadcrumbs,
                "full_price": full_price,
                "country_of_origin": country_of_origin
            })

        return current_page_products

    def save_to_database(self, products):
        """Save parsed data to MySQL database."""
        connection = mysql.connector.connect(**self.db_config)
        cursor = connection.cursor()

        # Создание таблицы (выполнить один раз)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS medmedical_ru (
                id INT AUTO_INCREMENT PRIMARY KEY,
                product_id VARCHAR(191) UNIQUE,
                name TEXT,
                link TEXT,
                article VARCHAR(255),
                stock TEXT,
                brand TEXT,
                image_link TEXT,
                description TEXT,
                breadcrumbs TEXT,
                full_price TEXT,
                country_of_origin TEXT
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)

        # Сохранение данных с проверкой на дублирование
        query = """
            INSERT IGNORE INTO medmedical_ru (product_id, name, link, article, stock, brand, image_link, description, breadcrumbs, full_price, country_of_origin)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        data = [
            (
                product["id"], product["name"], product["link"], product["article"], product["stock"], 
                product["brand"], product["image_link"], product["description"], 
                product["breadcrumbs"], product["full_price"], product["country_of_origin"]
            )
            for product in products
        ]
        cursor.executemany(query, data)
        connection.commit()
        connection.close()

    def scrape_all_pages(self):
        """Iterate through all pages for all base URLs and collect product data."""
        for base_url in self.base_urls:
            print(f"Начинаем парсинг: {base_url}")
            page_number = 1
            previous_page_products = []
            max_empty_pages = 2
            empty_page_count = 0

            while True:
                print(f"Fetching page {page_number} from {base_url}...")
                html = self.fetch_page(base_url, page_number)
                if html is None:
                    print(f"No more pages to fetch for {base_url}.")
                    break

                current_page_products = self.parse_page(html)

                if not current_page_products or current_page_products == previous_page_products:
                    empty_page_count += 1
                    print(f"No new products on page {page_number}. Empty count: {empty_page_count}")
                else:
                    empty_page_count = 0
                    self.save_to_database(current_page_products)

                if empty_page_count >= max_empty_pages:
                    print(f"Stopping due to {max_empty_pages} consecutive empty pages.")
                    break

                previous_page_products = current_page_products
                page_number += 1

        # # Сохранение в базу данных
        # self.save_to_database()

# Example usage:
if __name__ == "__main__":
    urls = [
        "https://medmedical.ru/catalog/akusherstvo_i_ginekologiya/",
        "https://medmedical.ru/catalog/anesteziya_i_reanimatsiya/",
        "https://medmedical.ru/catalog/bumaga_dlya_meditsinskikh_priborov/",
        "https://medmedical.ru/catalog/gastroenterologiya/",
        "https://medmedical.ru/catalog/geli_sprey_dlya_uzi_i_ekg/",
        "https://medmedical.ru/catalog/dezinfitsiruyushchie_sredstva/",
        "https://medmedical.ru/catalog/igly_shpritsy_/",
        "https://medmedical.ru/catalog/diagnostika/",
        "https://medmedical.ru/catalog/infuzionnaya_terapiya/",
        "https://medmedical.ru/catalog/laboratornye_raskhodnye_materialy/",
        "https://medmedical.ru/catalog/meditsinskaya_mebel/",
        "https://medmedical.ru/catalog/meditsinskie_instrumenty/",
        "https://medmedical.ru/catalog/meditsinskoe_bele/",
        "https://medmedical.ru/catalog/luchevaya_terapiya/",
        "https://medmedical.ru/catalog/oborudovanie_i_aksessuary/",
        "https://medmedical.ru/catalog/obshchaya_terapiya/",
        "https://medmedical.ru/catalog/pakety_dlya_utilizatsii_sterilizatsii_emkost_konteyner/",
        "https://medmedical.ru/catalog/perevyazochnyy_material/",
        "https://medmedical.ru/catalog/proktologiya/",
        "https://medmedical.ru/catalog/spirtovye_etilovye_dezinfitsiruyushchie_salfetki/",
        "https://medmedical.ru/catalog/travmotologiya/",
        "https://medmedical.ru/catalog/urologiya/",
        "https://medmedical.ru/catalog/khirurgiya/",
        "https://medmedical.ru/catalog/otorinolaringologiya/",
        "https://medmedical.ru/catalog/oftalmologiya/",
        "https://medmedical.ru/catalog/ekg_elektrody_i_lenty/",
        "https://medmedical.ru/catalog/endovaskulyarnaya_khirurgiya/",
        "https://medmedical.ru/catalog/endoskopicheskaya_khirurgiya/",
        "https://medmedical.ru/catalog/sredstva_dlya_ukhoda/",
        "https://medmedical.ru/catalog/sredstva_reabilitatsii/",
        "https://medmedical.ru/catalog/stomatologiya/", 
    ]
    scraper = ProductScraper(urls)
    scraper.scrape_all_pages()
    print("Парсинг завершен, данные сохранены в базу.")
