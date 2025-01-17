import requests
import time
from bs4 import BeautifulSoup
import mysql.connector

class ProductUpdater:
    def __init__(self):
        self.domain = "https://medmedical.ru"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
        }

        # Настройки базы данных
        self.db_config = {
            "host": "localhost",
            "user": "root",
            "password": "",
            "database": "parsers"
        }

    def fetch_product_links(self):
        """Извлекает ссылки продуктов с пустым полем breadcrumbs из базы данных."""
        connection = mysql.connector.connect(**self.db_config)
        cursor = connection.cursor()

        query = "SELECT id, link FROM medmedical_ru WHERE breadcrumbs = ''"
        cursor.execute(query)
        products = cursor.fetchall()

        connection.close()
        return products

    def fetch_page(self, url, stoped=0):
        """Загружает страницу продукта."""
        try:
            response = requests.get(url, headers=self.headers, timeout=7)
            if response.status_code == 200:
                return response.text
            else:
                print(f"Ошибка загрузки страницы: {url}, код: {response.status_code}")
                return None
        except:
            if stoped == 5:
                return False
            time.sleep(5)
            stoped_n = stoped+1
            print('restart: ', stoped_n)
            return self.fetch_page(url, stoped_n)

    def parse_product_page(self, html):
        """Парсит страницу продукта и извлекает description, breadcrumbs и country_of_origin."""
        soup = BeautifulSoup(html, 'html.parser')

        # Извлечение breadcrumbs
        breadcrumbs = " > ".join(
            [bc.text.strip() for bc in soup.select(".breadcrumbs__item-name")]
        )

        # Извлечение description
        description_tag = soup.select_one("#desc .content")
        description = description_tag.get_text("\n", strip=True) if description_tag else ""

        # Извлечение country_of_origin
        country_tag = soup.select_one(".props_list .char_name span", string="Страна производства")
        country_of_origin = country_tag.find_parent("tr").select_one(".char_value span").text.strip() if country_tag else ""

        return description, breadcrumbs, country_of_origin

    def update_database(self, product_id, description, breadcrumbs, country_of_origin):
        """Обновляет данные в базе данных для указанного продукта."""
        connection = mysql.connector.connect(**self.db_config)
        cursor = connection.cursor()

        query = """
        UPDATE medmedical_ru
        SET description = %s, breadcrumbs = %s, country_of_origin = %s
        WHERE id = %s
        """
        cursor.execute(query, (description, breadcrumbs, country_of_origin, product_id))
        connection.commit()
        connection.close()

    def update_products(self):
        """Основной метод для обновления данных продуктов."""
        products = self.fetch_product_links()

        for product_id, link in products:
            print(f"Обрабатывается продукт: {link}")
            html = self.fetch_page(link)
            if html:
                description, breadcrumbs, country_of_origin = self.parse_product_page(html)
                self.update_database(product_id, description, breadcrumbs, country_of_origin)
                print(f"Обновлено: ID={product_id}, breadcrumbs='{breadcrumbs}'")
            # break

# Пример использования
if __name__ == "__main__":
    updater = ProductUpdater()
    updater.update_products()
    print("Обновление завершено.")
