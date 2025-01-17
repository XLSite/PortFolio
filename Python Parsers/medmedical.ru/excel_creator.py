import mysql.connector
import xlsxwriter

class ProductExporter:
    def __init__(self):
        # Настройки базы данных
        self.db_config = {
            "host": "localhost",
            "user": "root",
            "password": "",
            "database": "parsers"
        }
        self.output_file = "products.xlsx"  # Имя выходного файла Excel

    def fetch_products(self):
        """Извлекает данные продуктов из базы данных."""
        connection = mysql.connector.connect(**self.db_config)
        cursor = connection.cursor()

        query = """
        SELECT breadcrumbs, name, article, country_of_origin, brand, stock, full_price, image_link, description
        FROM medmedical_ru
        """
        cursor.execute(query)
        products = cursor.fetchall()

        connection.close()
        return products

    def export_to_excel(self, products):
        """Экспортирует данные продуктов в Excel-файл."""
        workbook = xlsxwriter.Workbook(self.output_file)
        worksheet = workbook.add_worksheet("Products")

        # Заголовки
        headers = [
            "Breadcrumbs", "Название", "Артикул", "Страна производства", "Бренд",
            "Наличие на складе", "Цена (полностью)", "Ссылка на полное изображение", "Описание"
        ]

        # Записываем заголовки в первую строку
        for col_num, header in enumerate(headers):
            worksheet.write(0, col_num, header)

        # Записываем данные продуктов, начиная со второй строки
        for row_num, product in enumerate(products, start=1):
            for col_num, value in enumerate(product):
                worksheet.write(row_num, col_num, value)

        workbook.close()
        print(f"Данные успешно сохранены в файл: {self.output_file}")

    def export(self):
        """Основной метод для экспорта продуктов в Excel."""
        products = self.fetch_products()
        self.export_to_excel(products)

# Пример использования
if __name__ == "__main__":
    exporter = ProductExporter()
    exporter.export()
