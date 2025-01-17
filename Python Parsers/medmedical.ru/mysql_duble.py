import mysql.connector

# Настройки базы данных
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "parsers"
}

def remove_duplicates():
    try:
        # Подключение к базе данных
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # SQL-запрос для удаления дубликатов
        query = """
        DELETE t1
        FROM medmedical_ru t1
        JOIN medmedical_ru t2
        ON t1.product_id = t2.product_id
           AND t1.id > t2.id;
        """
        cursor.execute(query)
        connection.commit()

        print(f"Дубликаты удалены. Затронуто строк: {cursor.rowcount}")

    except mysql.connector.Error as err:
        print(f"Ошибка: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Вызов функции удаления дубликатов
remove_duplicates()


# import mysql.connector

# # Настройки базы данных
# db_config = {
#     "host": "localhost",
#     "user": "root",
#     "password": "",
#     "database": "parsers"
# }

# def check_duplicates():
#     try:
#         # Подключение к базе данных
#         connection = mysql.connector.connect(**db_config)
#         cursor = connection.cursor()

#         # SQL-запрос для поиска дубликатов по полю product_id
#         query = """
#         SELECT product_id, COUNT(*) as count
#         FROM medmedical_ru
#         GROUP BY product_id
#         HAVING count > 1
#         """
#         cursor.execute(query)
#         duplicates = cursor.fetchall()

#         if duplicates:
#             print("Найдены дубликаты:")
#             for duplicate in duplicates:
#                 print(f"product_id: {duplicate[0]}, Количество: {duplicate[1]}")
#         else:
#             print("Дубликаты не найдены.")

#     except mysql.connector.Error as err:
#         print(f"Ошибка: {err}")
#     finally:
#         if connection.is_connected():
#             cursor.close()
#             connection.close()

# # Вызов функции проверки дубликатов
# check_duplicates()
