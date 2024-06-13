import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text
import os
from config import settings  # Импортируем настройки

fake = Faker('ru_RU')

connection_string = f'postgresql://{settings.db_user}:{settings.db_pass.get_secret_value()}@{settings.db_host}:{settings.db_port}/{settings.db_name}'
engine = create_engine(connection_string, echo=True)  # Echo для отображения SQL запросов


def save_df_to_csv(df, filename):
    if os.path.exists(filename):
        os.remove(filename)
    df.to_csv(filename, index=False)


def recreate_table(table_name, create_table_sql, con):
    with con.connect() as connection:
        try:
            connection.execute(text(f'DROP TABLE IF EXISTS {table_name} CASCADE;'))
            connection.execute(text(create_table_sql))
            connection.execute(text('COMMIT;'))
            print(f"Table {table_name} created successfully.")
        except Exception as e:
            connection.execute(text('ROLLBACK;'))
            print(f"Error creating table {table_name}: {e}")


def save_df_to_sql(df, table_name, con):
    df.to_sql(table_name, con=con, if_exists='append', index=False)


def generate_data():
    tables = {
        'categories': {
            'data': {'id': [1, 2, 3, 4, 5],
                     'name': ['Техника для кухни', 'Смартфоны', 'Телевизоры и цифровое ТВ', 'Компьютеры',
                              'Аудиотехника']},
            'create_sql': """
                CREATE TABLE categories (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL
                );
            """
        },
        'products': {
            'data': {'id': list(range(1, 51)),
                     'name': ['Холодильник', 'Микроволновка', 'Посудомоечная машина', 'Мультиварка', 'Чайник',
                              'Блендер', 'Миксер', 'Соковыжималка', 'Тостер', 'Кофемашина', 'iPhone 12',
                              'Samsung Galaxy S21', 'Xiaomi Mi 11', 'Huawei P40', 'OnePlus 9', 'Google Pixel 5',
                              'Sony Xperia 1', 'Nokia 8.3', 'Oppo Find X3', 'Realme GT', 'Samsung QLED', 'LG OLED',
                              'Sony Bravia', 'Philips Ambilight', 'Panasonic Viera', 'Sharp Aquos', 'Toshiba Regza',
                              'Vizio SmartCast', 'Hisense H8G', 'TCL 6-Series', 'MacBook Pro', 'Dell XPS', 'HP Spectre',
                              'Lenovo ThinkPad', 'Asus ZenBook', 'Microsoft Surface', 'Acer Swift', 'Razer Blade',
                              'MSI Prestige', 'Huawei MateBook', 'JBL Speaker', 'Sony Headphones', 'Bose Sound System',
                              'Yamaha Receiver', 'Pioneer Car Audio', 'Beats by Dre', 'Sennheiser HD', 'Marshall Acton',
                              'Bowers & Wilkins', 'Klipsch R-41M'],
                     'category_id': [i for i in range(1, 6) for _ in range(10)]},
            'create_sql': """
                CREATE TABLE products (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    category_id INT NOT NULL,
                    FOREIGN KEY (category_id) REFERENCES categories(id)
                );
            """
        },
        'managers': {
            'data': {'id': list(range(1, 16)),
                     'full_name': [fake.name() for _ in range(15)],
                     'birth_date': [fake.date_of_birth(minimum_age=25, maximum_age=60).isoformat() for _ in range(15)]},
            'create_sql': """
                CREATE TABLE managers (
                    id SERIAL PRIMARY KEY,
                    full_name VARCHAR(255) NOT NULL,
                    birth_date DATE NOT NULL
                );
            """
        },
        'stores': {
            'data': {'id': list(range(1, 6)),
                     'name': [fake.company() for _ in range(5)],
                     'address': [
                         f"{fake.country()}, {fake.city()}, {fake.street_name()}, д. {fake.building_number()}, эт. {fake.random_int(min=1, max=10)}, {fake.postcode()}"
                         for _ in range(5)]},
            'create_sql': """
                CREATE TABLE stores (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    address VARCHAR(255) NOT NULL
                );
            """
        },
        'suppliers': {
            'data': {'id': list(range(1, 16)),
                     'name': [fake.company() for _ in range(15)],
                     'address': [fake.address().replace('\n', ', ') for _ in range(15)]},
            'create_sql': """
                CREATE TABLE suppliers (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    address VARCHAR(255) NOT NULL
                );
            """
        },
        'currencies': {
            'data': {'id': [1, 2, 3], 'name': ['RUB', 'USD', 'EUR']},
            'create_sql': """
                CREATE TABLE currencies (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50) NOT NULL
                );
            """
        },
        'units_of_measurement': {
            'data': {'id': [1, 2], 'short_name': ['шт.', 'кг'], 'full_name': ['Штуки', 'Килограммы']},
            'create_sql': """
                CREATE TABLE units_of_measurement (
                    id SERIAL PRIMARY KEY,
                    short_name VARCHAR(50) NOT NULL,
                    full_name VARCHAR(255) NOT NULL
                );
            """
        }
    }

    for table_name, table_info in tables.items():
        recreate_table(table_name, table_info['create_sql'], engine)
        df = pd.DataFrame(table_info['data'])
        save_df_to_csv(df, f'{table_name}.csv')
        save_df_to_sql(df, table_name, engine)
        print(f"{table_name.capitalize()} DataFrame:", df)


generate_data()
