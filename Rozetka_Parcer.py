import datetime as DT
import time

from typing import List, Tuple, Union
from pathlib import Path

# pip install pandas
import pandas as pd
from selenium.webdriver.common.by import By
# pip install selenium
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import NoSuchElementException
import gspread
from oauth2client.service_account import ServiceAccountCredentials
def write_to_csv_dont_clear_table(items) -> None:
    """Функция принимает dataframe с элементами, преобразует их в list и записывает их в google sheet"""
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secrets.json', scope)

    client = gspread.authorize(creds)
    sheet = client.open('Парсер по интернет-магазинам by Boyar.K Зима 2021-2022')
    sheet_instance = sheet.get_worksheet(3)
    sheet_instance.insert_rows(items.values.tolist())
def write_to_csv(items) -> None:
    """Функция принимает dataframe с элементами, преобразует их в list и записывает их в google sheet"""
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secrets.json', scope)

    client = gspread.authorize(creds)
    sheet = client.open('сводная наличия и ррц у партнеров online')
    sheet_instance = sheet.worksheet('Розетка данные выгрузки')
    sheet_instance.clear()
    sheet_instance.insert_rows(items.values.tolist())
    sheet_instance.delete_columns(26, 500)


def parse(url: str) -> List[Tuple[str, str, str]]:
    options = Options()
    options.add_argument('--headless')

    items = []

    driver = webdriver.Firefox(options=options)
    driver.implicitly_wait(10)
    try:
        page = last_page = 1
        while page <= last_page:
            if page > 1:
                url = f'https://rozetka.com.ua/search/?page={page}&producer=gazer&seller=rozetka&text=Gazer'

            print(f'Load: {url}')
            driver.get(url)
            index = 0
            for item_el in driver.find_elements(By.CSS_SELECTOR, ".goods-tile"):
                name = item_el.find_element(By.CSS_SELECTOR, '.goods-tile__title').text.strip('Автомагнитола штатная').strip('Видеорегистратор').strip('Ситема контроля слепых зон').strip().strip('').strip('Автомобильный монитор для задних пасажиров').strip('Амортизатор багажника').strip('Внутрення, выносная камера').rstrip('для видеорегистратора').strip('Парковочная система').strip('Камера заднего вида').strip('Телевизор').strip()
                # Не у всех товаров есть цена
                try:
                    price = int(item_el.find_element(By.CSS_SELECTOR, '.goods-tile__price-value').text.replace(' ', ''))
                except NoSuchElementException:
                    price = '-'

                nal = item_el.find_element(By.CSS_SELECTOR, '.goods-tile__availability').text

                row = name,index,f'{DT.datetime.now():%H:%M_%d-%m-%Y}', price, nal
                print(row)
                items.append(row)

            # Если есть кнопка перехода на следующую страницу, то продолжаем цикл, иначе завершаем
            try:
                last_page = 14

            except NoSuchElementException:
                break

            page += 1

    finally:
        driver.quit()

    return items

def save_goods(
        file_name: Union[str, Path],
        items: List[Tuple[str, str, str]],
        encoding='utf-8'
):
    df = pd.DataFrame(items,columns=['Name','index','data', 'Price', 'Nal'])
    write_to_csv(df)
    write_to_csv_dont_clear_table(df)

if __name__ == '__main__':
    url = "https://rozetka.com.ua/search/?page=1&producer=gazer&seller=rozetka&text=Gazer"
    items = parse(url)
    print(f'Total goods: {len(items)}')

    file_name = f'rozetka_parser_{DT.datetime.now():%Y-%m-%d}.csv'
    print(f'Saved to {file_name}')
    save_goods(file_name, items)
