#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'ipetrash'

import datetime as DT

from typing import List, Tuple, Union
from pathlib import Path
import gspread
from oauth2client.service_account import ServiceAccountCredentials
# pip install pandas
import pandas as pd

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
    sheet_instance = sheet.get_worksheet(4)
    sheet_instance.insert_rows(items.values.tolist())


def write_to_csv(items) -> None:

    """Функция принимает dataframe с элементами, преобразует их в list и записывает их в google sheet"""

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secrets.json', scope)

    client = gspread.authorize(creds)
    sheet = client.open('сводная наличия и ррц у партнеров online')
    sheet_instance = sheet.get_worksheet(13)
    sheet_instance.clear()
    sheet_instance.insert_rows(items.values.tolist())


def parse(url_TV,url_registrators,url_magnitols: str) -> List[Tuple[str, str, str]]:

    options = Options()
    options.add_argument('--headless')

    items = []

    driver = webdriver.Firefox(options=options)
    driver.implicitly_wait(0.1)

    try:

        while True:

            print("Load: ", url_TV)
            driver.get(url_TV)
            
            for item_el in driver.find_elements_by_css_selector(".product-card"):

                name = item_el.find_element_by_css_selector('.product-card__title').text.strip('Парктронік').strip(
                                                            'Монітор для задніх пасажирів').rstrip(
                                                            'з Android 9.').rstrip('Smart TV').rstrip(
                                                            'Graphite').strip('Професійна втомобільна відеокамера').strip(
                                                            'Антена').strip('- рамка перехідна').strip('2 din').strip(
                                                            'USB').rstrip('Gray').rstrip('Grafit').strip(
                                                            'Відеореєстратор').rstrip('Wi-Fi').strip(
                                                            'Камера заднього виду').strip(
                                                            'Система видеопарковки').strip(
                                                            'Відеокамера внутрішня').strip(
                                                            'Штатна магнітола').strip(
                                                            'Мультимедійний інтерфейс').strip(
                                                            'Мультимедійний центр').strip(
                                                            'Паркувальна камера заднього виду')

                # Не у всех товаров есть ценаr
                try:
                    price = item_el.find_element_by_css_selector('.v-price-box__cur').text.strip('грн').strip('₴')
                except NoSuchElementException:
                    price = '-'

                try:nal = item_el.find_element_by_css_selector('.v-price-box__text--out-stock').text
                except NoSuchElementException:
                    nal = "Есть в наличии"

                row = name,f'{DT.datetime.now():%H:%M_%d-%m-%Y}','', price, nal
                items.append(row)

            # Если есть кнопка перехода на следующую страницу, то продолжаем цикл, иначе завершаем
            try:
                a_next_page = driver.find_element_by_css_selector('.pagination__next a')
                url_TV = a_next_page.get_attribute('href')

            except NoSuchElementException:
                break
    
    finally:
     try:
        while True:

            print("Load: ", url_registrators)
            driver.get(url_registrators)

            for item_el in driver.find_elements_by_css_selector(".product-card"):

                name = item_el.find_element_by_css_selector('.product-card__title').text.strip('Парктронік').strip(
                                                            'Монітор для задніх пасажирів').rstrip(
                                                            'з Android 9.').rstrip('Smart TV').rstrip(
                                                            'Graphite').strip('Професійна втомобільна відеокамера').strip(
                                                            'Антена').strip('- рамка перехідна').strip('2 din').strip(
                                                            'USB').rstrip('Gray').rstrip('Grafit').strip(
                                                            'Відеореєстратор').rstrip('Wi-Fi').strip(
                                                            'Камера заднього виду').strip(
                                                            'Система видеопарковки').strip(
                                                            'Відеокамера внутрішня').strip(
                                                            'Штатна магнітола').strip(
                                                            'Мультимедійний інтерфейс').strip(
                                                            'Мультимедійний центр').strip(
                                                            'Паркувальна камера заднього виду')

                # Не у всех товаров есть ценаr
                try:

                    price = item_el.find_element_by_css_selector('.v-price-box__cur').text.strip('грн').strip('₴')

                except NoSuchElementException:
                    price = '-'

                try:

                    nal = item_el.find_element_by_css_selector('.v-price-box__text--out-stock').text
                except NoSuchElementException:

                    nal = "Есть в наличии"

                row = name,f'{DT.datetime.now():%H:%M_%d-%m-%Y}','', price, nal
                items.append(row)

            # Если есть кнопка перехода на следующую страницу, то продолжаем цикл, иначе завершаем
            try:
                a_next_page = driver.find_element_by_css_selector('.pagination__next a')
                url_registrators = a_next_page.get_attribute('href')

            except NoSuchElementException:
                break
    
     finally:
      try:
        while True:
            print('Load:', url_magnitols)
            driver.get(url_magnitols)

            for item_el in driver.find_elements_by_css_selector(".product-card"):

                name = item_el.find_element_by_css_selector('.product-card__title').text.strip('Парктронік').strip(
                                                            'Монітор для задніх пасажирів').rstrip(
                                                            'з Android 9.').rstrip('Smart TV').rstrip(
                                                            'Graphite').strip('Професійна втомобільна відеокамера').strip(
                                                            'Антена').strip('- рамка перехідна').strip('2 din').strip(
                                                            'USB').rstrip('Gray').rstrip('Grafit').strip(
                                                            'Відеореєстратор').rstrip('Wi-Fi').strip(
                                                            'Камера заднього виду').strip(
                                                            'Система видеопарковки').strip(
                                                            'Відеокамера внутрішня').strip(
                                                            'Штатна магнітола').strip(
                                                            'Мультимедійний інтерфейс').strip(
                                                            'Мультимедійний центр').strip(
                                                            'Паркувальна камера заднього виду')

                # Не у всех товаров есть ценаr
                try:

                    price = item_el.find_element_by_css_selector('.v-price-box__cur').text.strip('грн').strip('₴')

                except NoSuchElementException:

                    price = '-'

                try:

                    nal = item_el.find_element_by_css_selector('.v-price-box__text--out-stock').text

                except NoSuchElementException:

                    nal = "Есть в наличии"

                row = name,f'{DT.datetime.now():%H:%M_%d-%m-%Y}','', price, nal
                items.append(row)

            # Если есть кнопка перехода на следующую страницу, то продолжаем цикл, иначе завершаем
            try:

                a_next_page = driver.find_element_by_css_selector('.pagination__next a')
                url_magnitols = a_next_page.get_attribute('href')

            except NoSuchElementException:
                break

      finally:
        driver.quit()

    return items


def save_goods(
        file_name: Union[str, Path],
        items: List[Tuple[str, str, str]],
        encoding='utf-8'
):
    df = pd.DataFrame(items, columns=['Name','Data','', 'Price', 'Nal'])
    write_to_csv(df)
    write_to_csv_dont_clear_table(df)


if __name__ == '__main__':

    url_magnitols = "https://allo.ua/ua/avtomagnitoly/proizvoditel-gazer/seller-allo/"
    url_registrators = "https://allo.ua/ua/videoregistratory/proizvoditel-gazer/"
    url_TV = "https://allo.ua/ua/televizory/proizvoditel-gazer/seller-allo/"

    items = parse(url_TV,url_registrators,url_magnitols)
    print(f'Total goods: {len(items)}')

    file_name = f'ALLO_parser_{DT.datetime.now():%Y-%m-%d}.csv'
    print(f'Saved to {file_name}')

    save_goods(file_name, items)
