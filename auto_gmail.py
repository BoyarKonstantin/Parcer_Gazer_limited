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
import csv


class Gmail_message():

    def take_values(self, file_name):

        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

        creds = ServiceAccountCredentials.from_json_keyfile_name('client_secrets.json', scope)

        client = gspread.authorize(creds)
        sheet = client.open('сводная наличия и ррц у партнеров online')
        
        sheet_instance = sheet.get_worksheet(1)

        #получение количества строк
        rows_count = len(sheet_instance.col_values(1))
        print(rows_count)

    #запись данных в csv
        values = sheet_instance.get_all_values()

        sales_data = pd.DataFrame(values[1:],columns=values[0]) 

        write_to_csv = sales_data.to_csv(file_name,encoding = 'UTF-8' )
        print('Write complite')
    

    def compare_data(self, file_name, company_name, company_name_to_write): 

        file_name_to_write = f'{company_name_to_write}_to_write.csv'

        demping = []
        #Чтение данных из csv файла в котором данные из google sheets
        with open (file_name, encoding='UTF-8') as csvfile:

            reader = csv.DictReader(csvfile, delimiter = ',')
            #Алгоритм наполнения списка данных
            for row in reader:

                if row['Price MTI'] > row[company_name]:

                    if row[company_name] == '#N/A' or row[company_name] == '-' or row[company_name] == '':
                        continue

                    demping_name = row['Name MTI']
                    price_MTI = row['Price MTI']
                    demping_price = row[company_name]
                    
                    rows =  demping_name,  price_MTI, demping_price
                    demping.append(rows)

        #Создание нового csv файла с уже готовым списом нарушения РРЦ           
        df = pd.DataFrame(demping, columns=['Name','Actual price', 'Your price'])
        df.to_csv(file_name_to_write, encoding='UTF-8')          
        print(f'file saved to {file_name_to_write}.csv')  

    #Метод создания таблицы Gsheets с нарушением цены на сайте партнере
    def write_to_gsheets(self, company_name):

        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

        creds = ServiceAccountCredentials.from_json_keyfile_name('client_secrets.json', scope)

        client = gspread.authorize(creds)
        content = open(f'{company_name}_to_write.csv', 'r', encoding = 'UTF-8').read()

        #создал новую таблцу
        sheet_with_demping = client.create(f'{company_name} | {DT.datetime.now():%Y-%m-%d}')
        print(sheet_with_demping)
        #Доступ к таблице
        sheet_with_demping.share('k.boyar@gazer.com', perm_type = 'user', role = 'writer')
        sheet_with_demping.share('n.boyar@gazer.com', perm_type = 'user', role = 'writer')
        sheet_with_demping.share('v.samoylenko@gazer.com', perm_type = 'user', role = 'writer')
        sheet_with_demping.share('p.gulyk@gazer.com', perm_type = 'user', role = 'writer')
        sheet_with_demping.share(f'partner_email_1', perm_type = 'user', role = 'writer')
        sheet_with_demping.share(f'partner_email_2', perm_type = 'user', role = 'writer')

        #Выгрузка из созданного ранее файла csv в Gsheets
        upload_csv = client.import_csv(sheet_with_demping.id, content)
        print('Create new Gsheet tabel successful')


    def send_gmail(self):
        pass


def main(file_name):

    main = Gmail_message()
    main.take_values(file_name)


def rozetka_company(file_name):

    
    rozetka = Gmail_message()
    rozetka.compare_data(file_name, 'Розетка', 'Rozetka')
    rozetka.write_to_gsheets('Rozetka')
    partner_email_1 = ''
    partner_email_2 = ''
if __name__ == '__main__':

    file_name = 'write_to_csv.csv'
    main(file_name)
    rozetka_company(file_name)


