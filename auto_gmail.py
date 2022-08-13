# -*- coding: UTF-8 -*-

import datetime as DT
from http import client
import time
from typing import List, Tuple, Union
from pathlib import Path
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import csv
import smtplib
from password import *
from email.mime.text import MIMEText


class Gmail_message():

    def take_values(self, file_name, file_name_of_stock):
        
        global scope 
        scope = ['https://spreadsheets.google.com/feeds', 
                'https://www.googleapis.com/auth/drive']

        global creds
        creds = ServiceAccountCredentials.from_json_keyfile_name('client_secrets.json',  scope)

        global client
        client = gspread.authorize(creds)

        sheet = client.open('сводная наличия и ррц у партнеров online')        
        sheet_instance = sheet.worksheet('сводная цен у партнеров')
        sheet_instance_stock = sheet.worksheet('сводная наличия у партнеров')

        #получение количества строк
        rows_count = len(sheet_instance.col_values(1))
        print(rows_count)

        #запись данных в csv
        values = sheet_instance.get_all_values()
        values_stock = sheet_instance_stock.get_all_values()

        sales_data = pd.DataFrame(values[1:], columns=values[0]) 
        data_of_stock = pd.DataFrame(values_stock[1:], columns=values_stock[0])

        write_to_csv = sales_data.to_csv(file_name, encoding = 'UTF-8')
        write_to_csv_stock = data_of_stock.to_csv(file_name_of_stock, encoding='utf-8')

        print('Write complete')
    

    def compare_data(self, file_name, company_name, company_name_to_write, file_name_of_stock): 

        file_name_to_write = f'{company_name_to_write}_to_write.csv'
        file_name_to_write_stock = f'{company_name_to_write}_to_write_stock.csv'
        global demping
        demping = []
        global stock
        stock = []
        global na
        na = []
        #Чтение данных из csv файла в котором данные из google sheets
        with open (file_name, encoding='UTF-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter = ',')
            #Алгоритм наполнения списка данных
            for row in reader:
                
                if row['Price MTI'] > row[company_name]:

                    if row[company_name] == '#N/A'\
                       or row[company_name] == '-'\
                       or row[company_name] == ''\
                       or row['Price MTI'] == row[company_name]\
                       or row['Price MTI'] == '':
                        continue
                    
                    demping_name = row['Name MTI']
                    price_MTI = row['Price MTI'].replace('\xa0', '').replace(' ', '')
                    demping_price = row[company_name].strip('грн').strip('.').strip('₴').replace('\xa0', '').replace(
                                                      ' ', '').replace('грн', '').replace(',00.', '').replace(',00 грн.','')
                    available_MTI = row['Available']
                    if 'Есть в наличии' or 'В наявності' in available_MTI:
                        available_MTI = 'In stock' 
                    else:                                                             
                        available_MTI = 'Out of stock'

                    rows =  demping_name, price_MTI, demping_price, available_MTI
                    if int(price_MTI) - 1 > int(demping_price) and int(demping_price) != 0:
                            demping.append(rows)
        #Алгоритм наполнения списка по наличию товара на сайте-партнере
        with open(file_name_of_stock, encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile, delimiter = ',')
                for row in reader:
                    if row['Stock MTI'] == 'Есть в наличии' or 'В наявності':
                        if row[company_name] == '#N/A' or '*ціна на' in row[company_name]:
                            continue
                        else:
                            if row[company_name].capitalize() == 'Немає в наявності' or 'Нет в наличии' and row['Stock MTI']  == 'Есть в наличии' or 'В наявності': 
                                if row[company_name] == 'Есть в наличии'  or 'КУПИТЬ ' in row[company_name]:
                                    continue
                                stock_name_MTI = row['Name MTI']
                                stock_MTI = row['Stock MTI']
                                stock_MTI = 'Есть в наличии'
                                out_of_stock = row[company_name]
                                out_of_stock = 'Нет в наличии'

                            rows = stock_name_MTI, stock_MTI, out_of_stock
                            stock.append(rows)
        with open(file_name_of_stock, encoding='UTF-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter = ',')
            for row in reader:
                if row[company_name] == '#N/A':
                    stock_name_MTI = row['Name MTI']
                    stock_MTI = row['Stock MTI']
                    out_of_stock = row[company_name]
                    out_of_stock = 'Нет на сайте'
                    rows = stock_name_MTI, stock_MTI, out_of_stock
                    na.append(rows) 
                   
        #Создание нового csv файла с уже готовым списом нарушения РРЦ           
        df = pd.DataFrame(demping, 
                         columns=['Name','Actual price', 'Your price', 'Available'])
        df.to_csv(file_name_to_write, encoding='UTF-8')          
        print(f'file saved to {file_name_to_write} price.csv')  
        global value_writes
        value_writes = len(demping)
        print(value_writes)

        df_stock = pd.DataFrame(stock, 
                         columns=['Name','Stock MTI', 'Stock company'])
        df_stock.to_csv(file_name_to_write_stock, encoding='UTF-8')          
        print(f'file saved to {file_name_to_write_stock}.csv')  
        return df_stock
    #Метод создания таблицы Gsheets с нарушением цены на сайте партнере
    def write_to_gsheets(self, company_name, email_first, email_second):

        df = pd.DataFrame(demping, 
                         columns=['Name','Actual price', 'Your price', 'Available'])

        df_stock = pd.DataFrame(stock, 
                         columns=['Name','Stock MTI', 'Stock company'])
        df_na = pd.DataFrame(na, 
                         columns=['Name','Stock MTI', 'Stock company'])                 

        content = open(f'{company_name}_to_write.csv', 'r', encoding = 'UTF-8').read()
        content_stock = open(f'{company_name}_to_write_stock.csv', 'r', encoding='utf-8').read()
        #создал новую таблцу
        sheet_with_demping = client.create(f'{company_name} | {DT.datetime.now():%Y-%m-%d}')
        #upload_csv = client.import_csv(sheet_with_demping.id, content)

        share_emails = ['k.boyar@gazer.com', 'n.boyar@gazer.com', f'{email_first}', f'{email_second}']
                      
        global sheet_id 
        sheet_id = sheet_with_demping.id
        print(sheet_with_demping)
        sheet = client.open_by_key(sheet_id)
        
        worksheet_info = sheet.add_worksheet(title = 'Инструкция', rows = 5, cols = 13) 
        worksheet_price = sheet.add_worksheet(title = 'Контроль РРЦ', rows = 1000, cols = 6)
        worksheet_stock = sheet.add_worksheet(title = 'Нет в наличии', rows=1000, cols=6)
        #worksheet_na = sheet.add_worksheet(title = 'Нет на сайте', rows=1000, cols=6)

        worksheet_info_table = sheet_with_demping.get_worksheet(0)
        worksheet_info.update_cell(1, 2, 'Инструкция по применению \n \n На первом листе "Контроль РРЦ" мы видим 4 стоблца: \n \n - Номенклатура товара \n - Актуальный прайс с соответствием РРЦ \n - Неправильная цена на вашем сайте \n  наличие товара на сайте дистрибьютора \n \n Основная задача этого листа - наглядная демонстрация нарушения цены в вашем интернет-магазине \n \n На втором листе "Нет в наличии" - мы видим отсутствие товара на вашем сайте, при наличии товара на сайте дистрибьютора \n \n 1 столбец - наименование товара \n 2 столбец - наличие товара на сайте дистрибьютора \n 3 столбец - отсутствие наличия товара на вашем сайте \n \n Основная задача этого листа - получение информации для коррекции наличия товара на вашем сайте')

        sheet1 = sheet_with_demping.sheet1
        sheet_with_demping.del_worksheet(sheet1)
        #Выгрузка из созданного ранее файла csv в Gsheets

        #Формирование листа 
        worksheet_price_table = sheet_with_demping.get_worksheet(1)
        worksheet_price_table.update([df.columns.values.tolist()] + df.values.tolist())

        worksheet = sheet_with_demping.get_worksheet(2)
        worksheet.update([df_stock.columns.values.tolist()] + df_stock.values.tolist())

       # worksheet_na_site = sheet_with_demping.get_worksheet(3)
       # worksheet_na_site.update([df_na.columns.values.tolist()] + df_na.values.tolist())
        
        for share_email in share_emails:
        #Доступ к таблице
            if value_writes > 1:
                sheet_with_demping.share(share_email, perm_type = 'user', role = 'writer')
            else:
                print(f'Value writes on {company_name} < 1, continue')
                continue        

        print('Create new Gsheet tabel successful')
        return sheet_with_demping

    def send_gmail(self, company_name, email_first, email_second):
        
        sender = "k.boyar@gazer.com" 

        partners = ['n.boyar@gazer.com', 'k.boyar@gazer.com', f'{email_first}', f'{email_second}']
                   
        password = f'{password_gazer}'

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        
        message = f"Здравствуйте, коллеги, если у вас есть возможность, то большая просьба поправить цены на товары в Gsheets таблице к которой я предоставил Вам доступ, заранее спасибо и хорошего вам дня!\n https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid=0"

            
        try:

            server.login(sender, password)
            msg = MIMEText(message)
            msg["Subject"] = f"Демпинг {company_name} |{DT.datetime.now():%Y-%m-%d}"

            for send in partners:
                if value_writes > 1:
                    server.sendmail(sender, send, msg.as_string())
                    print(f"The message was sent successfully! {send}")
                else: 
                    print('values_writes < 1, massage successfully: FALSE')
                    continue
            

        except Exception as _ex:
            print(f"{_ex}\nCheck your login or password please!")

        time.sleep(20) #Создаем задержку после выгрузки таблицы в каждую компанию что бы не превысить количество запросов в минуту Gsheets API  
def main(file_name, file_name_of_stock):

    main = Gmail_message()
    main.take_values(file_name, file_name_of_stock)


def companies(file_name, file_name_of_stock):

    #Почты партнеров
    emails_first = {
                    '130com': 'ap@130.com.ua',
                    'Comfy': 'kostya20041234@gmail.com',
                    'Rozetka' : 'kostya20041234@gmail.com',
                    #'Allo': 'opezdalya@gmail.com'
                   # 'Rozetka': 'dyakova@rozetka.com.ua', 
                    'Allo': 'allo-km63@allo.ua', 
                    'Foxtrot': 'Skarlato-V@foxtrot.ua', 
                    'Citrus': 'nelin@citrus.com.ua', 
                    'Eldorado': 'Alexandr.Petrunek@eldorado.ua', 
                    'Baza autozvuka': 'zakaz.m@avtozvuk.ua', 
                    'Winauto': 'e.adamenko@winauto.ua', 
                    'ZZHUK': 'ostap.protsiv@zhuk.mobi', 
                    'ATL': 'accessories@tlauto.com.ua', 
                    'Stylus': 'sal@stylus.com.ua',
                     'Notebooker': 'kostya20041234@gmail.com'
                     }

    emails_second = {
                    '130com': 'kostya20041234@gmail.com',
                    'Comfy': 'kostya20041234@gmail.com',
                    'Rozetka': 'opezdalya@gmail.com',
                    #'Allo': 'kostya20041234@gmail.com'
                    # 'Rozetka': 'nagorodnij@rozetka.com.ua', 
                      'Allo': 'allo-mp94@allo.ua',
                     'Foxtrot': 'Makovenko-A@foxtrot.ua', 
                     'Citrus': 'kostya20041234@gmail.com',
                     'Eldorado': 'Igor.Statkevich@eldorado.ua', 
                     'Baza autozvuka': 'opezdalya@gmail.com', 
                     'Winauto': 'annav.adamenko@gmail.com',
                     'ZZHUK': 'opezdalya@gmail.com', 
                     'ATL': 'kovalchenko@atl.ua', 
                     'Stylus': 'pol@stylus.com.ua',
                     'Notebooker': 'opezdalya@gmail.com'
                    }       

    #Интернет-магазины партнеров
    companies = ['130com','Comfy','Rozetka', 'Allo', 'Foxtrot', 
                'Citrus', 'Eldorado', 'Baza autozvuka', 
                'Winauto', 'ZZHUK', 'ATL', 'Stylus', 
                'Notebooker']
                #'Epicentr', 'BRAIN', 
    #companies = ['ZZHUK']
    company_method = Gmail_message()

    #Цикл рассылки всех пріколов интернет-магазинам партнеров
    for company in companies:

        company_method.compare_data(file_name, company, company, file_name_of_stock)
        company_method.write_to_gsheets(company, emails_first[company], emails_second[company])
        company_method.send_gmail(company, emails_first[company], emails_second[company])

if __name__ == '__main__':
    
    file_name = 'write_to_csv.csv'
    file_name_of_stock = 'write_to_csv_stock.csv'
    main(file_name, file_name_of_stock)
    companies(file_name, file_name_of_stock)
