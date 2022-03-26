from asyncio import SendfileNotAvailableError
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

    def take_values(self, file_name):

        global scope 
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

        global creds
        creds = ServiceAccountCredentials.from_json_keyfile_name('client_secrets.json', scope)

        global client
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

                    if row[company_name] == '#N/A' or row[company_name] == '-' or row[company_name] == '' or row['Price MTI'] == row[company_name] or row['Price MTI'] == '':
                        continue
                    
                    demping_name = row['Name MTI']
                    price_MTI = row['Price MTI'].replace('\xa0', '').replace(' ', '')
                    demping_price = row[company_name].strip('грн').strip('.').strip('₴').replace('\xa0', '').replace(' ', '')
                    
                    rows =  demping_name,  price_MTI, demping_price
                    if int(price_MTI) - 1 > int(demping_price) and int(demping_price) != 0 :
                        demping.append(rows)

        #Создание нового csv файла с уже готовым списом нарушения РРЦ           
        df = pd.DataFrame(demping, columns=['Name','Actual price', 'Your price'])
        df.to_csv(file_name_to_write, encoding='UTF-8')          
        print(f'file saved to {file_name_to_write}.csv')  


    #Метод создания таблицы Gsheets с нарушением цены на сайте партнере
    def write_to_gsheets(self, company_name, partner_email_1, partner_email_2):

        content = open(f'{company_name}_to_write.csv', 'r', encoding = 'UTF-8').read()

        #создал новую таблцу
        sheet_with_demping = client.create(f'{company_name} | {DT.datetime.now():%Y-%m-%d}')

        global sheet_id  
        sheet_id = sheet_with_demping.id
        print(sheet_with_demping)

        #Доступ к таблице
        sheet_with_demping.share('k.boyar@gazer.com', perm_type = 'user', role = 'writer')
        #sheet_with_demping.share('n.boyar@gazer.com', perm_type = 'user', role = 'writer')

        #sheet_with_demping.share('v.samoylenko@gazer.com', perm_type = 'user', role = 'writer')
        #sheet_with_demping.share('p.gulyk@gazer.com', perm_type = 'user', role = 'writer')

        #sheet_with_demping.share(f'{partner_email_1}', perm_type = 'user', role = 'writer')
        #sheet_with_demping.share(f'{partner_email_2}', perm_type = 'user', role = 'writer')

        #Выгрузка из созданного ранее файла csv в Gsheets
        upload_csv = client.import_csv(sheet_with_demping.id, content)
        print('Create new Gsheet tabel successful')
        return sheet_with_demping

    def send_gmail(self, company_name):

        sender = "kostya20041234@gmail.com"
        password = f'{password_gmail}'

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        message = f"Здравствуйте, коллеги, если у вас есть возможность, то большая просьба поправить цены на товары в Gsheets таблице к которой я предоставил Вам доступ,  заранее спасибо и хорошего вам дня!\n https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid=0"

        try:

            server.login(sender, password)
            msg = MIMEText(message)
            msg["Subject"] = f"Демпинг {company_name} |{DT.datetime.now():%Y-%m-%d}"
            server.sendmail(sender, 'k.boyar@gazer.com', msg.as_string())

            print("The message was sent successfully!")

        except Exception as _ex:
            print( f"{_ex}\nCheck your login or password please!")


def main(file_name):

    main = Gmail_message()
    main.take_values(file_name)


def rozetka_company(file_name):

    rozetka = Gmail_message()
    rozetka.compare_data(file_name, 'Розетка', 'Rozetka')

    partner_email_1 = ''
    partner_email_2 = ''
    rozetka.write_to_gsheets('Rozetka', partner_email_1, partner_email_2)
   
    rozetka.send_gmail('Rozetka')

def allo_company(file_name):

    allo = Gmail_message()
    allo.compare_data(file_name, 'АЛЛО', 'Allo')

    partner_email_1 = ''
    partner_email_2 = ''
    allo.write_to_gsheets('Allo', partner_email_1, partner_email_2)

    #allo.send_gmail('Allo')


def foxtrot_company(file_name):

    foxtrot = Gmail_message()
    foxtrot.compare_data(file_name, 'Фокстрот', 'Foxtrot')

    partner_email_1 = ''
    partner_email_2 = ''
    foxtrot.write_to_gsheets('Foxtrot', partner_email_1, partner_email_2)

    #foxtrot.send_gmail('Allo')

if __name__ == '__main__':

    file_name = 'write_to_csv.csv'
    main(file_name)

   # Розетка работает отлично

    rozetka_company(file_name)
    
   # Разобраться с ценами, часто выйгружает одинаковые, возможно проблема в библиотеке

   # allo_company(file_name)

   #Работает отлично
    #foxtrot_company(file_name)
