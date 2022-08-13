import subprocess
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import csv
import pandas as pd
import requests
import datetime
from data_bot import token
from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.utils import executor
from ParcerAll import main
from subprocess import *
import datetime as DT


bot = Bot(token = token)
dp = Dispatcher(bot)


@dp.message_handler(commands=["start", "info"])
async def start_command(message: types.Message):
    await message.reply("Hello! This is bot will send information about morning dumping for you every day!")
    await message.reply("So, if you want to update data in your google sheets, enter /update_info\nIf you want to send info in this is chat, enter /send_info ")


@dp.message_handler(commands=['send_info'])
async def parce_info(message: types.Message, file_name=f'Parce information'):
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secrets.json', scope)
    client = gspread.authorize(creds)

    sheet = client.open('сводная наличия и ррц у партнеров online')
    sheet_instance = sheet.worksheet('Сводная по тв партнеры')
    values = sheet_instance.get_all_values()
    sales_data = pd.DataFrame(values[1:], columns=values[0])
    sales_data.to_csv(f'{file_name}.csv', encoding='UTF-8')

    await message.answer_document(open(f'{file_name}.csv', 'rb'))


@dp.message_handler(commands=["update_info"])
async def parce_start(message: types.Message):
    try:
        await message.reply("Process run! Wait a few minutes.")
        parcer = subprocess.run(['python', 'main.py'])
    except Exception as error:
        print('Process failed: ', error)
        await message.reply("Sorry, something went wrong! Please send message about errors to @koreechdhs")




if __name__ == '__main__':
    executor.start_polling(dp) # bot_start