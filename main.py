import os
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from subprocess import call
from subprocess import Popen
import subprocess
from subprocess import sys
from queue import Queue
from selenium.webdriver.common.by import By
# pip install selenium
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import NoSuchElementException
import gspread
from threading import Thread
import time


def Rozetka_Parcer():
        try:
            Rozetka = subprocess.run(["python",'Rozetka_Parcer.py'])
        except Exception as e:
         print ('Process failed: ',e)

def ALLO_Parcer():
    try:
        Allo = subprocess.run(['python','ALLOParcer.py'])
    except Exception as e:
        print ('Process failed: ',e)


def Eldo_Parcer():
    try:
        Eldo = subprocess.run(['python', 'ParcerEldorado.py'])
    except Exception as e:
        print('Process failed: ', e)     


def Foxtrot_Parcer():
    try:
        Fox = subprocess.run(['python', 'ParcerFoxtrot.py'])        
    except Exception as e:
        print ('Process failde: ', e) 


def ATL_Parcer():
    try:
        Fox = subprocess.run(['python','ParcerATL_ATL.py'])        
    except Exception as e:
        print ('Process failde: ', e) 


def Brain_ITBOX_parser():
    try:
        Brain = subprocess.run(['python', 'ParcerATL_ATL.py'])
    except Exception as e:
        print('Process failed: ', e)


def Parcer_Epicentr():
    try:
        Epicentr = subprocess.run(['python', 'ParcerATL.py'])
    except Exception as e:
        print('Process failed: ', e)

def Autobaza_Parcer():
    try:
        Autobaza = subprocess.run(['python', 'ParcerAUTOBAZA.py'])
    except Exception as e:
        print('Process failed: ', e)

def Citrus_Parcer():
    try:
        Citrus = subprocess.run(['python', 'ParcerCitrus.py'])
    except Exception as e:
        print('Process failed: ', e)
def Foxtrot_Parcer():
   try: 
    Foxtrot = subprocess.run(['python','ParcerFoxtrot.py'])
   except Exception as e:
       print('Process failed: ', e)

def MTI_Parcer():
    try:
        MTI = subprocess.run(['python','ParcerMTI.py'])
    except Exception as e:
        print('Process failse: ', e)

def Winauto_Parcer():
    try:
        Winauto = subprocess.run(['python','ParcerWinautoVersion2.0.py'])
    except Exception as e:
        print('Process failed: ', e)

def Stylus_Parcer():
    try:
        Stylus = subprocess.run(['python','Stylus_Parcer.py'])
    except Exception as e:
        print('Process failed: ', e)

def TTT_Parcer():
    try:
        TTT = subprocess.run(['python', 'TTT_Parcer.py'])
    except Exception as e:
        print('Process failed: ', e)

def ZZHUK_parcer():
    try:
        ZZHUK = subprocess.run(['python','ZZHUK_parcer.py'])
    except Exception as e:
        print('Process failed: ', e)

def stocom_parcer():
    try:
        ZZHUK = subprocess.run(['python','130Parcer.py'])
    except Exception as e:
        print('Process failed: ', e) 

def comfy_parcer():
    try:
        ZZHUK = subprocess.run(['python','ComfyParcer.py'])
    except Exception as e:
        print('Process failed: ', e)                   

if  __name__  ==  "__main__": 
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for f in [stocom_parcer, comfy_parcer,
        
                  Rozetka_Parcer, ALLO_Parcer, Eldo_Parcer, 

                  Foxtrot_Parcer, ATL_Parcer, Brain_ITBOX_parser, 

                  Parcer_Epicentr, Autobaza_Parcer, Citrus_Parcer, 

                  Foxtrot_Parcer, MTI_Parcer, Winauto_Parcer, 

                  Stylus_Parcer, TTT_Parcer, ZZHUK_parcer,

                  ]:

            futures.append(executor.submit(f))
    
        for future in futures:
         future.result()


    #Allo.start()
    #Eldo.start()
