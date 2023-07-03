# -*- coding: utf-8 -*-
"""
Created on Tue Jan  3 10:01:03 2023
@author: user
"""
# from selenium import webdriver
import requests as request 
import os 
import csv
import datetime
import time
from bs4 import BeautifulSoup
import configparser

WEATHER_STATION_FILE_PATH = ''
WEATHER_STATION_FILE_NAME = '測站網址(1).csv'
DOWNLOAD_PATH = 'E:/Central Weather Bureau/'
CONFIG_FILE_PATH = './config1.ini'
FIRST_DATE = '2022-01-01'
LAST_DATE = '2022-02-01'

COLS = ['測站時間', '測站氣壓', '海平面氣壓', '氣溫', '露點溫度', '相對溼度',
        '風速', '風向', '最大陣風', '最大陣風風向', '降水量', '降水時數', '日照時數',
        '全天空日射量', '能見度', '紫外線指數', '總雲量']

def read_config_file():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE_PATH)
    # Accessing values from the configuration file
    startRow = config.get('setting', 'DOWNLOAD_START_FROM_ROW_NUM')
    endRow = config.get('setting', 'DOWNLOAD_END_AT_ROW_NUM')

    return startRow, endRow

def write_config_file():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE_PATH)

    # Writing changes to the configuration file
    with open(CONFIG_FILE_PATH, 'w') as f:
        config.write(f)
    return 0

def choose_download_rows(startRow: int, endRow: int):

    stations = []
    countyNameList = []

    path = os.path.join(WEATHER_STATION_FILE_PATH, WEATHER_STATION_FILE_NAME)
    # 讀取全台灣所有氣象測站網址
    with open(path, newline='', encoding='utf-8') as csvfile:
         # 跳到指定的ROW: 選擇從哪一個測站開始下載
        rows = csv.reader(csvfile, skipinitialspace=True)
        rows = list(rows) #轉換為list
        # 跳到指定的ROW
        # for i in range(1):
        #     next(rows)
        for row in rows[startRow:endRow+1]:
            stationCode, countyName, stationName, address, url1, url2 = row[0], row[1], row[2], row[7], row[8], row[9]
            stations.append([stationCode, countyName, stationName, address, url1, url2])
            countyNameList.append(countyName)

    return stations, countyNameList

def change_directory(directoryName: str):
    try:
        os.chdir(directoryName)
    except FileNotFoundError as e:
        print("The system cannot find the path specified!!!")
        raise e

def check_html_status(code: int):
    if code == 200:
        return True
    else:
        return False


def writeToCSVfile(stationCode: str, exportFileName: str, mode: str, encoding: str):
    try:
        with open(exportFileName, mode, newline='', encoding=encoding) as f:
            writer = csv.writer(f)
            writer.writerow(COLS)
            for i in totalData:
                writer.writerow(i)
            print(f'測站代號: {stationCode} - 站名: {exportFileName}  is done!')

    except PermissionError as e:
        print("permission error!")
        raise e

    return 0

def get_weather_data(url1, url2, date):

    data = []
    year = datetime.datetime.strftime(date, "%Y")
    month = datetime.datetime.strftime(date, "%m")
    day = datetime.datetime.strftime(date, "%d")

    url = f'{url1}{year}-{month}-{day}{url2}'
    html = request.get(url)
    try:
    
        #嘗試進行網頁爬蟲連線
        if check_html_status(html.status_code):
            # print(f"已成功讀取網址{url}")
            soup = BeautifulSoup(html.text, 'html.parser')
            tag_table = soup.find(id='MyTable')
            rows = tag_table.findAll('tr')

            check = 0
            for row in rows:
                if check < 3:  # 跳過html網頁內容的前3行?
                    check += 1
                else:
                    text_info = row.findAll(['td'])
                    result = []
                    for sub_text in text_info:  # 存取資訊填入list?
                        info = sub_text.get_text()
                        info = info.strip()
                        result.append(info)

                    hour = result[0]

                    if result[0] == '24':
                        # 沒有24:00:00 -> 切換成下一天的00:00:00
                        result[0] = datetime.datetime.strftime(date + datetime.timedelta(days=1), "%Y-%m-%d") \
                                    + ' 00:00:00'
                    else:
                        result[0] = datetime.datetime.strftime(date, "%Y-%m-%d") \
                                    + ' ' + hour + ':00:00'
                    # return result
                    data.append(result)

        else:
            print("網頁無法正常存取!")
            print(1 / 0)  # 拋出例外

    except:
        print("發生錯誤!")
        import winsound
        SOUND_FREQ = 2500  # Set Frequency To 2500 Hertz
        SOUND_DURATION = 1000  # Set Duration To 1000 ms == 1 second
        winsound.Beep(SOUND_FREQ, SOUND_DURATION)

        write_config_file() #讀取目前資料已下載進度並寫入config

        raise

    return data


if __name__ == "__main__":

    print("------- 天氣資料下載 -------")
    startRow, endRow = read_config_file()
    print(f"指定編號{startRow}至{endRow}測站開始下載!")

    stations, countyNameList = choose_download_rows(startRow=int(startRow),
                                                    endRow=int(endRow))

    # 切換目標資料夾: 從當前資料夾至預備儲存資料的資料夾
    os.chdir(DOWNLOAD_PATH)
    # 新建各縣市資料夾
    for countyName in countyNameList:
        os.makedirs(countyName, exist_ok=True)

    #iterate each station
    for station in stations: #ignore row 0: column names
        stationCode, county, stationName, stationAddress, url1, url2 = station[0], station[1], station[2], station[3], station[4], station[5]
        #切換至指定縣市資料夾
        change_directory(county)
        #時間校正
        startDate = datetime.datetime.strptime(FIRST_DATE, '%Y-%m-%d')
        endDate = datetime.datetime.strptime(LAST_DATE, '%Y-%m-%d')

        totalData = []

        #取得氣象資料
        while startDate <= endDate:
            result = get_weather_data(url1=url1, url2=url2, date=startDate)
            totalData.append(result)
            # proceed to the next day
            startDate = startDate + datetime.timedelta(days=1)

        #export file name
        exportFileName = stationName + "_" + stationAddress +'.csv'
        #export csv file
        writeToCSVfile(stationCode=stationCode, exportFileName=exportFileName, mode='w', encoding='utf-8-sig')
        # CURRENT_PROCESS_ROW = stationCode #更新已經目前測站下載進度
        os.chdir('../') #資料夾跳回上層


print("All Tasks DONE!")
os.system("pause")
    
    
