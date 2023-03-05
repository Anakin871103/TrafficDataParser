# -*- coding: utf-8 -*-
"""
Created on Tue Jan  3 10:01:03 2023
@author: user
"""
from selenium import webdriver
import requests as request 
import os 
import csv
import datetime
import time
from bs4 import BeautifulSoup

COLS = ['測站時間', '測站氣壓', '海平面氣壓', '氣溫', '露點溫度', '相對溼度',
        '風速', '風向', '最大陣風', '最大陣風風向', '降水量', '降水時數', '日照時數',
        '全天空日射量', '能見度', '紫外線指數', '總雲量']

stations = []
countyNameList = []

# os.chdir('C://python')
with open('測站網址(1).csv', newline='', encoding='utf-8') as csvfile:
    SKIPROWS = 0  #跳到指定的ROW: 選擇從哪一個測站開始下載
    rows = csv.reader(csvfile, skipinitialspace=True)
    #跳到指定的ROW
    for i in range(SKIPROWS):
        next(rows)
    for row in rows:
        stations.append([row[0], row[1], row[6], row[-2], row[-1]])
        countyNameList.append(row[0])

first = '2021-01-01'
last = '2021-12-31'

#切換目標資料夾: 從當前資料夾至預備儲存資料的資料夾
os.chdir('../data/Reference-Data/Central Weather Bureau/天氣資料庫/')
#新建各縣市資料夾
for countyName in countyNameList:
    if not os.path.exists(countyName): #確認縣市的資料夾是否存在
        os.makedirs(countyName)

#iterate each station
for station in stations[1:]: #ignore row 0: column names

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

    def writeToCSVfile(exportFileName: str, mode: str):
        try:
            with open(exportFileName, mode, newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(COLS)
                for i in totalData:
                    writer.writerow(i)
                print(exportFileName + ' is done!')
        except PermissionError as e:
            print("permission error!")
            raise e

        return 0

    totalData = []

    county = station[0]
    stationName = station[1]
    stationAddress = station[2]
    url1 = station[3]
    url2 = station[4]

    #切換至指定縣市資料夾
    change_directory(county)
    #時間校正
    startDate = datetime.datetime.strptime(first, '%Y-%m-%d')
    endDate = datetime.datetime.strptime(last, '%Y-%m-%d')

    while startDate <= endDate:

        year = datetime.datetime.strftime(startDate, "%Y")
        month = datetime.datetime.strftime(startDate, "%m")
        date = datetime.datetime.strftime(startDate, "%d")

        url = url1 + year +'-' + month + '-' + date + url2
        html = request.get(url)

        if check_html_status(html.status_code):

            soup = BeautifulSoup(html.text, 'html.parser')
            tag_table = soup.find(id='MyTable')
            rows = tag_table.findAll('tr')

            check = 0
            for row in rows:
                if check < 3: #跳過html網頁內容的前3行?
                    check+=1
                else:
                    text_info = row.findAll(['td'])
                    result = []
                    for sub_text in text_info: #存取資訊填入list?
                        info = sub_text.get_text()
                        info = info.strip()
                        result.append(info)

                    hour = result[0]

                    if result[0] == '24':
                        #沒有24:00:00 -> 切換成下一天的00:00:00
                        result[0] = datetime.datetime.strftime(startDate + datetime.timedelta(days=1), "%Y-%m-%d") \
                                    + ' 00:00:00'
                    else:
                        result[0] = datetime.datetime.strftime(startDate, "%Y-%m-%d") \
                                    + ' ' + hour + ':00:00'

                    totalData.append(result)

        else:
            print("網頁無法正常存取!")
            print(1/0) #拋出例外

        #proceed to the next day
        startDate = startDate + datetime.timedelta(days=1)

    exportFileName = stationName + "_" + stationAddress +'.csv'
    #export csv file
    writeToCSVfile(exportFileName=exportFileName, mode='w')
    #資料夾跳回上層
    os.chdir('../')

        
print("All Tasks DONE!")
os.system("pause")
    
    
