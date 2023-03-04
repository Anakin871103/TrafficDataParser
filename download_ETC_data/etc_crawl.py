# -*- coding: utf-8 -*-
"""
Created on Wed Aug  3 20:36:53 2022
@author: Leo Wang
"""

import os 
import requests as request
import tarfile
import datetime
import pandas as pd
# import winsound
# frequency = 2500  # Set Frequency To 2500 Hertz
# duration = 1000  # Set Duration To 1000 ms == 1 second
# winsound.Beep(frequency, duration)


COLUMN_NAMES = {'M03A': ['TimeInterval', 'GantryID', 'Direction', 'VehicleType', 'Volume'],
               'M05A': ['TimeInterval', 'GantryFrom', 'GantryTo', 'VehicleType', 'SpaceMeanSpeed', 'Volume'],
               'M06A': ['VehicleType', 'DetectionTime_O', 'GantryID_O', 'DetectionTime_D', 'GantryID_D', 'TripLength', 'TripEnd', 'TripInformation'],
               'M08A': ['TimeInterval', 'GantryFrom', 'GantryTo', 'VehicleType', 'Volume']}

BASE_PATH = 'D:/freewayData/'
DOWNLOAD_PATH = "D:/freewayData/ETC_2022/download"

def change_directory(path: str):
    os.chdir(path)

def download_data():

    year = str(input("year = ?"))
    firstDate = input("firstDate = MMDD")
    lastDate = input("lastDate = MMDD")
    dataType = input('type = ? (EX. M06)') + 'A'

    first_date = year + firstDate + '0000'
    last_date = year + lastDate + '2355'

    start = datetime.datetime.strptime(first_date, '%Y%m%d%H%M')
    end = datetime.datetime.strptime(last_date, '%Y%m%d%H%M')

    while start <= end:

        year = datetime.datetime.strftime(start, "%Y")
        month = datetime.datetime.strftime(start, "%m")
        day = datetime.datetime.strftime(start, "%d")
        hour = datetime.datetime.strftime(start, "%H%M")

        #url = "https://tisvcloud.freeway.gov.tw/history/_vd/" + year + month + date + "/vd_value5_" + hour + ".xml.gz"
        download_ETC_fileName = dataType + "_" + year + month + day + ".tar.gz"
        url = "https://tisvcloud.freeway.gov.tw/history/TDCS/" + dataType + "/" + download_ETC_fileName
        html = request.get(url)

        if html.status_code == 200:

            # 切換目標資料夾: 從當前資料夾至預備儲存資料的資料夾
            os.chdir(DOWNLOAD_PATH)
            # 確認month的資料夾是否存在
            if not os.path.exists(month):
                os.makedirs(month) #新增month
            os.chdir(month)

            with open(download_ETC_fileName, "wb") as f:
                f.write(html.content)

            #解壓縮檔案
            # g_file = gzip.GzipFile(mode='rb', fileobj=open(download_ETC_fileName, 'rb'))
            # newFileName = download_ETC_fileName.replace(".tar.gz", "")

            # open file
            file = tarfile.open(download_ETC_fileName)
            # extracting file
            file.extractall('.')
            file.close()

            # open(newFileName, "wb+").write(g_file.read())
            # g_file.close()

            #刪除原本的壓縮檔
            if os.path.exists(download_ETC_fileName):
                os.remove(download_ETC_fileName)

            print(month + day + " is downloaded")

        start = start + datetime.timedelta(days=1)


def combine_data():

    dataType = input('type = ? (ex. M06)') + 'A'
    #default 20210101~20211231
    first_date = input('FirstDate YYYY-MMDD-HHMM: ') #'2021-0101-0000'
    last_date = input('EndDate YYYY-MMDD-HHMM: ') #'2021-0131-2355'
    
    change_directory(path=BASE_PATH)

    start = datetime.datetime.strptime(first_date, '%Y-%m%d-%H%M')
    end = datetime.datetime.strptime(last_date, '%Y-%m%d-%H%M')

    while start <= end: #day level

        #建立一個dataframe結構
        dfAppend = pd.DataFrame(columns=COLUMN_NAMES[dataType])

        year = datetime.datetime.strftime(start, "%Y")
        month = datetime.datetime.strftime(start, "%m")
        day = datetime.datetime.strftime(start, "%d")

        startHour = '00'
        startMin = '00'
        endHour = '23'
        endMin = '55'

        startTime = datetime.datetime.strptime(startHour + startMin, '%H%M')
        endTime = datetime.datetime.strptime(endHour + endMin, '%H%M')

        while startTime <= endTime:

            hour = datetime.datetime.strftime(startTime, "%H")
            min = datetime.datetime.strftime(startTime, "%M")
            second = '00'

            # 到M0X資料夾
            change_directory(path=f"{DOWNLOAD_PATH}/{month}/{dataType}/{year}{month}{day}/{hour}")
            #change_directory(path=BASE_PATH + month + '/' + dataType + '/' + year + month + day + '/' + hour)

            fileName = 'TDCS_' + dataType + '_' + year + month + day + '_' + hour + min + second

            try:
                dfTemp = pd.read_csv(fileName + ".csv") #讀取csv
                dfTemp.columns = COLUMN_NAMES[dataType] #存取欄位名稱
                dfAppend = dfAppend.append(dfTemp, ignore_index=True) #append
                print(f"Combined {dataType} -> {year}-{month}-{day}-{hour}-{min}-{second} successfully!")
            except pd.errors.EmptyDataError as e1:
                #例外處理: 內容為空
                print(f"Ignore {dataType} -> {year}-{month}-{day}-{hour}-{min}-{second} since it is empty")
            except FileNotFoundError as e2:
                #例外處理: 找不到檔案
                print(f"Ignore {dataType} -> {year}-{month}-{day}-{hour}-{min}-{second} since file is not found!")


            startTime = startTime + datetime.timedelta(minutes=5)


        change_directory(path=f"{BASE_PATH}/afterCombination/{dataType}/{year}年/{str(int(month))}月/")
        #change_directory(path=BASE_PATH + '/afterCombination/' + dataType + '/2021年/' + str(int(month)) + '月/')

        # 將dfAppend轉成csv
        dfAppend.to_csv(day + '日.csv')
        print(f"Create {day + '日.csv'} successfully!")

        start = start + datetime.timedelta(days=1)

    print("COMBINATION TASKS DONE!")


if __name__ == "__main__":
    #Program Start
    while True:
        mode = input("Mode 1 = Download Data / Mode 2 = Combine Data. Please Enter Mode: ")
        if mode == '1':
            print("Download Data!")
            download_data()
            break
        elif mode == '2':
            print("Combine Data!")
            combine_data()
            break
        else:
            print(f"Unkown Mode = {mode} Only accept 1 or 2")

#End
print("ALL TASKS DONE")

