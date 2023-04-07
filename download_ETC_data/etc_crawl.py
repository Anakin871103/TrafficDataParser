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
import winsound
import configparser
SOUND_FREQ = 2500  # Set Frequency To 2500 Hertz
SOUND_DURATION = 1000  # Set Duration To 1000 ms == 1 second


COLUMN_NAMES = {'M03A': ['TimeInterval', 'GantryID', 'Direction', 'VehicleType', 'Volume'],
               'M05A': ['TimeInterval', 'GantryFrom', 'GantryTo', 'VehicleType', 'SpaceMeanSpeed', 'Volume'],
               'M06A': ['VehicleType', 'DetectionTime_O', 'GantryID_O', 'DetectionTime_D', 'GantryID_D', 'TripLength', 'TripEnd', 'TripInformation'],
               'M08A': ['TimeInterval', 'GantryFrom', 'GantryTo', 'VehicleType', 'Volume']}

ETC_DATATYPE_LIST = ['M03A', 'M04A', 'M05A', 'M06A', 'M07A', 'M08A']

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def change_directory(path: str):
    #If the target path (directories) does not exist, will create one and inform users.
    #Otherwise, will move into the the target path.
    if not os.path.exists(path):
        print(f"Path {path} does not exist!")
        os.makedirs(path)
    os.chdir(path)

def download_data(year:str, firstDate:str, lastDate:str, dataType:str):

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


def combine_data(firstDate: str, lastDate:str, dataType: str):

    start = datetime.datetime.strptime(firstDate, '%Y-%m%d')
    end = datetime.datetime.strptime(lastDate, '%Y-%m%d')

    while start <= end: #Level: day 跑每天的loop迴圈

        #依照不同的ETC資料格式，建立一個dataframe結構
        dfAppend = pd.DataFrame(columns=COLUMN_NAMES[dataType])

        year = datetime.datetime.strftime(start, "%Y")
        month = datetime.datetime.strftime(start, "%m")
        day = datetime.datetime.strftime(start, "%d")

        #每天的起始時間與終止時間
        startHour, startMin, endHour, endMin = '00', '00', '23', '55'

        startTime = datetime.datetime.strptime(startHour + startMin, '%H%M')
        endTime = datetime.datetime.strptime(endHour + endMin, '%H%M')

        while startTime <= endTime:

            hour = datetime.datetime.strftime(startTime, "%H")
            min = datetime.datetime.strftime(startTime, "%M")
            second = '00'

            # 到M0X資料夾
            change_directory(path=f"{DOWNLOAD_PATH}/{month}/{dataType}/{year}{month}{day}/{hour}")
            fileName = f"TDCS_{dataType}_{year}{month}{day}_{hour}{min}{second}"

            try:
                dfTemp = pd.read_csv(fileName + ".csv") #讀取csv
                dfTemp.columns = COLUMN_NAMES[dataType] #存取欄位名稱
                dfAppend = pd.concat([dfAppend, dfTemp], ignore_index=True) #把資料新增進去
                print(f"Combined {dataType} -> {year}-{month}-{day}-{hour}-{min}-{second} successfully!")
            except pd.errors.EmptyDataError:
                #例外處理: 內容為空
                print(f"Ignore {dataType} -> {year}-{month}-{day}-{hour}-{min}-{second} since it is empty")
            except FileNotFoundError:
                #例外處理: 找不到檔案
                print(f"Ignore {dataType} -> {year}-{month}-{day}-{hour}-{min}-{second} since file is not found!")
            except KeyboardInterrupt:
                print("KeyboardInterrupt")
            except:
                #其他例外事件: 直接丟出來
                print("Exceptions that are not handled in this program occurs : ")
                winsound.Beep(SOUND_FREQ, SOUND_DURATION)
                raise


            startTime = startTime + datetime.timedelta(minutes=ETC_DATA_TIMESTEP) #切換到下一個資料時階


        change_directory(path=f"{COMBINATION_PATH}/{dataType}/{year}年/{str(int(month))}月/")

        # 將dfAppend轉成csv
        dfAppend.to_csv(day + '日.csv')
        print(f"Create {day + '日.csv'} successfully!")

        start = start + datetime.timedelta(days=1)  # switch to the next day

    print("COMBINATION TASKS DONE!")


def read_config_file():
    CONFIG_FILE_NAME = 'etc_config.ini'
    config = configparser.ConfigParser()
    config.read(os.path.join(BASE_DIR, CONFIG_FILE_NAME))
    # Get values from the configuration file
    global DOWNLOAD_PATH
    global COMBINATION_PATH
    global ETC_DATA_TIMESTEP
    DOWNLOAD_PATH = config.get('setting', 'download_path')
    COMBINATION_PATH = config.get('setting', 'combination_path')
    ETC_DATA_TIMESTEP = int(config.get('setting', 'data_timestamp'))
    print(f"Successfully read config file:")
    print(f"Download Path = {DOWNLOAD_PATH}")
    print(f"COMBINATION_PATH = {COMBINATION_PATH}")
    print(f"ETC_DATA_TIMESTAMP (ETC資料頻率(min)) = {ETC_DATA_TIMESTEP}")

    return 0

if __name__ == "__main__":
    read_config_file()


    #Program Start
    while True:
        mode = input("Mode 1 = Download Data / Mode 2 = Combine Data. Please Enter Mode: ")
        if mode == '1':
            print("Download Data!")
        elif mode == '2':
            print("Combine Data!")
        else:
            print(f"Unkown Mode = {mode} Only accept 1 or 2")
            continue

        while True:
            dataType = 'M' + input('type = ? (EX. 06)') + 'A'
            if dataType in ['M03A', 'M04A', 'M05A', 'M06A', 'M07A', 'M08A']:
                break
            else:
                print(f"Unkown dataType = {dataType} Only accept {ETC_DATATYPE_LIST}")

        if mode == '1':
            year = str(input("year = ?"))
            firstDate = input("firstDate = MMDD")
            lastDate = input("lastDate = MMDD")
            download_data(year, firstDate, lastDate, dataType)
        elif mode == '2':
            firstDate = input('FirstDate YYYY-MMDD: ')  # '2021-0101'
            lastDate = input('EndDate YYYY-MMDD: ')  # '2021-0131'
            combine_data(firstDate, lastDate, dataType)

        break


#End
print("ALL TASKS DONE")

