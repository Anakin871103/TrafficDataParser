# -*- coding: utf-8 -*-

"""
Created on Tue Oct  5 10:19:06 2021
@author: acer895395543
"""

import csv
import os
import datetime
import time
import dateutil.relativedelta

from datetime import timedelta
from dateutil.relativedelta import relativedelta


## Gloabal Variables Area ##
#VD Data type
MINUTE_TYPE = 5

#The date range of the start and the last date
FIRST_DATE = ''
LAST_DATE = ''

PATH_READ = 'E://VD_5分鐘資料/'
PATH_MOTHER = os.path.join('E:\\')

## Export files by MONTH or Day
EXPOR_UNIT_DICT = {0: 'Month', 1: 'Day'}


#Create dictionaries by Month or by Data
def create_dict(exportUnitNum: int, firstDate, lastDate):

    os.makedirs(PATH_MOTHER, exist_ok=True)

    while firstDate < lastDate:

        year = str(firstDate.year) + '年'
        path_year=os.path.join(PATH_MOTHER,year)
        if not os.path.isdir(path_year):
            os.mkdir(path_year)
        
        if EXPOR_UNIT_DICT[exportUnitNum] == 'Day':
            month = str(firstDate.month) + '月'
            path_month = os.path.join(PATH_MOTHER, year, month)
            os.makedirs(path_month, exist_ok=True)

        firstDate = firstDate + dateutil.relativedelta.relativedelta(months=1)


def create_header(exportUnitNum: int) -> list:
    if EXPOR_UNIT_DICT[exportUnitNum] == 'Month':
        header = ['vd_id','日期','{小時:{分:{車道:[speed,laneoccupy,S_volume,T_volume,L_volume]}}}   字典提取方法:字典名稱[hr][minute][lane]=[車速,佔有率,S,T,L]']
    elif EXPOR_UNIT_DICT[exportUnitNum] == 'Day':
        header = ['vd_id','{小時:{分:{車道:[speed,laneoccupy,S_volume,T_volume,L_volume]}}}   字典提取方法:字典名稱[hr][minute][lane]=[車速,佔有率,S,T,L]']
    else:
        raise Exception('Exception error!')

    return header


def calculate_running_time(startTime: float):
    end = time.time()
    total_time = end - startTime
    hr = int(total_time / 3600)
    minute = int((total_time - 3600 * hr) / 60)
    second = int(total_time - 3600 * hr - 60 * minute)

    print('使用時間: %d 小時 %d 分 %d 秒' % (hr, minute, second))

    return 0


if __name__ == "__main__":


    def start():
        while True:
            try:
                exportUnit = int(input(f"請輸入壓縮後VD輸出單位: {EXPOR_UNIT_DICT}\n"))
                EXPOR_UNIT_DICT[exportUnit]
                break
            except ValueError:
                print("Please enter integer value. ")
            except KeyError:
                print(f"The value {exportUnit} is not listed.")

        
        finalPath = os.path.join(f"壓縮後VD_按{EXPOR_UNIT_DICT[exportUnit]}產生", 'VD_' + str(MINUTE_TYPE) + '分鐘資料')
        global PATH_MOTHER
        PATH_MOTHER = os.path.join(PATH_MOTHER, finalPath)

        while True:
            try:
                global FIRST_DATE, LAST_DATE
                input_firstDate = input("請輸入資料下載的起始日期: yyyy-mm-dd\n")
                input_lastDate = input("請輸入資料下載的終止日期: yyyy-mm-dd\n")
                datetime.datetime.strptime(input_firstDate, '%Y-%m-%d')
                datetime.datetime.strptime(input_lastDate, '%Y-%m-%d')
                
                FIRST_DATE = input_firstDate
                LAST_DATE = input_lastDate
                break

            except ValueError:
                print("請輸入正確日期格式 yyyy-mm-dd !!")

        return exportUnit

    #Start
    exportUnitNum = start()

    # for running time calculation
    startTime = time.time()

    # convert to datetime object
    firstDate = datetime.datetime.strptime(FIRST_DATE, '%Y-%m-%d')
    lastDate = datetime.datetime.strptime(LAST_DATE, '%Y-%m-%d') + datetime.timedelta(days=1)

    #創資料夾
    create_dict(exportUnitNum=exportUnitNum, firstDate=firstDate, lastDate=lastDate)

    #Get header
    Header = create_header(exportUnitNum)

    #每月的迴圈尋找
    while firstDate < lastDate:

        # define day
        day = firstDate.day
        
        # define start day and end day
        startDay = firstDate
        if EXPOR_UNIT_DICT[exportUnitNum] == 'Month':
            endDay = firstDate + dateutil.relativedelta.relativedelta(months=1) 
            path_write_file = os.path.join(PATH_MOTHER, str(firstDate.year)+'年', str(firstDate.month)+'月.csv') 
        elif EXPOR_UNIT_DICT[exportUnitNum] == 'Day':
            endDay = firstDate + dateutil.relativedelta.relativedelta(days=1)
            path_write_file = os.path.join(PATH_MOTHER, str(firstDate.year)+'年', 
                                       str(firstDate.month)+'月', str(firstDate.day) + '日.csv')
        else: raise Exception("Exception")

        write_file = open(path_write_file, 'w', newline='', encoding='utf-8')
        writer = csv.writer(write_file)
        writer.writerow(Header)

        print(f"Start Compression VD data of each {EXPOR_UNIT_DICT[exportUnitNum]}")

        #壓縮資料(每日讀取)
        while startDay < endDay:
            print(f"Current process day is  = {startDay}")
            year, month, day, hour = startDay.year, startDay.month, startDay.day, startDay.hour
            PATH_READ_file = os.path.join(PATH_READ, str(year)+'年', str(month)+'月', str(day)+'日.csv')

            #所有VD_ID資料的總整理
            Total_VD_data={}

            #日期
            date=datetime.datetime.strftime(startDay,'%Y-%m-%d')

            #讀取每日資料
            with open(PATH_READ_file, 'r', newline='', encoding='utf-8') as csv_file:
                #header略過
                line=csv_file.readline()
                #檢查是否至少有三筆資料
                catch_data=[]
                check=0
                for i in range(3):
                    line=csv_file.readline()
                    line=line.strip()
                    line=line.split(',')
                    if line is not None and line!=['']:
                        check+=1
                        catch_data.append(line)

                while check==3:
                    #確認有三筆資料(S T L)
                    if catch_data[0][11]+catch_data[1][11]+catch_data[2][11] == 'STL':
                        if float(catch_data[0][5]) == 0:
                            vd_id=catch_data[0][4]
                            #如果還沒在ID集合內，創單日資料總集合
                            if vd_id not in Total_VD_data.keys():
                                #單日資料總集合
                                date_data={}
                                for create_hour in range(24):
                                    date_data[create_hour]={}
                                    for create_minute in range(0,60,MINUTE_TYPE):
                                        date_data[create_hour][create_minute]={}
                                Total_VD_data[vd_id]=date_data

                            #有效資料內容
                            speed = catch_data[0][9]
                            occupancy = catch_data[0][10]
                            S = catch_data[0][12] #S_volume
                            T = catch_data[1][12] #T_volume
                            L = catch_data[2][12] #L_volume
                            combine_data = [speed, occupancy, S, T, L]

                            #寫入位址
                            lane = catch_data[0][8] #col:vsrid
                            catch_time = datetime.datetime.strptime(catch_data[0][6],'%Y/%m/%d %H:%M:%S') #col: datacollecttime
                            hr = catch_time.hour
                            minute = catch_time.minute
                            #調整有效資料狀態
                            Total_VD_data[vd_id][hr][minute][lane] = combine_data

                        #重新抓資料
                        catch_data=[]
                        check=0
                        for i in range(3):
                            line = csv_file.readline()
                            line = line.strip()
                            line = line.split(',')
                            if line is not None and line!=['']:
                                check += 1
                                catch_data.append(line)

                    else:
                        check=2
                        catch_data=catch_data[1:]
                        #補進新的一行
                        line=csv_file.readline()
                        line=line.strip()
                        line=line.split(',')
                        if line is not None and line!=['']:
                            check+=1
                            catch_data.append(line)

            for VD_name in Total_VD_data.keys():

                if EXPOR_UNIT_DICT[exportUnitNum] == 'Month':
                    compress_data=[VD_name,date,Total_VD_data[VD_name]]
                elif EXPOR_UNIT_DICT[exportUnitNum] == 'Day':
                    compress_data=[VD_name,Total_VD_data[VD_name]]

                writer.writerow(compress_data)

            startDay=startDay+datetime.timedelta(days=1)

        write_file.close()

        if EXPOR_UNIT_DICT[exportUnitNum] == 'Month':
            firstDate = firstDate + dateutil.relativedelta.relativedelta(months=1) 
        elif EXPOR_UNIT_DICT[exportUnitNum] == 'Day':
            firstDate = firstDate + dateutil.relativedelta.relativedelta(days=1)

        calculate_running_time(startTime)




