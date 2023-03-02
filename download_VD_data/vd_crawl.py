# -*- coding: utf-8 -*-
"""
Created on Wed Aug  3 20:36:53 2022
@author: Leo Wang
"""

import requests
import csv
import os
import datetime
import time
import xml.etree.ElementTree as ET
import gzip
import dateutil.relativedelta
import shutil

#起始日和最終日(總抓取天數為最終日-起始日+1)
first_day = '2021-01-05'
last_day = '2021-01-05'

# 儲存VD資料的位置
PATH_DATABASE = {1: 'C:/VD_1分鐘資料', 5: 'C:/VD_5分鐘資料'}
# VD檔案名稱(前綴)
VD_FILENAME_PREFIX = {1: 'vd_value_', 5: 'vd_value5_'}

#雲端資料庫網址
VD_FILE_URL = 'https://tisvcloud.freeway.gov.tw/history/_vd/'

#國道VD里程範圍抓取
VD_mile_do={'N1':100.9,'N3':110.8}
VD_mile_dont=['N1H','N2','N3A','N3N','N3K','5N','5S','N5']



def create_dict_forMonth(path, first_day: datetime.datetime,
                last_day: datetime.datetime):
    while first_day < last_day:
        year = str(first_day.year) + '年'
        month = str(first_day.month) + '月'
        # path_year = os.path.join(path, year)
        # os.makedirs(path_year, exist_ok=True)
        path_month = os.path.join(path, year, month)
        os.makedirs(path_month, exist_ok=True)

        first_day = first_day + dateutil.relativedelta.relativedelta(months=1)

def calculate_running_time(start: float):
    end = time.time()
    total_time = end - start
    hr = int(total_time / 3600)
    minute = int((total_time - 3600 * hr) / 60)
    second = int(total_time - 3600 * hr - 60 * minute)
    print('使用時間: %d 小時 %d 分 %d 秒' % (hr, minute, second))
    return 0

#解壓縮檔案
def decompress(path_save_compress,download_first):
    f_name = download_first.replace(".gz","")
    path_save_xml=os.path.join(path_save_compress,f_name)
    g_file = gzip.GzipFile(download_first)
    try:
        open(path_save_xml, "wb").write(g_file.read())
        g_file.close()
    except:
        print("")
        g_file.close()

# 下載資料
def download_data(path1, path2, start_day):
    VD_DATA_COLNAME = ['version', 'listname', 'updatetime', 'interval', 'vdid', 'status', 'datacollecttime', 'vsrdir', 'vsrid',
         'speed', 'laneoccupy', 'carid', 'volume']

    url = VD_FILE_URL
    year = str(start_day.year) + '年'
    month = str(start_day.month) + '月'
    day = str(start_day.day) + '日'

    path_write_file = os.path.join(path1, str(year), str(month), str(day) + '.csv')
    write_file = open(path_write_file, 'w', newline='')
    writer = csv.writer(write_file)
    writer.writerow(VD_DATA_COLNAME)

    # 開始嘗試爬蟲下載VD
    end_day = start_day + datetime.timedelta(days=1)
    while start_day < end_day:
        # VD時間
        catch_time = datetime.datetime.strftime(start_day, '%Y/%m/%d Hour %H Min %M')
        date = datetime.datetime.strftime(start_day, '%Y%m%d')
        day_time = datetime.datetime.strftime(start_day, '%H%M')
        print(f"Try to crawl VD data, the datetime = {catch_time}")

        file_name = VD_FILENAME_PREFIX[vd_dataFrequency] + day_time + '.xml.gz'
        catch_file_path = url + '/' + date + '/' + file_name
        html = requests.get(catch_file_path)

        if html.status_code == 200:
            # 網頁端有回應，可爬蟲下載
            download_first = os.path.join(path2, file_name)
            with open(download_first, 'wb') as f:
                f.write(html.content)

            check_decompress = 0
            try:
                # 解壓縮
                decompress(path2, download_first)
                # 刪除壓縮檔
                os.remove(download_first)
                # 讀XML檔看有沒有問題
                path_read_xml = os.path.join(path2, file_name[:-3])
                tree = ET.parse(path_read_xml)
                check_decompress = 1
            except ET.ParseError as e:
                if e.code == 3:
                    print("XML檔案內容為空! 無法讀取")
                print(f"VD file has problem! Now delete it!")
                # 刪除壓縮檔
                if os.path.exists(download_first):
                    os.remove(download_first)
                # 刪除解壓縮後檔案
                if os.path.exists(path_read_xml):
                    os.remove(path_read_xml)

                raise e

            # 讀XML檔成功
            if check_decompress == 1:
                # 讀資料
                layer1 = tree.getroot()
                layer1_data = []

                # 抓layer1的屬性
                for catch_layer1 in layer1.attrib:
                    layer1_data.append(layer1.attrib[catch_layer1])

                # 抓layer1的子root-->稱layer2
                for layer2 in layer1:
                    layer2_data = layer1_data[:]
                    # 抓layer2的屬性
                    for catch_layer2 in layer2.attrib:
                        layer2_data.append(layer2.attrib[catch_layer2])

                    # 抓layer2的子root-->稱layer3
                    for layer3 in layer2:
                        layer3_data = layer2_data[:]
                        # 抓layer3的屬性
                        for catch_layer3 in layer3.attrib:
                            layer3_data.append(layer3.attrib[catch_layer3])

                        VD_info = layer3_data[4].split('-')
                        VD_info = [VD_info[i] for i in range(len(VD_info)) if VD_info[i] != '']
                        if len(VD_info) > 2:
                            Freeway = VD_info[1]

                            # 檢查國道種類
                            check_freeway_type = 0
                            # 需檢查里程的國道
                            if Freeway in VD_mile_do.keys():
                                mile = float(VD_info[3])
                                if mile < VD_mile_do[Freeway]:
                                    check_freeway_type = 1
                            # 不需檢查的國道
                            elif Freeway in VD_mile_dont:
                                check_freeway_type = 1

                            if check_freeway_type > 0:
                                # 抓layer3的子root-->稱layer4
                                for layer4 in layer3:
                                    layer4_data = layer3_data[:]
                                    # 抓layer4的屬性
                                    for catch_layer4 in layer4.attrib:
                                        layer4_data.append(layer4.attrib[catch_layer4])

                                    # 抓layer4的子root-->稱layer5
                                    for layer5 in layer4:
                                        layer5_data = layer4_data[:]
                                        # 抓layer5的屬性
                                        for catch_layer5 in layer5.attrib:
                                            layer5_data.append(layer5.attrib[catch_layer5])

                                        writer.writerow(layer5_data)
                os.remove(path_read_xml)
                print(f"Catch success!")

            else:
                print(f"Read file failed!")

        else:
            print(f"Catch failed!")

        #下載
        start_day = start_day + datetime.timedelta(minutes=vd_dataFrequency)

    write_file.close()



if __name__ == "__main__":

    def ask_input_vdFreq() -> int:
        # 要求使用者輸入欲下載VD資料時間頻率(1分鐘或5分鐘)
        while True:
            vd_dataFrequency = int(input("資料下載頻率(1分鐘或5分鐘): "))
            if vd_dataFrequency not in [1, 5]:
                print(f"不接受{str(vd_dataFrequency)}. 只能輸入1或5, 請再試一次!")
            else:
                break
        return vd_dataFrequency

    def download_data(firstDay, lastDay):

        def create_directories():
            print(f"Start creating necessary directories.... 開始建立下載VD資料所需資料夾... ")
            # 創每個月的資料夾
            # exist_ok: for value True leaves directory unaltered
            create_dict_forMonth(PATH_DATABASE[vd_dataFrequency], first_day, last_day)

            # 創桌面下載資料夾
            work = os.getcwd()
            path_download_dict = os.path.join(work, 'download_vd')
            os.makedirs(path_download_dict, exist_ok=True)
            path_download = os.path.join(path_download_dict, str(first_day.year))
            os.makedirs(path_download, exist_ok=True)

            print("Creating directories done! 資料夾建立成功!")
            return 0

        #建立資料夾
        create_directories()

        # 下載資料
        while firstDay < lastDay:
            download_data(PATH_DATABASE[vd_dataFrequency], path_download, firstDay)
            firstDay = firstDay + datetime.timedelta(days=1)

        # 刪除下載資料夾
        shutil.rmtree(path_download_dict)

        print("The whole data download success! 全部資料已下載完成!")

        return 0

    #for calculate running time
    start = time.time()

    first_day = datetime.datetime.strptime(first_day, '%Y-%m-%d')
    last_day = datetime.datetime.strptime(last_day, '%Y-%m-%d') + datetime.timedelta(days=1)

    vd_dataFrequency = ask_input_vdFreq()
    download_data(firstDay, lastDay)

    calculate_running_time(start=start)
    print("All Tasks Done! 所有工作已完成!")

