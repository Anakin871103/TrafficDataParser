import CSVParser as CSVParser
from datetime import datetime
import pandas as pd
import numpy as np

class WeatherDataParser(CSVParser.CSVParser):
    '''
    此類別(class)專門for從氣象局網頁爬蟲下來的資料
    資料位置: NAS/天氣資料庫
    '''
    def __init__(self, fileRoute, fileName):
        super().__init__(fileRoute, fileName)

    def get_rainfall(self, startTime: datetime, endTime: datetime):


        return 0