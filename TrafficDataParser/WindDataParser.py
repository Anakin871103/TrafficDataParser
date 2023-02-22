
import module.CSVParser as CSVParser
from datetime import datetime
import pandas as pd


class NotFindWindSpeedError(Exception):
    def __init__(self, inputArgs, message="Can not find Windspeed, the input datetime is = "):
        self.inputArgs = inputArgs
        self.message = message + inputArgs
        super().__init__(self.message)
    pass


class WindDataParser(CSVParser.CSVParser):
    def __init__(self, fileRoute, fileName):
        super().__init__(fileRoute, fileName)

    def get_windspeed(self, endtime: datetime):
        self.CSVFileContent['時間'] = self.CSVFileContent['時間'].replace(['24:00:00'], '00:00') #一個坑: replace "24:00:00" with "00:00"
        index = (pd.to_datetime('20' + self.CSVFileContent['日期'] + " " + self.CSVFileContent['時間']) >= endtime).idxmax()
        #挑出 >= endTime 最接近的那一個時間點
        # if not index:
        #     raise NotFindWindSpeedError(inputArgs=endtime.strftime('%Y/%m/%d %H:%M'))
        # else:
        windspeed = self.CSVFileContent.at[index, '風速']
        return windspeed