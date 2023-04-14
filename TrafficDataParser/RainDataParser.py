import CSVParser as CSVParser
from datetime import datetime
import pandas as pd
import numpy as np

class RainDataParser_forWBU(CSVParser.CSVParser):

    '''
    此類別(class)專門for中央氣象局付費購買之資料集使用
    資料位置: NAS/中央氣象局/
    '''

    def __init__(self, fileRoute, fileName):
        super().__init__(fileRoute, fileName)

    def get_rainfall(self, startTime: datetime, endTime: datetime):
        condition1 = (pd.to_datetime(self.CSVFileContent['DTIME']) >= startTime)
        condition2 = (pd.to_datetime(self.CSVFileContent['DTIME']) <= endTime)
        indexList = self.CSVFileContent.index[(condition1 & condition2)].tolist()
        if not indexList:
            rainfall = 0.0
            return rainfall
        rainfallList = []
        for index in indexList:
            rainfallList.append(self.CSVFileContent.at[index, 'RN'])

        rainfall = np.sum(rainfallList)

        return rainfall