import CSVParser as CSVParser
from datetime import datetime
import pandas as pd

class RainDataParser(CSVParser.CSVParser):
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