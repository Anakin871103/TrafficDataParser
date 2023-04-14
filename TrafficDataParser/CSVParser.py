import csv
import os
import pandas as pd

# colunmName = ['startkilo', 'endkilo', 'year', 'date', 'starttime', 'endtime', 'crash',
#               'lane', 'minlane', 'addlane', 'totalwidth', 'lanewidth',
#               'inshoulder', 'outshoulder', 'upslope','downslope','upslopelength',
#               'downslopelength', 'maxupslope', 'maxdownslope', 'curvelength', 'minradiuslength','minradius',
#               'continuouscurve', 'pavement', 'cement', 'interchange', 'tunnellength', 'tunnelin',
#               'tunnelout', 'remark', 'one', 'shouderoallow', 'speedlimit', 'camera',
#               'service', 'windspeed', 'rain', 'Var_windspeed', 'Var_rain', 'volume_S', 'volume_L',
#               'volume_T', 'volume', 'PCU', 'Speed_volume', 'Speed_PCU', 'heavy_rate',
#               'Var_volume', 'Var_PCU', 'Var_Speed_volume', 'Var_Speed_PCU']


class CSVParser():

    def __init__(self, fileRoute, fileName):
        self.fileRoute = fileRoute
        self.fileName = fileName
        self.path = os.path.join(self.fileRoute, self.fileName)
        self.CSVFileContent = []
        self.CSVFileColumnNames = 0


    def readCSVfile(self, encoding, **kwargs):
        self.CSVFileContent = pd.read_csv(filepath_or_buffer=self.path, encoding=encoding, **kwargs)
        self.CSVFileColumnNames = self.CSVFileContent.columns.values.tolist()

    def getCSVfileContent(self):
        return self.CSVFileContent

    def getColunmnames(self):
        return self.CSVFileColumnNames

    def get_CSVFileOriginalNumberOfRows(self):
        numberOfRows = len(pd.read_csv(self.path))
        return numberOfRows

    def columnName_to_index(self, columnName):
        try:
            index = self.CSVFileContent.index(columnName)
        except ValueError:
            print(f"columnName: {columnName} is not existed in the columnNameList {self.CSVFileColumnNames}")
            raise ValueError
        return index

