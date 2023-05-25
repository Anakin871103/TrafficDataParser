import csv
import os
import pandas as pd
import numpy as np

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

    def generate_skiprows(self, desired_start_row: int, desired_end_row: int) -> np.ndarray:
        # this is for the "skiprows" parameters in pd.read(). 
        # It will generate a set of number of "rows" that should be skipped accodrding to user's input. 
        originalNumberOfRows = self.get_CSVFileOriginalNumberOfRows()
        allRows = np.array([i for i in np.arange(originalNumberOfRows+1)])
        wantedRows = np.array([0] + [i for i in np.arange(desired_start_row, desired_end_row+1)])  # [0] -> row of column names
        skipRows = np.delete(allRows, wantedRows)
        print(f"WANTED ROWS = 0 (col row) and {desired_start_row} ~ {desired_end_row}")
        return skipRows
