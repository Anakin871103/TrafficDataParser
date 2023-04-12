
import numpy as np
import CSVParser as CSVParser

class NotFindEquipmentIDError(Exception):
    """Exception raised for errors in the input arguments.
      Attributes:
          inputArgs -- input arguments
          message -- explanation of the error
    """
    def __init__(self, inputArgs: dict, message="Can not find equipment ID corresponding to input arguments "):
        self.inputArgs = inputArgs
        self.message = message + str(inputArgs)
        super().__init__(self.message)
    pass

class UnknownDirectionError(Exception):
    pass


class MileToEquipementIDConverter(CSVParser.CSVParser):

    directionTransform = {'北': '北向', '南': '南向', '東': '東向', '西': '西向'}
    # Caution: According to the direction of freeways, the  Startkilo and endkilo may be different
    startAndEndKiloCorrespond = {'北向': {'startkilo': 'endkilo', 'endkilo': 'startkilo'},
                                  '南向': {'startkilo': 'startkilo', 'endkilo': 'endkilo'}}

    def __init__(self, fileRoute, fileName):
        super().__init__(fileRoute, fileName) #Input: Reference File
        super().readCSVfile()

    def get_VDID(self, inputArgs: dict) -> list:
        """inputArgs = ['freeway': a, 'direction':b, 'startkilo':c, 'endkilo':d]"""
        inputArgs['direction'] = MileToEquipementIDConverter.directionTransform[inputArgs['direction']]
        c1 = (self.CSVFileContent.freeway == inputArgs['freeway'])
        c2 = (self.CSVFileContent.direction == inputArgs['direction'])
        # Introduce np.isclose to solve float precise problem - 2022.08.01
        c3 = np.isclose(self.CSVFileContent.startkilo, inputArgs[MileToEquipementIDConverter.startAndEndKiloCorrespond[inputArgs['direction']]['startkilo']])
        c4 = np.isclose(self.CSVFileContent.endkilo, inputArgs[MileToEquipementIDConverter.startAndEndKiloCorrespond[inputArgs['direction']]['endkilo']])

        # 注意: 此處 Startkilo 和 endkilo 設備對照表中 反過來才是正確的
        content = self.CSVFileContent.loc[c1 & c2 & c3 & c4]

        if content.empty:
            print("According to input arguments, can not find VDID .... ")
            raise NotFindEquipmentIDError(inputArgs=inputArgs)

        VDID = content['VDID'].values[0]
        backup_VDID = content['Backup_VDID'].values[0]

        return [VDID, backup_VDID] # return: VDID and BackUpID

    def get_EtagEquipmentID(self, inputArgs: dict) -> list:
        """inputArgs = ['freeway': a, 'direction':b, 'startkilo':c, 'endkilo':d]"""
        inputArgs['direction'] = MileToEquipementIDConverter.directionTransform[inputArgs['direction']]

        c1 = (self.CSVFileContent.freeway == inputArgs['freeway'])
        c2 = (self.CSVFileContent.direction == inputArgs['direction'])
        c3 = np.isclose(self.CSVFileContent.startkilo, inputArgs[MileToEquipementIDConverter.startAndEndKiloCorrespond[inputArgs['direction']]['startkilo']])
        c4 = np.isclose(self.CSVFileContent.endkilo, inputArgs[MileToEquipementIDConverter.startAndEndKiloCorrespond[inputArgs['direction']]['endkilo']])

        index = (c1 & c2 & c3 & c4).idxmax()
        equipmentID = self.CSVFileContent.at[index, 'equipmentID']
        equipmentID_nextGantry = self.CSVFileContent.at[index+1, 'equipmentID']

        while equipmentID_nextGantry == equipmentID:
            index = index + 1
            equipmentID_nextGantry = self.CSVFileContent.at[index, 'equipmentID']

        if equipmentID:
            return [equipmentID, equipmentID_nextGantry]
        else:
            raise NotFindEquipmentIDError(inputArgs=inputArgs)

    def get_weatherEquipmentID(self, inputArgs: dict) -> list:

        return 0


    def get_equipmentName(self, inputArgs: dict):
        """inputArgs = ['freeway': a, 'direction':b, 'startkilo':c, 'endkilo':d]"""
        inputArgs['direction'] = MileToEquipementIDConverter.directionTransform[inputArgs['direction']]
        c1 = (self.CSVFileContent['freeway'] == inputArgs['freeway'])
        c2 = (self.CSVFileContent['direction'] == inputArgs['direction'])
        c3 = np.isclose(self.CSVFileContent.startkilo, inputArgs[MileToEquipementIDConverter.startAndEndKiloCorrespond[inputArgs['direction']]['startkilo']])
        c4 = np.isclose(self.CSVFileContent.endkilo, inputArgs[MileToEquipementIDConverter.startAndEndKiloCorrespond[inputArgs['direction']]['endkilo']])

        stationName = self.CSVFileContent.loc[c1 & c2 & c3 & c4]['stationName']
        # 注意: 此處 Startkilo 和 endkilo 設備對照表中 反過來才是正確的
        if stationName.empty:
            raise NotFindEquipmentIDError(inputArgs=inputArgs)
        else:
            stationName = stationName.tolist()[0]  # Convert to String
            return stationName