'''
Author: Hsuan-Chih (Leo) Wang
Date: 2022/7/31
Introduction:
A traffic data parser which supports two types of traffic data (VD and Etag) as input data.
Users can retrieve traffic characters in the data. For example:
(1)Speed (space speed or spot speed, depending on the type of traffic data)
(2)Traffic volume (cars, trucks, buses)
(3)Occupancy (only provided in VD data)

Besides, several functions are defined to transform the raw traffic characters into other forms, including
(1) PCE -> calculate PCE with user-defined parameters
(2) TotalTraffic Volumes -> sum up the traffic volumes
(3) HeavyRate -> calculate the heavy rate of the traffic,
which is defined as the proportion of the volume of heavy vehicles to all vehicles.

'''

import CSVParser as CSVParser
import numpy as np
from datetime import datetime

covertDirectionToEng = {'北': 'N', '南': 'S'}

class NotFindDataIndexError(Exception):

    def __init__(self, inputArgs: dict, dataType: str):
        message = f"Can not find {dataType} Index : "
        self.inputArgs = inputArgs
        self.message = message + str(inputArgs)
        super().__init__(self.message)
    pass

class TrafficDataParser(CSVParser.CSVParser):

    PCU = {'S': 1.0, 'L': 1.6, 'T': 2.0}
    CAR_TYPE_LIST = ['S', 'L', 'T']

    def __init__(self, fileRoute, fileName, encodingType: str):
        super().__init__(fileRoute=fileRoute, fileName=fileName)
        self.trafficVolumeDict = {'S': 0, 'L': 0, 'T': 0, 'volume': 0, 'PCU': 0.0, 'Occupancy': 0.0,
                                  'Speed_PCU': 0.0, 'Speed_volume': 0.0,
                                  'Var_PCU': 0.0, 'Var_Speed_PCU': 0.0, 'Var_Speed_Volume': 0.0, 'Var_volume': 0.0,
                                  'heavy_rate': 0.0}

        self.trafficSpeedDict = {'S': 0.0, 'L': 0.0, 'T': 0.0, 'Avg': 0.0, 'Median': 0.0}
        self.readCSVfile(encoding=encodingType)

    def reset_attributes(self):
        for item in self.trafficVolumeDict:
            self.trafficVolumeDict[item] = 0.0
        for item in self.trafficSpeedDict:
            self.trafficSpeedDict[item] = 0.0

    def get_PCU_Volumes(self):
        PCE = TrafficDataParser.PCU['S'] * self.trafficVolumeDict['S'] + \
              TrafficDataParser.PCU['T'] * self.trafficVolumeDict['T'] + \
              TrafficDataParser.PCU['L'] * self.trafficVolumeDict['L']
        return PCE

    def get_totalTrafficVolumes(self):
        totalVolume = self.trafficVolumeDict['S'] + self.trafficVolumeDict['L'] + self.trafficVolumeDict['T']
        return totalVolume

    def get_heavyRate(self):
        heavyVehicleVolume = self.trafficVolumeDict['L'] + self.trafficVolumeDict['T']
        totalVolume = self.trafficVolumeDict['L'] + self.trafficVolumeDict['T'] + self.trafficVolumeDict['S']

        try:
            heavyRate = round((heavyVehicleVolume / totalVolume), 3)
        except ZeroDivisionError:
            print(f"ZeroDivisionError! round({heavyVehicleVolume} / {totalVolume}, 3)")
            heavyRate = 0.0

        return heavyRate

    def get_avg_speed(self):
        result = np.mean(np.array([self.trafficSpeedDict['S'],
                                   self.trafficSpeedDict['L'],
                                   self.trafficSpeedDict['T']]))

        return round(result, 1)

    def get_median_speed(self):
        result = np.median(np.array([self.trafficSpeedDict['S'],
                                     self.trafficSpeedDict['L'],
                                     self.trafficSpeedDict['T']]))

        return round(result, 1)

    def __get_trafficVolumes(self, dateTime: datetime, gantryID, direction):
        '''EXAMPLE:
        dateTime = %Y/%m/%d %H:%M
        Direction = 北
        TimeInterval =  2020-02-10 00:00
        trafficVolumeDict = {31: 0, 32: 0, 41: 0, 42: 0, 5: 0}
        '''

        tempResults = {'5': 0, '31': 0, '32': 0, '41': 0, '42': 0}
        directionEng = covertDirectionToEng[direction]

        c1 = (self.CSVFileContent['TimeInterval'] == dateTime.strftime("%Y-%m-%d %H:%M"))
        c2 = (self.CSVFileContent['GantryID'] == gantryID)
        c3 = (self.CSVFileContent['Direction'] == directionEng)
        result = self.CSVFileContent.loc[c1 & c2 & c3]

        if result.empty:
            raise NotFindDataIndexError(inputArgs={'dateTime': dateTime.strftime("%Y-%m-%d %H:%M"), 'gantryID': gantryID, 'direction': direction})

        vehTypeList = result['VehicleType'].tolist()
        trafficList = result['Traffic'].tolist()
        for i in range(5):
            tempResults.update({str(vehTypeList[i]): trafficList[i]})

        self.trafficVolumeDict['S'] = int(tempResults['31']) + int(tempResults['32'])
        self.trafficVolumeDict['L'] = int(tempResults['41']) + int(tempResults['42'])
        self.trafficVolumeDict['T'] = int(tempResults['5'])
        self.trafficVolumeDict['PCU'] = self.get_PCU_Volumes()
        self.trafficVolumeDict['volume'] = self.get_totalTrafficVolumes()

        return self.trafficVolumeDict

    def get_trafficVolumes(self, dateTime: datetime, gantryID, direction):
        '''EXAMPLE:
        dateTime = %Y-%m-%d %H:%M
        Direction = 北
        '''

        tempResults = {'5': 0, '31': 0, '32': 0, '41': 0, '42': 0}  # five types of vehicles
        directionEng = covertDirectionToEng[direction]  # translate direction in Chinese to English

        c1 = (self.CSVFileContent['TimeInterval'] == dateTime.strftime("%Y-%m-%d %H:%M"))
        c2 = (self.CSVFileContent['GantryID'] == gantryID)
        c3 = (self.CSVFileContent['Direction'] == directionEng)
        indexList = self.CSVFileContent.index[(c1 & c2 & c3)].tolist()

        if not indexList:
            raise NotFindDataIndexError(inputArgs={'dateTime': datetime, 'gantryID': gantryID, 'direction': direction})

        vehTypeList = []
        trafficList = []

        for index in indexList:
            vehTypeList.append(self.CSVFileContent.at[index, 'VehicleType'])
            trafficList.append(self.CSVFileContent.at[index, 'Traffic'])

        for i in np.arange(5):
            tempResults.update({str(vehTypeList[i]): trafficList[i]})

        self.trafficVolumeDict['S'] = int(tempResults['31']) + int(tempResults['32'])
        self.trafficVolumeDict['L'] = int(tempResults['41']) + int(tempResults['42'])
        self.trafficVolumeDict['T'] = int(tempResults['5'])
        self.trafficVolumeDict['PCU'] = self.get_PCU_Volumes()
        self.trafficVolumeDict['volume'] = self.get_totalTrafficVolumes()
        self.trafficVolumeDict['heavy_rate'] = self.get_heavyRate()

        return self.trafficVolumeDict

    def get_trafficSpaceSpeed(self, dateTime: datetime, gantryFrom: str, gantryTo: str) -> dict:
        '''EXAMPLE:
        dateTime = %Y-%m-%d %H:%M
        Direction = 北
        '''
        tempResults = {'5': 0, '31': 0, '32': 0, '41': 0, '42': 0}  # five types of vehicles
        c1 = (self.CSVFileContent['TimeInterval'] == dateTime.strftime("%Y/%m/%d %H:%M"))
        c2 = (self.CSVFileContent['GantryFrom'] == gantryFrom)
        c3 = (self.CSVFileContent['GantryTo'] == gantryTo)
        indexList = self.CSVFileContent.index[(c1 & c2 & c3)].tolist()

        if not indexList:
            #特別修bug用
            if gantryFrom == '01F0509N' and gantryTo == '01F0467N':
                gantryFrom = '01F0511N'
                c1 = (self.CSVFileContent['TimeInterval'] == dateTime.strftime("%Y/%m/%d %H:%M"))
                c2 = (self.CSVFileContent['GantryFrom'] == gantryFrom)
                c3 = (self.CSVFileContent['GantryTo'] == gantryTo)
                indexList = self.CSVFileContent.index[(c1 & c2 & c3)].tolist()
            else:
                raise NotFindDataIndexError(inputArgs={'dateTime': dateTime.strftime("%Y-%m-%d %H:%M"),
                                                       'gantryFrom': gantryFrom,
                                                       'gantryTo': gantryTo,
                                                       'direction': direction})
        vehTypeList = []
        spaceSpeedList = []

        for index in indexList:
            vehTypeList.append(self.CSVFileContent.at[index, 'VehicleType'])
            spaceSpeedList.append(self.CSVFileContent.at[index, 'SpaceMeanSpeed'])

        for i in np.arange(5):  # five types of vehicle: 5, 31, 32, 41, 42
            tempResults.update({str(vehTypeList[i]): spaceSpeedList[i]})

        self.trafficSpeedDict['S'] = np.mean(np.array([int(tempResults['31']), int(tempResults['32'])]))
        self.trafficSpeedDict['L'] = np.mean(np.array([int(tempResults['41']), int(tempResults['42'])]))
        self.trafficSpeedDict['T'] = float(tempResults['5'])
        self.trafficSpeedDict['Avg'] = self.get_avg_SpaceSpeed()
        self.trafficSpeedDict['Median'] = self.get_median_SpaceSpeed()

        return self.trafficSpeedDict

    def get_trafficCharactersFromVD(self, dateTime: datetime, vdid: list):
        ''' (1)datetime -> %Y-%m-%d %H:%M (2) dateTime: please pass "endtime" instead of "starttime"
        (3)vdid -> [mainVD, backupVD] '''
        dateTimeInStr = dateTime.strftime("%Y/%m/%d %H:%M:%S")

        def get_constraints():
            return self.CSVFileContent['datacollecttime'] == dateTimeInStr, \
                   self.CSVFileContent['vdid'] == vdid[0], self.CSVFileContent['vdid'] == vdid[1]

        vGet_constraints = np.vectorize(get_constraints)
        c1, c2, c3 = vGet_constraints()

        def tryMainVDid():
            result = self.CSVFileContent.loc[(c1 & c2)]
            if not result.empty:
                return [True, result]
            else: #Can not find traffic volume index -> try backup vdid
                return [False]

        def tryBackupVDid():
            result = self.CSVFileContent.loc[(c1 & c3)]
            if not result.empty: #Can not find traffic volume index
                return [True, result]
            else:
                return [False]

        if tryMainVDid()[0]:
            result = tryMainVDid()
        else: #try Backup VD
            if tryBackupVDid()[0]:
                result = tryBackupVDid()
            else: #can not find data -> directly return
                print(f'Caution! Can not find data in VD file: dateTime = {dateTimeInStr}, VDID = {vdid}')
                self.reset_attributes() #reset arritbutes into 0.0
                return self.trafficVolumeDict, self.trafficSpeedDict
                #raise NotFindDataIndexError(dataType='VD', inputArgs={'dateTime': dateTimeInStr, 'VDID': vdid})

        trafficChars = result[1]

        self.trafficVolumeDict['S'] = np.sum(trafficChars[trafficChars.carid == 'S']['volume'])
        self.trafficVolumeDict['L'] = np.sum(trafficChars[trafficChars.carid == 'L']['volume'])
        self.trafficVolumeDict['T'] = np.sum(trafficChars[trafficChars.carid == 'T']['volume'])
        self.trafficVolumeDict['PCU'] = self.get_PCU_Volumes()
        self.trafficVolumeDict['volume'] = self.get_totalTrafficVolumes()
        self.trafficVolumeDict['heavy_rate'] = self.get_heavyRate()
        self.trafficVolumeDict['Occupancy'] = np.mean(trafficChars['laneoccupy'])

        self.trafficSpeedDict['S'] = np.mean(trafficChars[trafficChars.carid == 'S']['speed'])
        self.trafficSpeedDict['L'] = np.mean(trafficChars[trafficChars.carid == 'L']['speed'])
        self.trafficSpeedDict['T'] = np.mean(trafficChars[trafficChars.carid == 'T']['speed'])
        self.trafficSpeedDict['Avg'] = self.get_avg_speed()
        self.trafficSpeedDict['Median'] = self.get_median_speed()

        return self.trafficVolumeDict, self.trafficSpeedDict

    def get_trafficCharactersFromVD_new(self, vdid, dateTime: datetime):
        dateTimeInStr = dateTime.strftime("%Y/%m/%d %H:%M:%S")
        c1 = self.CSVFileContent['vd_id'] == vdid[0]
        c2 = self.CSVFileContent['vd_id'] == vdid[1]

        if c1.any():
            result = self.CSVFileContent.loc[c1]
        else:
            if c2.any():
                result = self.CSVFileContent.loc[c2]
            else:
                print(f'Caution! Can not find specific VDID: dateTime = {dateTimeInStr}, VDID = {vdid}')
                # raise NotFindDataIndexError(dataType='VD', inputArgs={'dateTime': dateTimeInStr, 'VDID': vdid})
                self.reset_attributes() #reset arritbutes into 0.0
                return self.trafficVolumeDict, self.trafficSpeedDict

        content = result["{小時:{分:{車道:[speed,laneoccupy,S_volume,T_volume,L_volume]}}}   字典提取方法:字典名稱[hr][minute][lane]=[車速,佔有率,S,T,L]"]
        index = list(dict(content).keys())[0]
        content_dict = dict(content)
        content_dict = eval(content_dict[index])

        min = dateTime.minute
        hour = dateTime.hour
        trafficChars = content_dict[hour][min]

        if trafficChars:
            # [車速,佔有率,S,T,L]
            occupancyList = []
            speedList = []
            volumeList = {'S': [], 'L': [], 'T': []}
            for lane in trafficChars:
                speedList.append(float(trafficChars[lane][0]))
                occupancyList.append(float(trafficChars[lane][1]))
                volumeList['S'].append(int(trafficChars[lane][2]))
                volumeList['T'].append(int(trafficChars[lane][3]))
                volumeList['L'].append(int(trafficChars[lane][4]))

            meanSpotSpeed = round(np.mean(speedList), 2)
            self.trafficSpeedDict['S'] = meanSpotSpeed
            self.trafficSpeedDict['L'] = meanSpotSpeed
            self.trafficSpeedDict['T'] = meanSpotSpeed
            self.trafficSpeedDict['Avg'] = self.get_avg_speed()
            self.trafficSpeedDict['Median'] = self.get_median_speed()

            self.trafficVolumeDict['Occupancy'] = round(np.mean(occupancyList), 2)
            self.trafficVolumeDict['S'] = np.sum(volumeList['S'])
            self.trafficVolumeDict['T'] = np.sum(volumeList['T'])
            self.trafficVolumeDict['L'] = np.sum(volumeList['L'])
            self.trafficVolumeDict['PCU'] = self.get_PCU_Volumes()
            self.trafficVolumeDict['volume'] = self.get_totalTrafficVolumes()
            self.trafficVolumeDict['heavy_rate'] = self.get_heavyRate()

        else:
            print(f'Caution! Can not find any data of specific VDID: dateTime = {dateTimeInStr}, VDID = {vdid}')
            self.reset_attributes()

        return self.trafficVolumeDict, self.trafficSpeedDict



