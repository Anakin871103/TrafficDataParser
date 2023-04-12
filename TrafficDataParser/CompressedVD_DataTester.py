
'''
Author: Hsuan-Chih (Leo) Wang
Date: 2022/7/31
Introduction: To retrieve the content of compressed VD data with a list of user-defined input (date, vdid)
'''

from TrafficDataParser import TrafficDataParser
import os
from datetime import datetime

YEAR = '2020'
MONTH = '1'
DAY = '13'
HOUR = '12'
MIN = '40'
VDID = 'nfbVD-N1-N-86.122-M-RS'

class TrafficDataTester(TrafficDataParser):
    def get_compressedVD_content(self, vdid, dateTime: datetime):
        print(f"vdid = {vdid}, dateTime = {dateTime}")
        c1 = (self.CSVFileContent['vd_id'] == vdid)
        result = self.CSVFileContent.loc[c1]
        content = result["{小時:{分:{車道:[speed,laneoccupy,S_volume,T_volume,L_volume]}}}   字典提取方法:字典名稱[hr][minute][lane]=[車速,佔有率,S,T,L]"]
        index = list(dict(content).keys())[0]
        content_dict = dict(content)
        content_dict = eval(content_dict[index])

        min = dateTime.minute
        hour = dateTime.hour
        trafficChars = content_dict[hour][min]

        for lane in trafficChars:
            # [車速,佔有率,S,T,L]
            print(f"lane {lane}: ")
            print(f"speed: {trafficChars[lane][0]}, occupancy: {trafficChars[lane][1]}, "
                  f"S: {trafficChars[lane][2]}, T: {trafficChars[lane][3]}, L: {trafficChars[lane][4]}")


if __name__ == '__main__':
    VD_Data_Route = os.path.join('data', 'VD', 'Compress(fromNAS)', YEAR, MONTH + '月')
    VD_DataName = str(int(DAY))+'日.csv'
    # datetime(year, month, day, hour, minute, second, microsecond)
    dateTime = datetime(int(YEAR), int(MONTH), int(DAY), int(HOUR), int(MIN))

    trafficDataTester = TrafficDataTester(fileRoute=VD_Data_Route, fileName=VD_DataName, encodingType='Big5')
    trafficDataTester.get_compressedVD_content(vdid=VDID, dateTime=dateTime)


