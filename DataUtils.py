import pandas as pd
import os
import numpy as np
from numpy.f2py.auxfuncs import throw_error
import yaml
from datetime import datetime
import openpyxl

from pandas.core.interchange.dataframe_protocol import DataFrame


def dataSelect(row_num):
    ## this function is to take the data partly from the LargeScaleNetworkFlow.csv, as the test example
    selectData = pd.read_csv('Dataset-LargeScaleNetworkFlow.csv', delimiter=',', nrows=row_num)
    selectData.dataframeName = 'Dataset-LargeScaleNetworkFlow.csv'
    saveAsCsv(selectData, 'selectData-' + str(row_num), os.getcwd())

def saveAsCsv(target_csv, filename, target_directory):
    saveDirectory = target_directory + '/' + filename + '.csv'
    target_csv.to_csv(saveDirectory, index=False)

def dataGetCols(originData, row_num, col_list):
    selectedRows = originData.iloc[0:row_num]
    selectedCols = selectedRows[col_list]
    saveAsCsv(selectedCols, 'constructData', os.getcwd())

def dataConstruct(originData):
    dataInDates = []
    dates = ['26/04/2017', '27/04/2017', '28/04/2017', '09/05/2017', '11/05/2017', '15/05/2017']
    for i in dates:
        dataInDates.append(
            {
                'date' : i,
                'time' : [],
                'sourceIp' : [],
                'sourcePort' : [],
                'destinationIp' : [],
                'destinationPort' : [],
                'fwdPackets' : [],
                'bwdPackets' : []
            }
        )
    for index, row in originData.iterrows():
        date = row['Timestamp'][:10]
        time = row['Timestamp'][10:]
        dataInDates[dates.index(date)].get('time').append(time)
        dataInDates[dates.index(date)].get('sourceIp').append(row['Source.IP'])
        dataInDates[dates.index(date)].get('sourcePort').append(row['Source.Port'])
        dataInDates[dates.index(date)].get('destinationIp').append(row['Destination.IP'])
        dataInDates[dates.index(date)].get('destinationPort').append(row['Destination.Port'])
        dataInDates[dates.index(date)].get('fwdPackets').append(row['Total.Fwd.Packets'])
        dataInDates[dates.index(date)].get('bwdPackets').append(row['Total.Backward.Packets'])
    for date in dates:
        filename = date.replace('/', '_') + '.yaml'
        with open(filename, 'w') as file:
            yaml.dump(dataInDates[dates.index(date)], file)


def getTimestamp(originData):
    timestampList = originData['Timestamp']
    dates = ['26/04/2017', '27/04/2017', '28/04/2017', '09/05/2017', '11/05/2017', '15/05/2017']
    time0 = []
    time1 = []
    time2 = []
    time3 = []
    time4 = []
    time5 = []
    timeDict = {
        dates[0] : time0,
        dates[1] : time1,
        dates[2] : time2,
        dates[3] : time3,
        dates[4] : time4,
        dates[5] : time5,
    }
    for timestamp in timestampList:
        date = timestamp[:10]
        time = timestamp[10:]
        if date == dates[0]:
            timeDict[dates[0]].append(time)
        elif date == dates[1]:
            timeDict[dates[1]].append(time)
        elif date == dates[2]:
            timeDict[dates[2]].append(time)
        elif date == dates[3]:
            timeDict[dates[3]].append(time)
        elif date == dates[4]:
            timeDict[dates[4]].append(time)
        elif date == dates[5]:
            timeDict[dates[5]].append(time)
        else:
            print('There is a timestamp is not recorded:' + date)
    with open('dates.yaml', 'w') as file:
        yaml.dump(timeDict, file)
    for i in range(len(timeDict)):
        print(len(timeDict.get(dates[i])))

def sortYamlByTime(yaml_filename):
    # 读取 YAML 文件
    with open(yaml_filename, 'r') as file:
        data = yaml.safe_load(file)

    # 如果存在'time'键且其值是一个列表
    if 'time' in data and isinstance(data['time'], list):
        # 创建一个包含索引和时间字符串的列表
        time_list_with_index = [(index, time_str) for index, time_str in enumerate(data['time'])]

        # 对时间字符串进行排序
        sorted_time_list_with_index = sorted(time_list_with_index, key=lambda item: item[1])

        # 根据排序后的索引重新组织其他键的值
        sorted_data = {}
        for key in data:
            if key == 'time':
                sorted_data[key] = [time_str for _, time_str in sorted_time_list_with_index]
            else:
                # 确保 data[key] 是一个列表才能使用索引访问
                if isinstance(data[key], list):
                    sorted_data[key] = [data[key][index] for index, _ in sorted_time_list_with_index]
                else:
                    sorted_data[key] = data[key]

        return sorted_data
    else:
        return data

def readYaml(fileName):
    with open(fileName, 'r') as file:
        yaml_data = yaml.safe_load(file)
    return yaml_data

def saveYaml(fileName, yamlFile):
    with open(fileName, 'w') as file:
        yaml.dump(yamlFile, file)

# 将一个date内的csv数据根据time进行切分，并保存在一个yaml文件中
def excelToYaml(excelFile, filename):
    # 创建一个三维列表timeList ArrayList<List>[ArrayList<dict>[dict{}]]来存储多个时间点下的所有源-目的IP对:
    # [
    #     [
    #         {'SourceIP': 'xxx', ... , 'DestinationIP': 'xxx'},
    #         {...},
    #     ],
    #     [...]
    # ]
    dataList = []
    timeList = []
    timeGlobeFlag = None
    timeLocalFlag = None
    for index, row in excelFile.iterrows():
        # 开始时，让timeGlobeFlag和timeLocalFlag都等于time值，在每次换行时，更新timeLocalFlag的值，不更新timeGlobeFlag，
        # 直到time值改变时再更新不更新timeGlobeFlag
        timeDict = {
            'date': '',
            'time': '',
            'sourceIp': '',
            'sourcePort': '',
            'destinationIp': '',
            'destinationPort': '',
            'fwdPackets': '',
            'bwdPackets': ''
        }
        if timeGlobeFlag is None and timeLocalFlag is None:
            timeGlobeFlag = row['time']
            timeLocalFlag = row['time']
        else:
            # 在每次换行时，更新timeLocalFlag的值
            timeLocalFlag = row['time']

        if timeLocalFlag == timeGlobeFlag:   # time没有变化，继续将一行内容转换成字典并压入timeList中
            timeDict.update([('date', row['date']), ('time', row['time']), ('sourceIp', row['Source.IP']),
                             ('sourcePort', row['Source.Port']), ('destinationIp', row['Destination.IP']),
                             ('destinationPort', row['Destination.Port']), ('fwdPackets', row['Total.Fwd.Packets']),
                             ('bwdPackets', row['Total.Backward.Packets'])])
            timeList.append(timeDict)
        else:   # time发生变化，说明time已经进入到下一段，将当前timeList压入dataList中，并清空，进行下一时间段的存储，并更新timeGlobeFlag
            timeGlobeFlag = row['time']
            dataList.append(timeList.copy())
            timeList.clear()
            timeDict.update([('date', row['date']), ('time', row['time']), ('sourceIp', row['Source.IP']),
                             ('sourcePort', row['Source.Port']), ('destinationIp', row['Destination.IP']),
                             ('destinationPort', row['Destination.Port']), ('fwdPackets', row['Total.Fwd.Packets']),
                             ('bwdPackets', row['Total.Backward.Packets'])])
            timeList.append(timeDict)
    saveYaml(filename + '-times.yaml', dataList)






def divideTimestampInExecl(excelFile, newExcelFilename):
    excelFile['date'] = excelFile['Timestamp'].str[:10]
    excelFile['time'] = excelFile['Timestamp'].str[10:]
    excelFile.drop(columns=['Timestamp'], inplace=True)
    excelFile.to_csv(newExcelFilename, index=False)

def divideExcelByDate(excelFile):
    uniqueValues = excelFile['date'].unique()
    for value in uniqueValues:
        subset_df = excelFile[excelFile['date'] == value]
        filename = value.replace('/', '_')
        output_csv = f'{filename}.csv'
        subset_df.to_csv(output_csv, index=False)






if __name__ == '__main__':

    # colList = ['Source.IP', 'Source.Port', 'Destination.IP', 'Destination.Port', 'Timestamp', 'Total.Fwd.Packets', 'Total.Backward.Packets']
    # rows, cols = data.shape
    # # 构造无序yaml
    # data = pd.read_csv('constructData.csv', delimiter=',')
    # dataConstruct(data)
    # # 构造无序yaml
    # 对yaml排序
    # yamlFileNames = ['28_04_2017.yaml', '27_04_2017.yaml', '26_04_2017.yaml', '15_05_2017.yaml', '11_05_2017.yaml', '09_05_2017.yaml']
    # for yamlFileName in yamlFileNames:
    #     sortedData = sortYamlByTime(yamlFileName)
    #     saveName = yamlFileName + '-sorted.yaml'
    #     saveYaml(saveName, sortedData)
    # 对yaml排序
    # 切分constructData，将timestamp切分为date和time
    # data = pd.read_csv('constructData.csv', delimiter=',')
    # divideTimestampInExecl(data, 'constructData-splitTimestamp.csv')
    # 切分constructData，将timestamp切分为date和time
    # 根据日期将excel切分成六份
    # data = pd.read_csv('constructData-splitTimestamp.csv', delimiter=',')
    # divideExcelByDate(data)
    # 根据日期将excel切分成六份
    # 根据time将excel划分为多个list，并存储到一个yaml中。
    csvFileNames = ['28_04_2017.csv', '27_04_2017.csv', '26_04_2017.csv', '15_05_2017.csv', '11_05_2017.csv',
                     '09_05_2017.csv']
    for csvFileName in csvFileNames:
        data = pd.read_csv(csvFileName, delimiter=',')
        excelToYaml(data, csvFileName.split('.')[0])
    # 根据time将excel划分为多个list，并存储到一个yaml中。
