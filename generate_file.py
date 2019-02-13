#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import csv
import json
import time
import socket
import struct
import datetime
import re
import os
import threading

PNT_LIST_FILE = 'pnt_info.csv'
FILE_PATH = 'd:\upload\\YHQT\\10038788\\'

# 接收端地址
# HOST = "localhost"
# PORT = 8000
HOST = "192.168.2.5"
PORT = 9010
SOCKETSERVER = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
SOCKETSERVER.bind((HOST,PORT))

# 用于存放所有的点信息
ALL_PNT = []        
# 用于存放所有点的序列关系 点编号： 点信息
ALL_PNT_INFO = {}
# 用于存放带有点值信息的数组
ALL_PNT_INFO_LIST = []

# 用于存放报警点的信息
ALARM_PNT_INFO = {}
# 用于存放报警点信息的数组
ALARM_PNT_INFO_LIST = []
# 周期内生成了报警文件
GEN_ALARM_FILE = False
# 周期内生成了故障文件
GEN_FAULT_FILE = False

# 用于存放故障点信息
FAULT_PNT_INFO = {}
# 用于存放故障点信息的数组
FAULT_PNT_INFO_LIST = []

# 存储文件的时间间隔
INTERVAL = 20
# 用于记录第一次接收到数据包的时间
T0 = None


# 实时值池
dip_pnts = {'\x00\x00\x00\x00': ['\x00', '\x00\x00\x00\x00\x00\x00\x00\x00']}


def save(data, path_name):
    '''判断、创建目录，并保存文件'''
    # 判断目录是否存在
    if not os.path.exists(os.path.split(path_name)[0]):
        # 目录不存在创建，makedirs可以创建多级目录
        try:
            os.makedirs(os.path.split(path_name)[0])
        except Exception as e:
            print('make directory failed')
    try:
        # 保存数据到文件
        with open(path_name, 'wb') as f:
            f.write(data.encode('utf8'))
        print('save successlly', path_name,time.ctime())
    except Exception as e:
        print('Save failed', e)

def get_time_str(time_stamp):
    '''生成指定的时间格式'''
    local_time = time.localtime(time_stamp)
    time_str = time.strftime('%Y%m%d%H%M%S',local_time)
    return time_str

def create_file_name(file_type, create_time):
    '''生成指定文件名称格式'''
    if file_type == 'pnt_def':
        file_name = "MNDY_" + create_time + ".txt"
    elif file_type == 'realtime_data':
        file_name = "SSSJ_" +  create_time + ".txt"
    elif file_type == 'alarm_data':
        file_name = 'BJXX_' + create_time + ".txt"
    elif file_type == 'fault_data':
        file_name = 'GZXX_' + create_time + ".txt"
    else:
        print "file type error"
    return file_name

def generate_pntdefdata(time_str):
    '''生成测点定义数据文件'''
    ALL_PNT = []        # 用于存放所有的点信息
    csv_file = csv.reader(open(PNT_LIST_FILE,'rb'))
    # 跳过表头
    next(csv_file)      
    for row in csv_file:
        if row[5] == '':            # row[5] 是上上限,如果测点没有上上限则不写入此条信息
            pnt_def = {
            "sensorID":row[0].decode('gb2312'),
            "deviceAddr":row[1].decode('gb2312'),
            "deviceName":row[2].decode('gb2312'),
            "deviceType":row[3][1:].decode('gb2312'),
            "alarmUpperLimit":float(row[4].decode('gb2312')),
            # "alarmUpperLimit2":float(row[5].decode('gb2312')),
            "unit":row[6].decode('gb2312'),
            "rangeLowerLimit":float(row[10].decode('gb2312')),
            "rangeUpperLimit":float(row[11].decode('gb2312'))}
        else:
            pnt_def = {
            "sensorID":row[0].decode('gb2312'),
            "deviceAddr":row[1].decode('gb2312'),
            "deviceName":row[2].decode('gb2312'),
            "deviceType":row[3][1:].decode('gb2312'),
            "alarmUpperLimit":float(row[4].decode('gb2312')),
            "alarmUpperLimit2":float(row[5].decode('gb2312')),
            "unit":row[6].decode('gb2312'),
            "rangeLowerLimit":float(row[10].decode('gb2312')),
            "rangeUpperLimit":float(row[11].decode('gb2312'))}
        ALL_PNT.append(pnt_def)

        # 生成实时值的池
        ALL_PNT_INFO[row[8]] ={
            'sensorID':row[0].decode('gb2312'),
            'status':'',
            'value':'',
            'realTime':''
        }
        ALL_PNT_INFO_LIST.append(ALL_PNT_INFO[row[8]])

        # 生成报警值的池
        if row[5] == '':            # row[5] 是上上限,如果测点没有上上限则不写入此条信息
            ALARM_PNT_INFO[row[8]] = {
                'sensorID':row[0].decode('gb2312'),
                'value':'',
                'alarmType':'',
                'realTime':'',
                'alarmUpperLimit':float(row[4].decode('gb2312')),
                # 'alarmUpperLimit2':float(row[5].decode('gb2312')),
                'maxValue':'',
                'maxTime':'',
                'minValue':'',
                'minTime':'',
                'alarmStartTime':None,
                'alarmEndTime':None,
                'is_in_alarm':False,
                'alarm_record':{'alarmStartTime':None,'alarmEndTime':None},
                'alarms':[],
                'values':{}
            }
        else:
            ALARM_PNT_INFO[row[8]] = {
                'sensorID':row[0].decode('gb2312'),
                'value':'',
                'alarmType':'',
                'realTime':'',
                'alarmUpperLimit':float(row[4].decode('gb2312')),
                'alarmUpperLimit2':float(row[5].decode('gb2312')),
                'maxValue':'',
                'maxTime':'',
                'minValue':'',
                'minTime':'',
                'alarmStartTime':None,
                'alarmEndTime':None,
                'is_in_alarm':False,
                'alarm_record':{'alarmStartTime':None,'alarmEndTime':None},
                'alarms':[],
                'values':{}
            }

        ALARM_PNT_INFO_LIST.append(ALARM_PNT_INFO[row[8]])

        # 生成故障值的池
        FAULT_PNT_INFO[row[8]] = {
            'sensorID':row[0].decode('gb2312'),
            'status':'',
            'realTime':'',
            'faultStartTime':None,
            'faultEndTime':None,
            'is_in_fault':False,
            'fault_record':{'faultStartTime':None,'faultEndTime':None},
            'faults':[],
        }
        FAULT_PNT_INFO_LIST.append(FAULT_PNT_INFO[row[8]])

    raw_data = json.dumps(ALL_PNT)
    file_path_name = FILE_PATH + create_file_name('pnt_def', time_str)
    save(raw_data,file_path_name)

def update_data_pool():
    # print "-------------------->>>> in update thread", FAULT_PNT_INFO_LIST
    '''更新实时数据池'''
    gen_pntdef_file = True
    while True:
        data = SOCKETSERVER.recv(2048)
        print "received data"
        gen_realtime_file = False
        gen_alarm_file = False
        gen_fault_file = False
        
        # 解析数据部分
        pnt_num = struct.unpack_from('BBBBIHHI', data)[6]       # 接收包中点的数量
        receive_time = float(struct.unpack_from('BBBBIHHI', data)[4])  # 接收到数据包的时间:到秒级
        time_str = get_time_str(receive_time)
        if gen_pntdef_file == True:
            generate_pntdefdata(time_str)
            gen_pntdef_file = False
        
        # 记录第一次接收到数据包的时间 T0
        global T0
        if T0 == None:
            T0 = receive_time

        for i in range(pnt_num):
            # key:点表号 = value: 每个数据的4-12位，即两个状态 + 真正的值 pnt_dip_num 为 DIP 发送的点编号
            pnt_dip_num = struct.unpack_from('I',data[i * 12 + 16 : i * 12 + 16 + 4])[0]        # NT+ 中的点表号
            pnt_AS = struct.unpack_from('H',data[i * 12 + 16 + 4 : i * 12 + 16 + 6])[0]
            pnt_value = struct.unpack_from('f',data[i * 12 + 16 + 8 : i * 12 + 16 + 12])[0]
            dip_pnts[pnt_dip_num] = struct.unpack_from('HHf',data[i * 12 + 16 + 4 : i * 12 + 16 + 12]) 

            # 解析测点状态 AS
            if ((pnt_AS >> 8) % 2 == 0 )  and ((pnt_AS >> 9) % 2 == 0) and ((pnt_AS >> 3) % 2 == 0 ) and ((pnt_AS >> 15) % 2 != 1):
                pnt_status = '01'          # 正常：01，测点品质好且未超过报警值且不超时
            elif ((pnt_AS >> 8) % 2 == 0 )  and ((pnt_AS >> 9) % 2 == 0) and ((pnt_AS >> 3) % 2 == 1):
                pnt_status = '02'          # 报警：02， 测点品质好且实时值达到报警阈值
            elif (((pnt_AS >> 8) % 2 == 1 )  and ((pnt_AS >> 9) % 2 == 1)) or ((pnt_AS >> 15) % 2 == 1):
                pnt_status = '10'          # 故障: 03, 测点品质为 bad 或者超时
            elif ((pnt_AS >> 8) % 2 == 0 ) and ((pnt_AS >> 9) % 2 == 1 ):
                pnt_status = '11'          # 其他：11, 测点品质为 poor
            else:
                pnt_status = "11"        # 目前测点状态暂定 4 种

            # 更新点池数据---实时数据
            pnt_info = ALL_PNT_INFO[str(pnt_dip_num)]
            pnt_info['value'] = pnt_value
            pnt_info['status'] = pnt_status
            pnt_info['realTime'] = time_str

            # 更新点池数据---报警标校数据
            pnt_alarm_info = ALARM_PNT_INFO[str(pnt_dip_num)]
            # 上次扫描周期没报警，这次扫描周期报警
            if pnt_status == '02' and pnt_alarm_info['is_in_alarm'] == False:
                print "start alarm -------> value is : " , pnt_value,pnt_info['sensorID']
                pnt_alarm_info['values'][time_str] = pnt_value
                pnt_alarm_info['value'] = pnt_value
                pnt_alarm_info['alarmType'] = pnt_status
                pnt_alarm_info['is_in_alarm'] = True
                pnt_alarm_info['alarms'].append({'alarmStartTime':time_str,'alarmEndTime':0,'value':pnt_value})

            # 上次扫描周期报警，这次扫描周期报警消除
            elif (pnt_alarm_info['is_in_alarm'] == True) and (pnt_status != '02'):
                print "cancel alarm_value------- save   --->", pnt_value,pnt_info['sensorID']
                pnt_alarm_info['values'][time_str] = pnt_value
                pnt_alarm_info['value'] = pnt_value
                pnt_alarm_info['is_in_alarm'] = False
                pnt_alarm_info['alarm_record']['alarmEndTime'] = time_str
                pnt_alarm_info['alarms'][-1]['alarmEndTime'] = time_str
                # 若在固定生成时间之间产生报警文件，则将 GEN_ALARM_FILE 置 1
                gen_alarm_file = True

            # 上次扫描周期报警，这次扫描周期依然报警，需要记录报警期间的测点值
            elif (pnt_alarm_info['is_in_alarm'] == True and pnt_status == '02'):
                print "still alarm_value-------->", pnt_value, pnt_info['sensorID']
                pnt_alarm_info['values'][time_str] = pnt_value
                pnt_alarm_info['value'] = pnt_value
            else:
                pass

            # 更新点池数据---故障信息数据
            pnt_fault_info = FAULT_PNT_INFO[str(pnt_dip_num)]
            # 上次扫描周期没故障，这次扫描周期故障
            if pnt_status == '10' and pnt_fault_info['is_in_fault'] == False:
                pnt_fault_info['status'] = pnt_status
                pnt_fault_info['is_in_fault'] = True
                pnt_fault_info['faults'].append({'faultStartTime':time_str, 'faultEndTime':0})

            # 上次扫描周期故障，这次扫描周期故障消除
            elif pnt_fault_info['is_in_fault'] == True and pnt_status != '10':
                pnt_fault_info['is_in_fault'] = False
                pnt_fault_info['faults'][-1]['faultEndTime'] = time_str
                gen_fault_file = True
            else:
                pass

        if receive_time - T0 >= INTERVAL:
            # 到达 20 秒间隔生成实时数据文件、报警文件和故障文件
            # print "-------------------------------------------------------------------deadline"
            T0 = receive_time
            gen_alarm_file = True
            gen_fault_file = True
            gen_realtime_file = True
            T0 = receive_time

        # 集中生成文件
        if gen_realtime_file == True:
            generate_realtime_file(time_str)

        # 生成报警文件
        if gen_alarm_file == True:
            generate_alarm_file(time_str)
        
        # 生成故障文件
        if gen_fault_file == True:
            generater_fault_file(time_str)

def generate_realtime_file(time):
    realtime_file_path = FILE_PATH + create_file_name('realtime_data',time)
    realtime_file_data = json.dumps(ALL_PNT_INFO_LIST)
    save(realtime_file_data, realtime_file_path)

def generate_alarm_file(time):
    alarm_file_path = FILE_PATH + create_file_name('alarm_data', time)
    alarm_file_data = []
    for item in ALARM_PNT_INFO_LIST:
        if item['alarms'] != []:
            # 报警在一个周期内发生，有 starttime 和 endtime
            if item['alarms'][-1]['alarmEndTime'] != 0 :
                delete_min_value = min(item['values'],key = item['values'].get) # 返回最小值的删去，因为最小值已经是报警取消时的值了
                del item['values'][delete_min_value]
                max_time = max(item['values'],key = item['values'].get)     # 返回 item['values']中最大值的键：时间
                max_value = item['values'][max_time]
                min_time = min(item['values'],key = item['values'].get)     # 返回 item['values']中最大值的键：时间
                min_value = item['values'][min_time]

                expect_format = {
                    'sensorID':item['sensorID'],
                    'value':item['value'],
                    'alarmType':item['alarmType'],
                    'realTime':time,
                    'alarmUpperLimit':item['alarmUpperLimit'],
                    'alarmUpperLimit2':item['alarmUpperLimit2'],
                    'maxValue':max_value,
                    'maxTime':max_time,     
                    'minValue':min_value,
                    'minTime':min_time,
                    'alarmStartTime':item['alarms'][-1]['alarmStartTime'],
                    'alarmEndTime':item['alarms'][-1]['alarmEndTime']
                }
            elif item['alarms'][-1]['alarmEndTime'] == 0 :
                max_time = max(item['values'],key = item['values'].get)     # 返回 item['values']中最大值的键：时间
                max_value = item['values'][max_time]
                min_time = min(item['values'],key = item['values'].get)     # 返回 item['values']中最大值的键：时间
                min_value = item['values'][min_time]
                expect_format = {
                    'sensorID':item['sensorID'],
                    'value':item['value'],
                    'alarmType':item['alarmType'],
                    'realTime':time,
                    'alarmUpperLimit':item['alarmUpperLimit'],
                    'alarmUpperLimit2':item['alarmUpperLimit2'],
                    'maxValue':max_value,
                    'maxTime':max_time,     
                    'minValue':min_value,
                    'minTime':min_time,
                    'alarmStartTime':item['alarms'][-1]['alarmStartTime'],
                }
            alarm_file_data.append(expect_format)

        elif item['alarms'] == []:
            pass

    if len(alarm_file_data) != 0:
        alarm_file_data = json.dumps(alarm_file_data)
        save(alarm_file_data, alarm_file_path)
    # 如果没有报警则不生成文件
    else:
        pass

    # 当本次报警消除时，清空这次报警记录的值、起止时间信
    for item in ALARM_PNT_INFO_LIST:
        if item['alarms'] != []:
            if item['alarms'][-1]['alarmEndTime'] != 0:
                item['values'] = {}
                item['alarms'] = []

def generater_fault_file(time):
    fault_file_path = FILE_PATH + create_file_name('fault_data', time)
    fault_file_data = []
    for item in FAULT_PNT_INFO_LIST:
        if item['faults'] != []:
            # 故障在一个周期内发生，且有starttime 和 endtime
            if item['faults'][-1]['faultEndTime'] != 0:
                expect_format = {
                    'sensorID':item['sensorID'],
                    'status':item['status'],
                    'realTime':time,
                    'faultStartTime':item['faults'][-1]['faultStartTime'],
                    'faultEndTime':item['faults'][-1]['faultEndTime']
                }
            elif item['faults'][-1]['faultEndTime'] == 0:
                expect_format = {
                    'sensorID':item['sensorID'],
                    'status':item['status'],
                    'realTime':time,
                    'faultStartTime':item['faults'][-1]['faultStartTime'],
                    # 'faultEndTime':item['faults'][-1]['faultEndTime']
                }
            fault_file_data.append(expect_format)

        elif item['faults'] == []:
            pass
    if len(fault_file_data) != 0:
        fault_file_data = json.dumps(fault_file_data)
        save(fault_file_data, fault_file_path)
    else:
        pass
    # 当本次故障消除时，清空这次故障记录的值、起止时间信
    for item in FAULT_PNT_INFO_LIST:
        if item['faults'] != []:
            if item['faults'][-1]['faultEndTime'] != 0:
                item['values'] = {}
                item['faults'] = []

if __name__ == "__main__":
    update_data_pool()


