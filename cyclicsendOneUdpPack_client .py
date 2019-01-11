#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# 按照 DIP 方式组包发送数据（UDP协议）
# 在组包时需要注意按照点的类型（AP、DP、GP）等，根据不同点的类型打包数值

import socket
import csv
import struct
import time
import math

# DataBaseIp = ("47.105.128.23",9010)
DataBaseIp = ("localhost",8000)

sockClient = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
csv_file = csv.reader(open('dip_pnt.csv','r'))

# 数据包头部分
domainNum = 1
dropNum = 1
byteSeq = 1
packType =1
timestamp = math.modf(time.time())
packTimeSec = timestamp[1]
pakcTimeMil = timestamp[0]
pntNum = 1
fileVersion = 1


headPack = struct.pack('B',domainNum) + struct.pack('B',dropNum) + struct.pack('B',byteSeq) + struct.pack('B',packType) + struct.pack('I',packTimeSec) + struct.pack('H',pakcTimeMil) + struct.pack('H',pntNum) + struct.pack('I',fileVersion)
# print 'headPack',len(headPack)
# pntInfo = []
# print csv_file

# 用于测试写 ap 类型点 
pnt_value = 1321.123
pnt_info =  struct.pack('I',1) + struct.pack('H',5102) + struct.pack('H',1) + struct.pack('f',pnt_value)

# # 用于测试写 dp 类型点 
# pnt_value = 0
# pnt_info =  struct.pack('I',1001) + struct.pack('H',125) + struct.pack('H',1) + struct.pack('i',0)

# 数据区的格式: 点表号 4字节 + 状态字1(unsigned short) 2字节 + 状态字2(unsigned short) 2字节 + 数值 4字节
# pntInfo.append(pnt_info)

full_package = headPack + pnt_info
# print "full_package",full_package
while True:
    sockClient.sendto(full_package, DataBaseIp)
    print "sent"
    time.sleep(1)
# sockClient.sendto(full_package,("192.168.31.34",8000))
