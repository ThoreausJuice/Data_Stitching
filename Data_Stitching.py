#!/usr/bin/env python3

# 批量拼接1~12月的初处理数据
# 其中“0XX”代表第一阶段A、“1XX”代表第二阶段B、“2XX”代表第三阶段C
# 本程序旨在将三阶段不同数量的数据拼成一个完整的流程

from math import floor
import pandas as pd
# 下方为自写函数导入
from Data_Processing_Function import *

# 列名设定
# 历史继承问题：由于初处理文件列名与小张处理好的数据文件列名不统一，故在此做详细说明
# 全流程共3个阶段，在此初始化将要写入的列名列表
column_name_to_be_written = []
# 时间（共3列）
# 将会写入的列名 对应 初处理文件的阶段及列号（从0开始）:
# 0:阶段A 对应 A-0
column_name_to_be_written.append('阶段A')
# 1: 阶段B 对应 B-0
column_name_to_be_written.append('阶段B')
# 2: 阶段C 对应 C-0
column_name_to_be_written.append('阶段C')
# A阶段
# 3:B线润叶加水流量 对应 A-3 (叶片B线润叶加水流量)
column_name_to_be_written.append('B线润叶加水流量')
# 4:B线润叶回风温度蒸汽阀开度 对应 A-5 (B线加料入口蒸汽阀门开度)
column_name_to_be_written.append('B线润叶回风温度蒸汽阀开度')
# 5:B线润叶注入蒸汽流量 对应 A-10 (叶片B线TBL注入蒸汽流量实际值)
column_name_to_be_written.append('B线润叶注入蒸汽流量')
# 6:B线润叶前温度仪实际值 对应 A-11 (B线润叶前温度仪实际值)
column_name_to_be_written.append('B线润叶前温度仪实际值')
# 7:B线润叶回风温度 对应 A-2 (叶片B线润叶回风温度)
column_name_to_be_written.append('B线润叶回风温度')
# 8:空列 用于给前后阶段做分割，视觉上好看
column_name_to_be_written.append('')
# B阶段
# 9:B线加料入口蒸汽阀门开度 对应 B-5 (B线加料入口蒸汽阀门开度)
column_name_to_be_written.append('B线加料入口蒸汽阀门开度')
# 10:叶片B线TBL注入蒸汽流量实际值 对应 B-10 (叶片B线TBL注入蒸汽流量实际值)
column_name_to_be_written.append('叶片B线TBL注入蒸汽流量实际值')
# 11:B线加料加水瞬时流量 对应 B-7 (B线加水瞬时流量(流量计))
column_name_to_be_written.append('B线加料加水瞬时流量')
# 12:叶片B加料机出口温度[℃] 对应 B-1 (叶片B加料机出口温度[℃])
column_name_to_be_written.append('叶片B加料机出口温度[℃]')
# 13:空列 用于给前后阶段做分割，视觉上好看
column_name_to_be_written.append('')
# C阶段
# 14:燃烧炉出口工艺气温度实际值 对应 C-9 (燃烧炉出口工艺气温度实际值)
column_name_to_be_written.append('燃烧炉出口工艺气温度实际值')
# 15:风选出口水分仪水分实际值 对应 C-6 (风选出口水分仪水分实际值)
column_name_to_be_written.append('风选出口水分仪水分实际值')

# 做一个全年数据集
full_year_dataset = []

# 12个月全处理：(不包含1月，1月是小张处理的样板)
for data_month in range(2,13):
    # 文件名
    if data_month < 10:
        a_file = '12个月份数据初处理/00' + str(data_month) + '.csv'
        b_file = '12个月份数据初处理/10' + str(data_month) + '.csv'
        c_file = '12个月份数据初处理/20' + str(data_month) + '.csv'
    else:
        a_file = '12个月份数据初处理/0' + str(data_month) + '.csv'
        b_file = '12个月份数据初处理/1' + str(data_month) + '.csv'
        c_file = '12个月份数据初处理/2' + str(data_month) + '.csv'
    
    # 初始化待写入的完整列表
    data_to_be_written = []

    a_first_processing = Basic_file_processing(a_file)

    b_first_processing = Basic_file_processing(b_file)

    c_first_processing = Basic_file_processing(c_file)

    # with open('已处理完成/1.csv', 'w', encoding='utf-8') as d:
    #     d.write(a_first_processing[0])

    # 虽然这样拼接毫无意义，但是，我就当是为了测试算法的准确性强行找了套数据好了。
    # 由于每个文件（阶段）数据的长度不一，按理说应该是把同一批次的数据整成一条数据，
    # 比如我同一批次第一阶段有2条，第二阶段5条，第三阶段3条，那么每个阶段应该取平均值，
    # 变成同一批次一二三阶段都只有一条数据，这样。
    # 但是现在就很简单的根据数量按比例拼接了。

    # 分别计算三段数据的长度
    a_lenth = len(a_first_processing) -1
    b_lenth = len(b_first_processing) -1
    c_lenth = len(c_first_processing) -1

    # 获取最小的值用作数据总量，以及其与剩余两个长度的比值，用作增量
    lenth_abc = []
    lenth_abc.append(a_lenth)
    lenth_abc.append(b_lenth)
    lenth_abc.append(c_lenth)

    lenth_min = min(lenth_abc)

    a_count = 1
    b_count = 1
    c_count = 1
    for i in range(lenth_min):
        A = a_first_processing[floor(a_count)].split(',')
        B = b_first_processing[floor(b_count)].split(',')
        C = c_first_processing[floor(c_count)].split(',')
        
        # 初始化待写入的每一行列表
        row_to_be_written = []
        
        # 拼接数据
        row_to_be_written.append(A[0])
        row_to_be_written.append(B[0])
        row_to_be_written.append(C[0])
        row_to_be_written.append(A[3])
        row_to_be_written.append(A[5])
        row_to_be_written.append(A[10])
        row_to_be_written.append(A[11])
        row_to_be_written.append(A[2])
        row_to_be_written.append('')
        row_to_be_written.append(B[5])
        row_to_be_written.append(B[10])
        row_to_be_written.append(B[7])
        row_to_be_written.append(B[1])
        row_to_be_written.append('')
        row_to_be_written.append(C[9])
        row_to_be_written.append(C[6])

        # 检测是否有"NULL"数据，如果有，变成0
        for i in range(len(row_to_be_written)):
            if row_to_be_written[i] == 'NULL':
                row_to_be_written[i] = 0

        # 将拼好的一行数据加入 待写入数据 的列表
        data_to_be_written.append(row_to_be_written)

        a_count += a_lenth / lenth_min
        b_count += b_lenth / lenth_min
        c_count += c_lenth / lenth_min

    processing_completed = pd.DataFrame(columns=column_name_to_be_written, data=data_to_be_written)
    if data_month < 10:
        done_file = '已处理完成/20210' + str(data_month) + '_done.csv'
    else:
        done_file = '已处理完成/2021' + str(data_month) + '_done.csv'
    processing_completed.to_csv(done_file, encoding='utf-8', index=False)

    full_year_dataset += data_to_be_written

    print(len(data_to_be_written))

full_year_processing_completed = pd.DataFrame(columns=column_name_to_be_written, data=full_year_dataset)
full_year_processing_completed.to_csv('已处理完成/2021_all_year_done.csv', encoding='utf-8', index=False)

print(len(full_year_processing_completed))