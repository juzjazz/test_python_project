from datetime import datetime

import xlrd
import pandas

# 替换为你的xls文件路径
file_path = '能力订购单_test.xls'
# 打开xls文件
workbook = xlrd.open_workbook(file_path)
# 获取名为“订购能力服务明细”的表
sheet = workbook.sheet_by_name('订购能力服务明细')

df = pandas.read_excel('能力订购单_test.xls', sheet_name='订购能力服务明细')
# 根据子订单编号列去重
df.drop_duplicates(subset='子订单编号', inplace=True)
# 保存去重后的数据到新的Excel文件
df.to_excel('去重后的表格.xls', index=False)

# 替换为你的xls文件路径
qc_file_path = '去重后的表格.xls'
# 打开xls文件
qc_workbook = xlrd.open_workbook(qc_file_path)
# 获取名为“订购能力服务明细”的表
qc_sheet = qc_workbook.sheet_by_name('订购能力服务明细')

output_file = '市场化结算结果.xls'

# 遍历表格中的每一行
column_1 = sheet.row_values(0).index('计费信息状态')
column_2 = sheet.row_values(0).index('使用方单位')
column_3 = sheet.row_values(0).index('能力提供方')
column_4 = sheet.row_values(0).index('服务使用终止时间')
column_5 = sheet.row_values(0).index('计费模型')
column_6 = sheet.row_values(0).index('计价周期')
column_7 = sheet.row_values(0).index('首次接入费用')

for row in range(1, sheet.nrows):
    # 获取当前行的计费信息单元格的值和“使用方”单元格的值
    cell_value = sheet.cell_value(row, column_1)
    user_value = sheet.cell_value(row, column_2)
    ability_provide_value = sheet.cell_value(row, column_3)[:4]
    type_value = sheet.cell_value(row, column_5)
    billing_cycle = sheet.cell_value(row, column_6)
    # 首次接入费用
    if sheet.cell_value(row, column_7):
        is_first_cost = sheet.cell_value(row, column_7)
    else:
        is_first_cost = 0

    if not sheet.cell_value(row, column_4):
        continue
    service_end_time = datetime.strptime(sheet.cell_value(row, column_4), '%Y-%m-%d %H:%M:%S')
    threshold = datetime(2024, 1, 1)

    # 如果计费信息单元格的值为“已完成”且“使用方”不为“江苏公司”，则输出该行数据
    if cell_value == '已确认' and user_value != '江苏公司' and ability_provide_value == '江苏公司' and service_end_time > threshold:
        if type_value == '打包模型':
            if billing_cycle == '年':
                # 无首次接入费
                if is_first_cost == 0:

                    print(sheet.row_values(row))
