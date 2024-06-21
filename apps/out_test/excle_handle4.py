import os

import pandas as pd
import datetime


# 计算使用时长
def calculate_usage_duration(row):
    start_date = row['服务使用开始时间']
    end_date = row['服务使用终止时间']
    start = start_date.month
    end = end_date.month
    current_year = datetime.datetime.now().year
    try:
        if not pd.isna(row['服务使用终止时间']):
            if row['首次接入费是否包含当年所有费用'] == '是':
                if start_date.year == current_year - 1:
                    if start_date.month == end_date.month and start_date.day == end_date.day and end_date.year == current_year:
                        return 0
                    # 出账月份
                    date_income = calculate_date_income(row)
                    # 计费开始23-4-27 终止时间23-5-1
                    if date_income == end_date.month and end_date.year == current_year:
                        return 0
                    else:
                        return 12 - date_income + 1
                if start_date.year < current_year - 1:
                    # 2022<2024-1 2022首次接入费是否包含当年所有费用 设22年2月出全年账 23年3月开始正常出账 看结束时间 若为24年则24年1月开始正常出账到结束
                    if end_date.year > current_year:
                        return 12
                    if end_date.year == current_year:
                        if end_date.day > 1:
                            end = end_date.month + 1
                        return end
                    else:
                        return 0
            else:
                if start_date.year < current_year < end_date.year:
                    return 12
                if end_date < datetime.datetime(2024, 1, 1):
                    return 0
                if end_date.year > 2024:
                    end_date = datetime.datetime(2024, 12, 31)
                if start_date.year < 2024:
                    start_date = datetime.datetime(2024, 1, 1)
                if end_date.day > 1:
                    end = end_date.month + 1
            return end - start_date.month
    except:
        print(row['子订单编号'])


# 返回出账月份
def calculate_date_income(row):
    current_year = datetime.datetime.now().year
    start_date = row['服务使用开始时间']

    if row['首次接入费是否包含当年所有费用'] == '是':
        if row['计费信息审核时间'].year == row['计费开始时间'].year and row['计费信息审核时间'].month == row['计费开始时间'].month:
            # 次月出账
            return (row['计费开始时间'] + datetime.timedelta(days=31)).replace(day=1).month
        if row['计费信息审核时间'].year < current_year:
            if start_date.year == current_year - 1:
                return row['计费信息审核时间'].month
            else:
                return 1

    if row['计费信息审核时间'].year < current_year:
        return 1
    # 如果 confirm_time 与 start_time 同年同月，则返回 start_time 的下一个月
    if row['计费信息审核时间'].year == row['计费开始时间'].year and row['计费信息审核时间'].month == row['计费开始时间'].month:
        return (row['计费开始时间'] + datetime.timedelta(days=31)).replace(day=1).month
    # 否则，返回 confirm_time 的月份
    return row['计费信息审核时间'].replace(day=1).month


#
def calculate_income(row, month, cycle, type):
    if row['使用时长'] != 0:
        # 出账月份
        date_income = calculate_date_income(row)
        if date_income <= month < date_income + row['使用时长']:  # 添加 count < row['使用时长'] 条件
            if type == 'lg':
                if cycle == 'y':
                    return row['单价'] * row['数量'] / 12 * row['优惠折扣']
                if cycle == 'm':
                    return row['单价'] * row['数量'] * row['优惠折扣']
            else:
                if cycle == 'y':
                    return row['单价'] / 12 * row['优惠折扣']
                if cycle == 'm':
                    return row['单价'] * row['优惠折扣']

        else:
            return 0
    else:
        return 0


# 按模型计算出账情况
def calculate_monthly_income(row, month):
    current_year = datetime.datetime.now().year
    if row['计费模型'] == '打包模型' or row['计费模型'] == '包年包月模型':
        if row['计价周期'] == '年':
            # 无首次接入费用
            if pd.isna(row['首次接入费用']) is True:
                return calculate_income(row, month, 'y', '')
            elif pd.isna(row['首次接入费用']) is False:
                if row['首次接入费是否包含当年所有费用'] == '否':
                    if row['计费开始时间'].year != 2024:
                        return calculate_income(row, month, 'y', '')
                    else:
                        if row['使用时长'] != 0:
                            # 出账月份
                            date_income = calculate_date_income(row)
                            if date_income == month:
                                return (row['首次接入费用'] + row['单价'] / 12) * row['优惠折扣']
                            if date_income < month < date_income + row['使用时长']:  # 添加 count < row['使用时长'] 条件
                                return row['单价'] / 12 * row['优惠折扣']
                            else:
                                return 0
                elif row['首次接入费是否包含当年所有费用'] == '是':
                    # 是否是第一年
                    # 若不是第一年，则分为是否为本年终止两种情况，若本年终止则出账为0，反之则满一年后正常出账
                    if row['计费开始时间'].year != 2024:
                        # 出账月份
                        date_income = calculate_date_income(row)
                        if date_income >= row['服务使用终止时间'].month and row['服务使用终止时间'].year == current_year:
                            return 0
                        if month < date_income:
                            return 0
                        else:
                            return calculate_income(row, month, 'y', '')
                    else:
                        # 出账月份
                        date_income = calculate_date_income(row)
                        if date_income == month:
                            return row['首次接入费用'] * row['优惠折扣']
                        else:
                            return 0
        elif row['计价周期'] == '月':
            # 无首次接入费用
            if pd.isna(row['首次接入费用']) is True:
                return calculate_income(row, month, 'm', '')
            elif pd.isna(row['首次接入费用']) is False:
                if row['首次接入费是否包含当年所有费用'] == '否':
                    if row['计费开始时间'].year != 2024:
                        return calculate_income(row, month, 'm', '')
                    else:
                        if row['使用时长'] != 0:
                            # 出账月份
                            date_income = calculate_date_income(row)
                            if date_income == month:
                                return (row['首次接入费用'] + row['单价']) * row['优惠折扣']
                            if date_income < month < date_income + row['使用时长']:  # 添加 count < row['使用时长'] 条件
                                return row['单价'] * row['优惠折扣']
                            else:
                                return 0
                elif row['首次接入费是否包含当年所有费用'] == '是':
                    if row['计费开始时间'].year != 2024:
                        # 出账月份
                        date_income = calculate_date_income(row)
                        if month < date_income:
                            return 0
                        else:
                            return calculate_income(row, month, 'm', '')
                    else:
                        # 出账月份
                        date_income = calculate_date_income(row)
                        if date_income == month:
                            return row['首次接入费用'] * row['优惠折扣']
                        else:
                            return 0
        elif row['计价周期'] == '一次性':
            if row['计费开始时间'].year == row['计费信息审核时间'].year and row['计费开始时间'].month == row['计费信息审核时间'].month:
                if row['计费信息审核时间'].year < datetime.datetime.now().year - 1 or (
                        row['计费信息审核时间'].year == datetime.datetime.now().year - 1 and row['计费信息审核时间'].month != 12):
                    return 0
                else:
                    # 计费信息审核时间次月全额出账
                    date_income = (row['计费信息审核时间'] + datetime.timedelta(days=31)).replace(day=1).month
                    if date_income == month:
                        return row['单价'] * row['优惠折扣']
                    else:
                        return 0
            else:
                if row['计费信息审核时间'].year <= datetime.datetime.now().year - 1:
                    return 0
                else:
                    # 计费信息审核时间当月全额出账
                    date_income = row['计费信息审核时间'].month
                    if date_income == month:
                        return row['单价'] * row['优惠折扣']
                    else:
                        return 0


    elif row['计费模型'] == '交付模型':
        if row['计费开始时间'].year == row['计费信息审核时间'].year and row['计费开始时间'].month == row['计费信息审核时间'].month:
            if row['计费信息审核时间'].year < datetime.datetime.now().year - 1 or (
                    row['计费信息审核时间'].year == datetime.datetime.now().year - 1 and row['计费信息审核时间'].month != 12):
                return 0
            else:
                # 计费信息审核时间次月全额出账
                date_income = (row['计费信息审核时间'] + datetime.timedelta(days=31)).replace(day=1).month
                if date_income == month:
                    return row['单价'] * row['优惠折扣']
                else:
                    return 0
        else:
            if row['计费信息审核时间'].year <= datetime.datetime.now().year - 1:
                return 0
            else:
                # 计费信息审核时间当月全额出账
                date_income = row['计费信息审核时间'].month
                if date_income == month:
                    return row['单价'] * row['优惠折扣']
                else:
                    return 0

    # elif row['计费模型'] == '交付模型':
    #     if row['计费开始时间'].year < datetime.datetime.now().year - 1:
    #         return 0
    #     elif row['计费开始时间'].year == datetime.datetime.now().year - 1 and row['计费开始时间'].month != 12:
    #         return 0
    #     else:
    #         # 计费开始时间次月全额出账
    #         date_income = (row['计费开始时间'] + datetime.timedelta(days=31)).replace(day=1).month
    #         if date_income == month:
    #             return row['单价'] * row['优惠折扣']
    #         else:
    #             return 0

    elif row['计费模型'] == '量纲模型':
        if row['计价周期'] == '一次性':
            if row['计费信息审核时间'].year != datetime.datetime.now().year and row[
                '计费开始时间'].year != datetime.datetime.now().year:
                return 0
            else:
                date_income = calculate_date_income(row)
                if month == date_income:
                    return row['单价'] * row['数量'] * row['优惠折扣']
                else:
                    return 0

        elif row['计价周期'] == '月':
            return calculate_income(row, month, 'm', 'lg')
        elif row['计价周期'] == '年':
            return calculate_income(row, month, 'y', 'lg')
    # 其他模型直接返回0
    else:
        return 0


# 计算出账情况
# def calculate_monthly_income(row, month, ):
#     if row['使用时长'] != 0:
#         # 出账月份
#         date_income = calculate_date_income(row)
#         if date_income <= month and month < date_income + row['使用时长']:  # 添加 count < row['使用时长'] 条件
#             return row['单价'] / 12 * row['优惠折扣']
#         else:
#             return 0
#     else:
#         return 0

#
def handle_df(df, type, file_name):
    # 写入结果表
    def write_to_excel(result_df, type):
        sheet_name = '收入' if type == 'income' else '支出'
        if os.path.isfile(file_name):
            with pd.ExcelWriter(file_name, mode='a', engine='openpyxl') as writer:

                try:
                    result_df.to_excel(writer, sheet_name=sheet_name, index=False)
                except Exception as e:
                    print(f"数据写入失败: {e}")
        else:

            with pd.ExcelWriter(file_name, mode='w', engine='openpyxl') as writer:
                result_df.to_excel(writer, sheet_name=sheet_name, index=False)

    if df.empty:
        result_df = df[
            ['子订单编号', '订单编号', '计费信息审核时间', '使用方单位', '能力提供方', '计费信息状态', '优惠折扣', '能力名称', '能力编码', '计费模型', '首次接入费用',
             '首次接入费是否包含当年所有费用', '服务是否统一收取首次接入费', '单价', '数量', '服务开通情况', '服务使用开始时间', '服务使用终止时间', '计费开始时间', '计价周期']]
        # 写入结果到新Excel文件
        if type == 'income':
            write_to_excel(result_df, 'income')
        elif type == 'outlay':
            write_to_excel(result_df, 'outlay')
    else:

        # 条件五：服务使用终止时间字符串要求不为空，且大于2024年1月1号
        df['服务使用终止时间'] = pd.to_datetime(df['服务使用终止时间'], errors='coerce')
        df = df[~df['服务使用终止时间'].isnull()]
        df = df[df['服务使用终止时间'] > datetime.datetime(2024, 1, 1)]

        # 打包模型按子订单编号去重
        def remove_duplicates_based_on_condition(df):
            condition = (df['计费模型'] == '打包模型')
            df_copy = df[condition].copy()
            df_copy.drop_duplicates(subset='子订单编号', keep='first', inplace=True)
            df_filtered = pd.concat([df[~condition], df_copy])
            return df_filtered

        df = remove_duplicates_based_on_condition(df)

        df['服务使用开始时间'] = pd.to_datetime(df['服务使用开始时间'])
        df['服务使用终止时间'] = pd.to_datetime(df['服务使用终止时间'])
        df['计费开始时间'] = pd.to_datetime(df['计费开始时间'])
        df['计费信息审核时间'] = pd.to_datetime(df['计费信息审核时间'])

        # 将计算使用时长的代码应用于DataFrame
        df['使用时长'] = df.apply(calculate_usage_duration, axis=1)

        # 计算每个月收入
        for month in range(1, 13):
            column_name = f"{month}月"
            df[column_name] = df.apply(lambda x: calculate_monthly_income(x, month), axis=1)

        # 计算全年收入
        df['全年收入'] = df['1月'] + df['2月'] + df['3月'] + df['4月'] + df['5月'] + df['6月'] + df['7月'] + df['8月'] + df['9月'] + \
                     df['10月'] + df['11月'] + df['12月']

        # 选择需要的字段
        result_df = df[
            ['子订单编号', '订单编号', '计费信息审核时间', '使用方单位', '能力提供方', '计费信息状态', '优惠折扣', '能力名称', '能力编码', '计费模型', '首次接入费用',
             '首次接入费是否包含当年所有费用', '服务是否统一收取首次接入费', '单价', '数量', '服务开通情况', '服务使用开始时间', '服务使用终止时间', '计费开始时间', '计价周期'] + [
                f"{month}月" for month in
                range(1, 13)] + ['全年收入']]

        # 写入结果到新Excel文件
        if type == 'income':
            write_to_excel(result_df, 'income')
        elif type == 'outlay':
            write_to_excel(result_df, 'outlay')


def run():
    # 读取原始数据
    df = pd.read_excel('能力订购单_20240619.xls', sheet_name='订购能力服务明细', engine='xlrd')

    # 筛选条件计费模型不为空
    df = df[df['计费模型'].notnull()]

    # 读取“江苏主建能力信息（含时间和原厂商）.xlsx”中“全量能力清单”表数据
    js_capabilities_df = pd.read_excel('江苏主建能力信息（含时间和原厂商）.xlsx', sheet_name='全量能力清单', engine='openpyxl')
    # 条件一：按能力名称匹配“全量能力清单”表数据
    income_df = df[df['能力名称'].isin(js_capabilities_df['能力名称'])]
    # 条件二：计费信息字段值为“已确认”
    income_df = income_df[income_df['计费信息状态'] == '已确认']
    # 条件三：使用方单位字段不等于“江苏公司”
    income_df = income_df[income_df['使用方单位'] != '江苏公司']
    # 条件四：能力提供方字段截取前4个字符，要求前4个字符不为“江苏公司”
    income_df = income_df[income_df['能力提供方'].str[:4].eq('江苏公司')]

    #######################################################################
    # 支出
    # 使用方：江苏公司
    # 能力提供方：去除第一个是江苏公司的

    capabilities_df = pd.read_excel('全量上台能力.xlsx', sheet_name='Sheet2', engine='openpyxl')
    # 条件一：按能力名称匹配“全量能力清单”表数据
    outlay_df = df[df['能力名称'].isin(capabilities_df['能力名称'])]
    # 条件二：计费信息字段值为“已确认”
    outlay_df = outlay_df[outlay_df['计费信息状态'] == '已确认']
    # 条件三：使用方单位字段等于“江苏公司”
    outlay_df = outlay_df[outlay_df['使用方单位'] == '江苏公司']
    # 条件四：能力提供方字段截取前4个字符，要求前4个字符为“江苏公司”
    outlay_df = outlay_df[~outlay_df['能力提供方'].str[:4].eq('江苏公司')]

    # 获取当前时间的时间戳
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f'市场化结算结果表_{timestamp}.xlsx'  # 加入时间戳

    # 收入 支出
    handle_df(income_df, 'income', file_name)
    handle_df(outlay_df, 'outlay', file_name)

    # 条件五：服务使用终止时间字符串要求不为空，且大于2024年1月1号
    # income_df['服务使用终止时间'] = pd.to_datetime(income_df['服务使用终止时间'], errors='coerce')
    # income_df = income_df[~income_df['服务使用终止时间'].isnull()]
    # income_df = income_df[income_df['服务使用终止时间'] > datetime.datetime(2024, 1, 1)]

    # # 条件五：服务使用终止时间字符串要求不为空，且大于2024年1月1号
    # outlay_df['服务使用终止时间'] = pd.to_datetime(outlay_df['服务使用终止时间'], errors='coerce')
    # outlay_df = outlay_df[~income_df['服务使用终止时间'].isnull()]
    # outlay_df = outlay_df[income_df['服务使用终止时间'] > datetime.datetime(2024, 1, 1)]

    # 条件六：筛选计费模型为打包模型，按子订单编号去重，计算出每月的出账情况
    # income_df = df[df['计费模型'] == '打包模型'].copy()
    # income_df = df[(df['计费模型'] == '打包模型') & (df['计价周期'] == '年') & (df['首次接入费用'].isnull())].copy()
    # if (income_df['计费模型'] == '打包模型').any():
    #     income_df.drop_duplicates(subset='子订单编号', keep='first', inplace=True)
    # 按子订单编号去重
    # income_df.drop_duplicates(subset='子订单编号', keep='first', inplace=True)

    # 打包模型按子订单编号去重
    # def remove_duplicates_based_on_condition(df):
    #     condition = (df['计费模型'] == '打包模型')
    #     df_copy = df[condition].copy()
    #     df_copy.drop_duplicates(subset='子订单编号', keep='first', inplace=True)
    #     df_filtered = pd.concat([df[~condition], df_copy])
    #     return df_filtered
    #
    # income_df = remove_duplicates_based_on_condition(income_df)
    #
    # income_df['服务使用开始时间'] = pd.to_datetime(income_df['服务使用开始时间'])
    # income_df['服务使用终止时间'] = pd.to_datetime(income_df['服务使用终止时间'])
    # income_df['计费开始时间'] = pd.to_datetime(income_df['计费开始时间'])
    # income_df['计费信息审核时间'] = pd.to_datetime(income_df['计费信息审核时间'])

    # 将计算使用时长的代码应用于DataFrame
    # income_df['使用时长'] = income_df.apply(calculate_usage_duration, axis=1)

    # 计算每个月收入
    # for month in range(1, 13):
    #     column_name = f"{month}月"
    #     income_df[column_name] = income_df.apply(lambda x: calculate_monthly_income(x, month), axis=1)
    #
    # # 计算全年收入
    # income_df['全年收入'] = income_df['单价'] / 12 * income_df['使用时长'] * income_df['优惠折扣']
    #
    # # 选择需要的字段
    # result_df = income_df[
    #     ['子订单编号', '订单编号', '计费信息审核时间', '使用方单位', '能力提供方', '计费信息状态', '优惠折扣', '能力名称', '能力编码', '计费模型', '首次接入费用',
    #      '首次接入费是否包含当年所有费用', '服务是否统一收取首次接入费', '单价', '数量', '服务开通情况', '服务使用开始时间', '服务使用终止时间', '计费开始时间', '计价周期'] + [
    #         f"{month}月" for month in
    #         range(1, 13)] + ['全年收入']]
    #
    # # 写入结果到新Excel文件
    # result_df.to_excel('结果25.xls', sheet_name='收入', index=False)


if __name__ == "__main__":
    run()
