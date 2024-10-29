import pandas as pd
from PIL import Image, ImageDraw, ImageFont
import os
from datetime import datetime


def add_text_to_image(image_path, output_path, text_positions):
    # 打开原始图像
    image = Image.open(image_path).convert("RGB")  # 确保图像是RGB模式
    draw = ImageDraw.Draw(image)

    # 在指定位置添加文本
    for text_info in text_positions:
        text = text_info['text']
        position = text_info['position']
        font_size = text_info['font_size']
        color = text_info['color']
        font_path = text_info['font_path']  # 取出字体路径

        # 加载指定字体
        try:
            font = ImageFont.truetype(font_path, font_size)
        except IOError:
            print(f"字体文件无法加载，请检查字体路径：{font_path}")
            return

        draw.text(position, text, fill=color, font=font)

    # 保存修改后的图像
    image.save(output_path)
    print(f"新图像已保存到 {output_path}")


def generate_images_from_excel(excel_path, image_template_path):
    # 读取 Excel 文件
    df = pd.read_excel(excel_path, engine='openpyxl')

    # 确保输出文件夹存在
    output_dir = 'output_images'
    os.makedirs(output_dir, exist_ok=True)

    # 遍历每一行数据
    for index, row in df.iterrows():
        if pd.notna(row['子订单编号']):

            order_number = row['子订单编号']  # 替换为您 Excel 文件中的相应列名
            # 将字符串转换为 datetime 对象
            date_obj = datetime.strptime(str(row['邮件发送时间']), "%Y-%m-%d %H:%M:%S")
            # 使用format方法格式化日期和时间
            formatted_date = date_obj.strftime("%Y{}%m{}%d{} %H:%M").format('年', '月', '日')
            #江苏公司计划订购贵公司“A157100031 携转挡板拨测能力”，烦清评估能否订购使用，后续将会通过中台门户下单订购，感谢。

            position_x = 151 if len(row['T4']) == 3 else 135
            position = (position_x, 151)


            text_positions = [
                {
                    'text': f"江苏公司计划订购贵公司'{row['T2']}{row['T1']}'，烦清评估能否订购使用，后续将会通过中台门户下单订购，感谢",
                    'position': (10, 8),
                    'font_size': 20,
                    'color': "#3F464D",
                    'font_path': "C:/Windows/Fonts/微软雅黑/msyhbd.ttc"
                },
                {
                    'text': "张焓",
                    'position': (22, 54),
                    'font_size': 15,
                    'color': "black",  # 字体颜色
                    'font_path': "C:/Windows/Fonts/微软雅黑/msyhbd.ttc"  # 使用黑体字体
                },

                {
                    'text': row['邮件发送时间'][:16],
                    'position': (1090, 80),
                    'font_size': 15,
                    'color': "#9090ab",  # 字体颜色
                    'font_path': "C:/Windows/Fonts/微软雅黑/msyh.ttc"  # 使用黑体字体
                },
                {
                    'text': row['T4'],
                    'position': (59, 78),
                    'font_size': 15,
                    'color': "black",  # 字体颜色
                    'font_path': "C:/Windows/Fonts/微软雅黑/msyhl.ttc"  # 使用黑体字体
                },
                {
                    'text': "张焓",
                    'position': (102, 118),
                    'font_size': 15,
                    'color': "black",  # 字体颜色
                    'font_path': "C:/Windows/Fonts/微软雅黑/msyhl.ttc"  # 使用黑体字体
                },
                {
                    'text': "<zhanghansgs@js.chinamobile.com>",
                    'position': (135, 118),
                    'font_size': 15,
                    'color': "#9090ab",  # 字体颜色
                    'font_path': "C:/Windows/Fonts/微软雅黑/msyh.ttc"  # 使用黑体字体
                },
                {
                    'text':  row['T4'],
                    'position': (102, 149),
                    'font_size': 15,
                    'color': "black",  # 字体颜色
                    'font_path': "C:/Windows/Fonts/微软雅黑/msyhl.ttc"  # 使用黑体字体
                },
                {
                    'text':  f"<{row['收件人']}>",
                    'position': position,
                    'font_size': 15,
                    'color': "#9090ab",  # 字体颜色
                    'font_path': "C:/Windows/Fonts/微软雅黑/msyh.ttc"  # 使用黑体字体
                },
                {
                    'text': formatted_date,
                    'position': (102, 182),
                    'font_size': 15,
                    'color': "black",  # 字体颜色
                    'font_path': "C:/Windows/Fonts/微软雅黑/msyhl.ttc"  # 使用黑体字体
                },
                {
                    'text': f"{row['T3']}{row['T4']}您好:",
                    'position': (22, 254),
                    'font_size': 17,
                    'color': "black",  # 字体颜色
                    'font_path': "C:/Windows/Fonts/微软雅黑/msyh.ttc"  # 使用黑体字体
                },
                {
                    'text': f"江苏公司计划订购贵公司'{row['T2']}{row['T1']}'，烦清评估能否订购使用，后续将会通过中台门户下单订购，感谢",
                    'position': (98, 280),
                    'font_size': 17,
                    'color': "black",  # 字体颜色
                    'font_path': "C:/Windows/Fonts/微软雅黑/msyh.ttc"  # 使用黑体字体
                }

            ]

            # 在 output_images 文件夹下创建一个以子订单编号命名的子文件夹
            subfolder_path = os.path.join(output_dir, order_number)
            os.makedirs(subfolder_path, exist_ok=True)

            output_file_name = f"{order_number}.jpg"  # 使用子订单编号作为文件名
            output_path = os.path.join(subfolder_path, output_file_name)

            # 生成输出文件路径
            # output_file_name = f"{order_number}.jpg"  # 使用子订单编号作为文件名
            # output_path = os.path.join(output_dir, output_file_name)

            # 生成图像
            add_text_to_image(image_template_path, output_path, text_positions)
        else:
            break


# 使用示例
excel_path = "组巡证明材料-沟通证明截图.xlsx"  # 输入您的 Excel 文件路径
image_template_path = "截图模板.jpg"  # 输入图片模板路径

# 生成图像
generate_images_from_excel(excel_path, image_template_path)
