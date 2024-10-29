from PIL import Image, ImageDraw, ImageFont


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


# 使用示例
image_path = "截图模板.jpg"  # 输入图片路径
output_path = "截图1.jpg"  # 输出图片路径

# 要添加的文本及其对应的坐标位置，颜色和字体大小
text_positions = [
    {
        'text': "江苏公司想订购携转挡板拨测能力，能力编码:A157100031。烦请评估能否订购使用，后续将会通过中台下单，感谢。",
        'position': (10, 8),
        'font_size': 20,
        'color': "#3F464D",  # 字体颜色
        'font_path': "C:/Windows/Fonts/微软雅黑/msyhbd.ttc"  # 使用微软雅黑字体
    },
    {
        'text': "张焓",
        'position': (22, 54),
        'font_size': 15,
        'color': "black",  # 字体颜色
        'font_path': "C:/Windows/Fonts/微软雅黑/msyhbd.ttc"  # 使用黑体字体
    },

    {
        'text': "2024-06-30 10:00",
        'position': (1090, 80),
        'font_size': 15,
        'color': "#9090ab",  # 字体颜色
        'font_path': "C:/Windows/Fonts/微软雅黑/msyh.ttc"  # 使用黑体字体
    },
    {
        'text': "易晓媛",
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
        'text': "易晓媛",
        'position': (102, 149),
        'font_size': 15,
        'color': "black",  # 字体颜色
        'font_path': "C:/Windows/Fonts/微软雅黑/msyhl.ttc"  # 使用黑体字体
    },
    {
        'text': "<yixiaoyuan@zj.chinamobile.com>",
        'position': (151, 151),
        'font_size': 15,
        'color': "#9090ab",  # 字体颜色
        'font_path': "C:/Windows/Fonts/微软雅黑/msyh.ttc"  # 使用黑体字体
    },
    {
        'text': "2024年6月30日 10:00",
        'position': (102, 182),
        'font_size': 15,
        'color': "black",  # 字体颜色
        'font_path': "C:/Windows/Fonts/微软雅黑/msyhl.ttc"  # 使用黑体字体
    },
    {
        'text': "浙江公司易晓媛您好:",
        'position': (22, 254),
        'font_size': 17,
        'color': "black",  # 字体颜色
        'font_path': "C:/Windows/Fonts/微软雅黑/msyh.ttc"  # 使用黑体字体
    },
    {
        'text': "江苏公司想订购携转挡板拨测能力，能力编码:A157100031。烦清评估能否订购使用，后续将会通过中台下单，感谢。",
        'position': (98, 280),
        'font_size': 17,
        'color': "black",  # 字体颜色
        'font_path': "C:/Windows/Fonts/微软雅黑/msyh.ttc"  # 使用黑体字体
    }
]

# 调用函数并指定字体路径
add_text_to_image(image_path, output_path, text_positions)
