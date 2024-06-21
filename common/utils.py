import os
from logging.handlers import TimedRotatingFileHandler
import logging
import time
import datetime
import os
import re
import time
import xlwt
import json
import logging
import requests

from urllib.parse import urlencode, urlparse, urlunparse, parse_qs

from retry import retry


def get_conf_dict():
    '''
    读取conf.json中字段
    :return:
    '''
    data = {}
    with open('conf.json', encoding='UTF-8') as file:
        data = json.load(file)
    return data


def str_to_dict(cookie_str):
    cookies = {}
    for line in cookie_str.split(';'):
        key, value = line.split('=', 1)
        key = key.strip()
        value = value.strip()
        cookies[key] = value
    return cookies


def get_token_from_cookie():
    """
    从cookie中获取token
    :return:
    """
    data = get_conf_dict()
    return str_to_dict(data['cookie'])['XSRF-TOKEN']


def save_file(save_file, str_data, append_flag=False, encoding="utf-8"):
    """
    保存文件
    :param save_file: 路径
    :param str_data: 数据
    :param append_flag: 是否为追加模式
    :return:
    """
    if append_flag:
        open_mode = 'a'
    else:
        open_mode = 'w'

    # 当目录不存在，创建目录
    path = os.path.dirname(save_file)
    if path and not os.path.exists(path):
        os.makedirs(path)

    with open(save_file, open_mode, encoding=encoding) as f:
        f.write(str_data)


def read_json_file(file, encoding="utf-8"):
    """
    读取json文件
    :param file:
    :param encoding:
    :return:
    """
    json_content = None
    if os.path.exists(file):
        try:
            with open(file, 'r', encoding=encoding) as f:
                json_content = json.load(f)
        except:
            logging.error(f'加载json文件{file}异常')
    return json_content


def get_source(url, use_setting_default_cookie=True, is_get=True, post_data={}, header_extra=None, cookie_str=None,
               timeout=15):
    status_code = 0
    content = None
    try:
        status_code, content = get_source_core(url, use_setting_default_cookie, is_get, post_data, header_extra,
                                               cookie_str, timeout)
    except:
        logging.exception(f'访问{url}异常')
    return status_code, content



def get_source_core(url, use_default_setting_cookie=True, is_get=True, post_data={}, header_extra=None, cookie_str=None,
                    timeout=15):
    """
    获取url对应的网页源码
    :param url:
    :param use_default_setting_cookie:是否使用settings中指定的cookie
    :return:
    """
    # 假如是get的方式，需要对参数进行urlencode
    if is_get:
        url = encode_url_params(url)

    logging.info(f'访问地址：{url}')
    # if not is_get:
    #     logging.info(f'访问POST参数：{post_data}')
    conf_dict = get_conf_dict()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.104 Safari/537.36 Core/1.53.4882.400 QQBrowser/9.7.13059.400',
    }
    if use_default_setting_cookie:
        headers.update({'Cookie': conf_dict['cookie']})
    else:
        if cookie_str:
            headers.update({'Cookie': cookie_str})

    if header_extra:
        headers.update(header_extra)

    if is_get:
        res = requests.get(url, headers=headers, timeout=timeout)
    else:
        res = requests.post(url, headers=headers, data=json.dumps(post_data), timeout=timeout)

    if res.status_code == 200:
        logging.info(f'加载URL{url},返回status_code:{res.status_code}')
        return res.status_code, res.text
    elif res.status_code == 502:
        logging.warning(f'加载URL{url},返回status_code:{res.status_code},重试')
        time.sleep(2)
        # 502错误,抛出异常,让retry来捕获重试
        raise Exception(f'加载URL{url},502错误,返回status_code:{res.status_code}')
    else:
        logging.error(f'加载URL{url},返回status_code:{res.status_code}')
        if res.status_code == 403:
            print('***错误提示***\r\n返回HTTP状态码显示位403,cookie可能过期，请尝试更新配置文件中的cookie信息，然后重启exe文件。')
        return res.status_code, None


def get_url(relative_url):
    conf_dict = get_conf_dict()
    base_url = conf_dict['base_url']
    if relative_url.startswith('.'):
        relative_url = relative_url.lstrip('.')
    return base_url + relative_url


def get_backend_url(relative_url):
    conf_dict = get_conf_dict()
    base_url = conf_dict['backend_base_url']
    if relative_url.startswith('.'):
        relative_url = relative_url.lstrip('.')
    return base_url + relative_url


def get_oa_url(relative_url):
    conf_dict = get_conf_dict()
    base_url = conf_dict['oa_base_url']
    if relative_url.startswith('.'):
        relative_url = relative_url.lstrip('.')
    return base_url + relative_url


def log(content):
    print(content)


def replace_blank(content):
    """
    去除爬虫中的乱七八糟的空格字符
    :param content:
    :return:
    """
    return re.sub('\s', ' ', content).strip()


def path_handle(path: str):
    """
    假如当前是win，处理下路径符号
    :param path:
    :return:
    """
    if os.name == 'nt':
        return path.replace('\\', '/')
    else:
        return path


def get_excel_sheet_data(title_dict, data_list):
    """
    把json数据转换成带标题的数组，用于sheet的数据
    :param title_dict:标题key和标题对应关系｛‘key’:'key_name','user_name':'用户名'｝
    :param data_list:数据数组[{},{}]
    :return:
    """
    sheet_data = []
    # 获取title数组
    title_list = []

    for key in title_dict:
        title_list.append(title_dict[key])

    # 增加标题
    sheet_data.append(title_list)
    for item in data_list:
        item_data = []
        for key in title_dict:
            item_data.append(item[key])
        sheet_data.append(item_data)
    return sheet_data


def data_2_excel(sheet_name_data, table_data, path, excel_name):
    """
    将数据导出excel
    :param sheet_name_data:
    :param table_data:
    :param excel_name:
    :return:
    """
    # 实例化一个Workbook()对象(即excel文件)
    workbook = xlwt.Workbook(encoding='utf-8', style_compression=0)
    # 字体加粗
    style = xlwt.easyxf('font: bold on')
    for key in sheet_name_data:
        # 新建一个名为Sheet1的excel sheet。此处的cell_overwrite_ok =True是为了能对同一个单元格重复操作。
        sheet = workbook.add_sheet(sheet_name_data[key], cell_overwrite_ok=True)
        # 将获取到的datetime对象仅取日期如：2016-8-9
        # today_date = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        for i in range(len(table_data[key])):
            # 对result的每个子元素作遍历，
            for j in range(len(table_data[key][i])):
                # 将每一行的每个元素按行号i,列号j,写入到excel中。
                cell_data = table_data[key][i][j]
                if i == 0:
                    sheet.write(i, j, cell_data, style)
                else:
                    # 日期格式特殊处理，防止输出到excel中异常
                    if isinstance(cell_data, datetime.datetime):
                        cell_data = cell_data.strftime('%Y-%m-%d %H:%M:%S')
                    sheet.write(i, j, cell_data)
    file_name = f'{excel_name}.xls'
    file_path = path_handle(os.path.join(path, file_name))
    if not os.path.exists(path):
        os.makedirs(path)
    workbook.save(file_path)


def get_day_str(day_delta, time_str='%Y-%m-%d'):
    """
    获取时间字符串
    :param day_delta:
    :param time_str:
    :return:
    """
    now_time = datetime.datetime.now()
    result_time = now_time + datetime.timedelta(days=day_delta)
    result_str = result_time.strftime(time_str)
    return result_str


def get_pre_month_str(month_delta=-1, time_str='%Y-%m'):
    """
    获取上个月月份字符串
    :param month_delta:
    :param time_str:
    :return:
    """
    today = datetime.date.today()
    last_month = None
    month_delta = abs(month_delta)
    # 循环获取N个月前的月份
    for i in range(month_delta):
        month_first = today.replace(day=1)
        last_month = month_first - datetime.timedelta(days=1)
        today = last_month
    return last_month.strftime(time_str)


def is_ability_code_jiangsu(code):
    """
    能力代码是否是江苏的
    :param code:
    :return:
    """
    return code[2:5] == '250'


def generate_seq():
    """
    根据时间生成seq
    :return:
    """
    return datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')


def encode_url_params(url):
    # 解析URL，获取参数部分
    parsed_url = urlparse(url)
    params = parsed_url.query

    # 如果参数部分为空，则直接返回原始URL
    if not params:
        return url

    params_dict = parse_qs(params, keep_blank_values=True)
    # 对参数部分进行urlencode编码
    encoded_params = urlencode(params_dict, doseq=True)

    # 构建编码后的URL
    encoded_url = urlunparse(parsed_url._replace(query=encoded_params))
    return encoded_url


def iniLog_everyday():
    log_dir = 'logs'
    a = os.path.exists(log_dir)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 使用strftime()函数按照指定格式生成文件名
    logfile = os.path.join(log_dir, 'logging_{}.log'.format(time.strftime('%Y_%m_%d')))
    log_format = '[%(asctime)s] [%(levelname)s] %(message)s'
    formatter = logging.Formatter(log_format)

    # 声明一个按天切分日志文件的文件处理器对象
    file_handler = TimedRotatingFileHandler(logfile, when='D', interval=1, encoding="UTF-8")
    file_handler.setFormatter(formatter)

    # 控制台handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    root_logger.setLevel(logging.INFO)
