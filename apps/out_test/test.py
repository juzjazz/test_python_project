import json
import requests

# 给定的数据
file_count = 0
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0',
    'Content-Type': 'application/vnd.ms-excel'
}

url = 'https://aaas.10086.cn/wisdom-admin/abilitySubOrder/export?'
cookie_str = 'XSRF-TOKEN=fb5138a6-d898-4d74-9fe9-1c3e7b29ff87; SESSION=ZDc4ZmY2OWUtZDZlNS00YTVhLWI3ZjAtYzE4MjFkZGZhNWMz; mobile=8578-42273-5039-36952; WT_FPC=id=212e3b48555dcbcbc271709101092514:lv=1713228310962:ss=1713227570153'
headers.update({'Cookie': cookie_str}
               )
# https://aaas.10086.cn/wisdom-admin/abilitySubOrder/export?currentPage=1&pageSize=10&subOrderId=&apiName=&status=&linkManName=&contrStatus=&appSceneInternal=&middleGroundId=&isPlatform=&providerCode=&apiProviderCode=&beginDateTime=&endDateTime=&statusUpdateTimeStart=&statusUpdateTimeEnd=
file_url = "https://aaas.10086.cn/wisdom-admin/abilitySubOrder/export?}"
response = requests.get(file_url, headers=headers, timeout=3000)
print("response.apparent_encoding", response.apparent_encoding)
print("response.encoding", response.encoding)

if response.status_code == 200:
    with open("能力订购单_20240422.xls", "wb") as file:
        file.write(response.content)
        file_count += 1
print("response.apparent_encoding", response.apparent_encoding)
print("response.encoding", response.encoding)
print(f"成功下载 {file_count} 个文件。")
