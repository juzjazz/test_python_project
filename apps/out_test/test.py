import json
import requests

# 给定的数据
file_count = 0
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0',
    'Content-Type': 'application/vnd.ms-excel'
}

url = 'https://aaas.10086.cn/wisdom-admin/abilitySubOrder/export?'
cookie_str = 'XSRF-TOKEN=94fb7773-a6f1-4355-a723-767092bcc809; SESSION=MGFiODU0ZmItNTk3OS00YzAwLWEzMTgtNzEzYWUyMDk3NGI1; mobile=4482-62753-5048-36956; WT_FPC=id=212e3b48555dcbcbc271709101092514:lv=1718777779713:ss=1718777720765'
headers.update({'Cookie': cookie_str}
               )
# https://aaas.10086.cn/wisdom-admin/abilitySubOrder/export?currentPage=1&pageSize=10&subOrderId=&apiName=&status=&linkManName=&contrStatus=&appSceneInternal=&middleGroundId=&isPlatform=&providerCode=&apiProviderCode=&beginDateTime=&endDateTime=&statusUpdateTimeStart=&statusUpdateTimeEnd=
file_url = "https://aaas.10086.cn/wisdom-admin/abilitySubOrder/export?}"
response = requests.get(file_url, headers=headers, timeout=3000)
print("response.apparent_encoding", response.apparent_encoding)
print("response.encoding", response.encoding)

if response.status_code == 200:
    with open("能力订购单_20240619.xls", "wb") as file:
        file.write(response.content)
        file_count += 1
print("response.apparent_encoding", response.apparent_encoding)
print("response.encoding", response.encoding)
print(f"成功下载 {file_count} 个文件。")
