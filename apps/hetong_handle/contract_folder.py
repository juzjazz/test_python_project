import os
import shutil
import zipfile
import pandas as pd
import re


def get_valid_folder_path(prompt_message, check_contents=False):
    """获取有效的文件夹路径，如果路径无效或为空则提示重新输入"""
    while True:
        folder_path = input(prompt_message)
        if not os.path.exists(folder_path):
            print("路径不存在，请确认后重新输入。")
        elif not os.path.isdir(folder_path):
            print("输入的路径不是文件夹路径，请重新输入一个文件夹路径。")
        elif check_contents and not any(os.listdir(folder_path)):
            print("文件夹中未找到任何合同文件或文件夹，请确认路径是否正确并重新输入。")
        else:
            return folder_path  # 返回有效的文件夹路径


def get_valid_file_path(prompt_message):
    """获取有效的文件路径，如果路径无效则提示重新输入"""
    while True:
        file_path = input(prompt_message)
        if not os.path.exists(file_path):
            print("文件路径不存在，请确认后重新输入。")
        elif not os.path.isfile(file_path):
            print("输入的路径不是文件路径，请重新输入一个文件路径。")
        else:
            return file_path  # 返回有效的文件路径


# 获取合同文件总文件夹和能力供应链总文件夹路径
contract_folder = get_valid_folder_path("请输入上报合同总文件夹路径: ", check_contents=True)
capability_folder = get_valid_folder_path("请输入上传扫描件总文件夹路径: ")
excel_file = get_valid_file_path("请输入Excel文件路径: ")

print("所有路径有效，正在处理，请稍等...")

# contract_folder = "D:\\wechatDownload\\WeChat Files\\wxid_t64k8xvksp7c22\\FileStorage\\File\\2024-11\\上报合同\\上报合同"
# capability_folder = "D:\\wechatDownload\\WeChat Files\\wxid_t64k8xvksp7c22\\FileStorage\File\\2024-11\\上传扫描件\上传扫描件"
# excel_file = "D:\\wechatDownload\\WeChat Files\\wxid_t64k8xvksp7c22\\FileStorage\\File\\2024-11\\order_模版10月-1.xlsx"

# 读取Excel文件，假设合同编码列为 "合同编码" ，能力名列为 "能力名"
# 使用 openpyxl 引擎读取 .xlsx 文件，并将合同编码和能力名转换为字符串
df = pd.read_excel(excel_file, engine='openpyxl')
df['合同编码'] = df['合同编码'].astype(str)
df['能力'] = df['能力'].astype(str)

# 创建一个新的文件夹来存储打包后的文件
output_folder = 'output_zips'
os.makedirs(output_folder, exist_ok=True)




def remove_quotes(text):
    return re.sub(r'[^\w\u4e00-\u9fff]', '', text)


# 遍历每一个合同编码
for contract_code in df['合同编码'].unique():
    # 为每个合同编码创建一个文件夹
    contract_dir = os.path.join(output_folder, contract_code)
    os.makedirs(contract_dir, exist_ok=True)

    # 1. 在合同文件总文件夹中找到合同编码对应的文件或文件夹并复制到新建文件夹
    found = False  # 标记是否找到匹配项
    for root, dirs, files in os.walk(contract_folder):
        # 检查文件夹
        for dir_name in dirs:
            if contract_code in dir_name:
                shutil.copytree(os.path.join(root, dir_name), os.path.join(contract_dir, dir_name))
                found = True
                break  # 找到匹配的文件夹后退出循环

        if found:
            break  # 若已找到匹配文件夹，结束合同文件总文件夹的遍历

        # 检查文件
        for file_name in files:
            if contract_code in file_name.split('.')[0]:
                shutil.copy(os.path.join(root, file_name), os.path.join(contract_dir, file_name))
                found = True
                break  # 找到匹配的文件后退出循环

        if found:
            break  # 若已找到匹配文件，结束合同文件总文件夹的遍历

    # 2. 在能力供应链总文件夹中找到合同编码对应的能力名文件并复制到新建文件夹
    # for ability_name in df[df['合同编码'] == contract_code]['能力'].unique():
    #     for root, dirs, files in os.walk(capability_folder):
    #         for file in files:
    #             if remove_quotes(ability_name) in remove_quotes(file.split('.')[0]) or file == contract_code:
    #                 # 复制文件到新建文件夹
    #                 shutil.copy(os.path.join(root, file), os.path.join(contract_dir, file))
    # 2. 在能力供应链总文件夹中找到与能力名相同的文件或与合同编码匹配的文件夹并复制到新建文件夹
    for ability_name in df[df['合同编码'] == contract_code]['能力'].unique():
        clean_ability_name = remove_quotes(ability_name)
        # 查找与能力名匹配的文件
        # for root, dirs, files in os.walk(capability_folder):
        #     for file_name in files:
        #         if clean_ability_name in remove_quotes(file_name.split('.')[0]):
        #             shutil.copy(os.path.join(root, file_name), os.path.join(contract_dir, file_name))
        #             break
        # 遍历能力供应链文件夹，查找与能力名匹配的文件
        for root, dirs, files in os.walk(capability_folder):
            for file_name in files:
                # 预处理文件名，去除特殊字符后进行比较
                processed_file_name = remove_quotes(file_name.split('.')[0])

                # 检查文件名是否包含 clean_ability_name，并确保目标文件夹中不存在同名文件
                if clean_ability_name in processed_file_name:
                    # 构建目标路径，并预处理目标路径文件名用于相似性判断
                    target_path = os.path.join(contract_dir, file_name)
                    target_name = remove_quotes(os.path.splitext(file_name)[0])

                    # 如果 contract_dir 中已存在同名（相似）文件，则跳过复制
                    if any(target_name == remove_quotes(existing_file.split('.')[0]) for existing_file in
                           os.listdir(contract_dir)):
                        continue

                    # 否则，进行文件复制
                    shutil.copy(os.path.join(root, file_name), target_path)
                    break  # 找到一个匹配的文件后跳出循环

                    # 否则，进行文件复制
                    shutil.copy(os.path.join(root, file_name), target_path)
                    break  # 找到一个匹配的文件后跳出循环

    for root, dirs, files in os.walk(capability_folder):
        # 查找与合同编码匹配的文件夹
        for dir_name in dirs:
            if contract_code in dir_name:
                # 设置初始目标文件夹名称
                target_dir = os.path.join(contract_dir, dir_name)

                # 如果文件夹已存在，添加 "_copy" 后缀，直到找到一个不存在的文件夹名称
                while os.path.exists(target_dir):
                    target_dir = os.path.join(contract_dir, f"{dir_name}_扫描件")

                # 复制文件夹
                shutil.copytree(os.path.join(root, dir_name), target_dir)
                break

        # 查找与合同编码匹配的压缩包文件
        for file_name in files:
            if contract_code in file_name.split('.')[0] and file_name.endswith(('.zip', '.rar')):
                shutil.copy(os.path.join(root, file_name), os.path.join(contract_dir, file_name))
                break

        # 查找与能力名匹配的文件
        # for file_name in files:
        #     if clean_ability_name in remove_quotes(file_name.split('.')[0]):
        #         shutil.copy(os.path.join(root, file_name), os.path.join(contract_dir, file_name))
        #         break
        break

    # 3. 将合同编码对应的文件夹打成zip包
    zip_file = os.path.join(output_folder, f"{contract_code}.zip")
    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(contract_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, contract_dir)
                zipf.write(file_path, arcname)

    # 移除原始文件夹，仅保留压缩包
    shutil.rmtree(contract_dir)

print("所有合同文件处理完成，ZIP文件已生成。")
