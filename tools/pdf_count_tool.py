"""
输入：指定文件夹路径
输出：该文件夹中pdf的总页数
"""
import os
from PyPDF2 import PdfReader


# 获取文件夹内所有pdf文件，以及打印文件数量
def GetFileInfo(path, fileType=()):
    fileList = []

    # root 表示当前正在访问的文件夹路径
    # dirs  是 list , 表示该文件夹中所有的目录的名字(不包括子目录)
    # files 是 list , 表示内容是该文件夹中所有的文件(不包括子目录)
    # (每遍历一次相当于进入下级子目录)
    for root, dirs, files in os.walk(path):
        for name in files:
            fname = os.path.join(root, name)
            if fname.endswith(fileType):
                fileList.append(fname)

    print("总共有%d个PDF文件" % fileList.__len__())
    return fileList


def compute_pdfpage(path):
    TotalPageNum = 0
    fileType = ("PDF", "pdf")
    fileList = GetFileInfo(path=path, fileType=fileType)
    for pdf in fileList:
        try:
            reader = PdfReader(pdf)
            # 获取单个文件页数
            pageNum = len(reader.pages)
            TotalPageNum += pageNum
        except Exception as e:
            print("-" * 70)
            print(pdf + "该文件出现异常，可能是权限问题")
            print(e)
            print("-" * 70)
    return TotalPageNum


if __name__ == '__main__':
    while True:
        path = input('请输入要检测的文件夹路径:\n')
        TotalPageNum = compute_pdfpage(path)
        print("总共%d页" % TotalPageNum)
