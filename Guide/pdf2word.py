from pdf2docx import Converter
import os
from pathlib import Path

# ************************ 固定路径配置（无需修改） ************************
# PDF源文件夹路径
pdf_dir = r"E:\Datas\KY_dataset\GUIDE\Processed_Guides\Guides"
# Word目标文件夹路径
word_dir = r"E:\Datas\KY_dataset\GUIDE\Processed_Guides\Guides_word"
# *************************************************************************

# 自动创建目标文件夹（若不存在）
Path(word_dir).mkdir(parents=True, exist_ok=True)

# 遍历源文件夹下所有PDF文件
for file_name in os.listdir(pdf_dir):
    # 筛选仅PDF格式文件（忽略隐藏文件/其他格式）
    if file_name.lower().endswith(".pdf"):
        # 拼接PDF文件完整路径
        pdf_file_path = os.path.join(pdf_dir, file_name)
        # 生成Word文件名（替换后缀为.docx，保留原文件名）
        word_file_name = os.path.splitext(file_name)[0] + ".docx"
        # 拼接Word文件完整保存路径
        word_file_path = os.path.join(word_dir, word_file_name)

        try:
            # 初始化转换器并执行转换
            cv = Converter(pdf_file_path)
            cv.convert(word_file_path, start=0, end=None)  # start/end：转换页码，None表示全部
            cv.close()
            print(f"✅ 转换成功：{file_name} -> {word_file_name}")
        except Exception as e:
            # 捕获异常，避免单个文件失败导致批量中断
            print(f"❌ 转换失败：{file_name}，错误信息：{str(e)[:100]}")

print("\n📌 批量转换完成！所有Word文件已保存至：", word_dir)