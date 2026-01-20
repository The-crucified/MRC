"""
测试脚本：创建测试数据并运行分类器
"""

import os
import sys
import shutil

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from medical_guide_classifier import MedicalGuideClassifier


def create_test_data():
    """创建测试数据"""
    test_dir = r"E:\KY_agent\MRC\test_data"
    
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    
    os.makedirs(test_dir)
    
    # 创建一些测试文件（空文件用于测试）
    test_files = [
        "中国糖尿病防治指南（2024版）.pdf",
        "中国高血压临床实践指南.pdf",
        "中国心力衰竭诊断和治疗指南2024.pdf",
        "中国成人失眠诊断与治疗指南（2023版）.pdf",
        "中国骨关节炎诊疗指南（2024 版）.pdf",
        "支气管哮喘防治指南（2024年版）.pdf",
        "中国胃癌筛查与早诊早治指南（2022，北京）.pdf",
        "中国帕金森疾病蓝皮书_王坚.pdf",
        "抑郁症基层诊疗指南（2021年）.pdf",
        "中国药物性肝损伤诊治指南 2023.pdf",
    ]
    
    for filename in test_files:
        file_path = os.path.join(test_dir, filename)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"测试文件: {filename}\n")
            f.write("这是一个测试用的PDF文件\n")
        
        print(f"创建测试文件: {filename}")
    
    print(f"\n测试数据已创建在: {test_dir}")
    return test_dir


def test_classifier():
    """测试分类器"""
    print("=== 开始测试医学指南分类器 ===\n")
    
    # 创建测试数据
    test_dir = create_test_data()
    
    # 使用默认配置运行分类器
    print("\n使用默认配置运行分类器...")
    classifier = MedicalGuideClassifier(test_dir)
    
    # 测试分类功能
    print("\n测试分类功能...")
    classification_results = classifier.classify_all_files()
    
    # 测试验证功能
    print("\n测试验证功能...")
    is_valid = classifier.verify_classification(classification_results)
    
    # 测试创建文件夹和移动文件功能
    print("\n测试创建文件夹和移动文件功能...")
    classifier.create_folders_and_move_files(classification_results)
    
    # 测试最终验证
    print("\n测试最终验证功能...")
    classifier.verify_final_classification()
    
    print("\n=== 测试完成 ===")
    print(f"测试数据保存在: {test_dir}")
    print("请检查测试结果是否正确")


if __name__ == "__main__":
    test_classifier()
