"""
医学指南自动分类工具
功能：对医学指南文件进行智能分类、验证和修正
"""

import os
import shutil
import json
from PyPDF2 import PdfReader


class MedicalGuideClassifier:
    """医学指南分类器"""
    
    def __init__(self, source_dir, config_file=None):
        self.source_dir = source_dir
        self.result_file = os.path.join(source_dir, "classification_results.json")
        self.correction_log_file = os.path.join(source_dir, "correction_log.txt")
        
        # 加载配置文件
        if config_file and os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            self.disease_keywords = config.get("disease_keywords", {})
            self.corrections = config.get("corrections", {})
            self.settings = config.get("settings", {})
        else:
            # 默认配置
            self.disease_keywords = {
                "糖尿病": ["糖尿病", "血糖", "胰岛素", "糖代谢", "糖尿病前期", "二甲双胍"],
                "高血压": ["高血压", "血压", "难治性高血压", "妊娠期高血压"],
                "心血管疾病": ["心血管", "冠心病", "心绞痛", "心肌梗死", "心力衰竭", "心衰", 
                             "心房颤动", "房颤", "心源性卒中", "心原性休克", "心肌炎", 
                             "冠状动脉", "心源性", "川崎病"],
                "睡眠障碍": ["睡眠", "失眠", "阻塞性睡眠呼吸暂停", "发作性睡病", 
                            "快速眼动睡眠", "日间过度思睡", "睡眠呼吸"],
                "骨关节疾病": ["骨关节炎", "骨质疏松", "骨折", "强直性脊柱炎", "类风湿关节炎", "痛风"],
                "呼吸系统疾病": ["哮喘", "支气管哮喘", "慢性阻塞性肺疾病", "慢阻肺", "肺炎", "肺栓塞", "鼻炎"],
                "消化系统疾病": ["胃炎", "胃溃疡", "消化性溃疡", "肝硬化", "肝炎", "幽门螺杆菌", 
                             "胰腺炎", "胆囊炎", "消化不良", "消化道出血", "腹泻"],
                "肿瘤癌症": ["肺癌", "胃癌", "乳腺癌", "肝癌", "结直肠癌", "膀胱癌", "前列腺癌", 
                           "恶性肿瘤", "癌症", "肿瘤", "癌痛"],
                "神经系统疾病": ["帕金森", "阿尔茨海默", "痴呆", "认知障碍", "脑卒中", "中风", 
                             "不宁腿综合征", "偏头痛", "眩晕", "脑梗死"],
                "精神心理疾病": ["抑郁", "焦虑", "暴食障碍", "广泛性焦虑障碍", "考试焦虑", "精神障碍"],
                "肾脏疾病": ["肾脏病", "肾移植", "慢性肾脏病", "上尿路感染性结石"],
                "肝脏疾病": ["肝炎", "肝硬化", "脂肪肝", "药物性肝损伤", "自身免疫性肝炎"],
                "血液疾病": ["阵发性睡眠性血红蛋白尿症"],
                "内分泌疾病": ["甲状腺", "代谢综合征", "多囊卵巢综合征", "绝经", "雄激素性脱发"],
                "皮肤病": ["银屑病", "痤疮", "脱发", "日晒伤"],
                "感染性疾病": ["艾滋病", "病毒性肝炎", "流感", "流行性感冒", "新型冠状病毒"],
                "其他": ["身体活动", "久坐行为", "营养", "康复", "健康管理", "体检", 
                        "围手术期", "围产期", "妊娠期", "失能预防", "肌少症"]
            }
            
            self.corrections = {
                "2型糖尿病合并心血管疾病诊断和治疗行业标准.pdf": "心血管疾病",
                "中国成人2型糖尿病及糖尿病前期患者动脉粥样硬化性心血管疾病预防与管理专家共识（2023）.pdf": "心血管疾病",
                "糖尿病患者合并心血管疾病诊治专家共识.pdf": "心血管疾病",
                "妊娠期高血压疾病心血管风险综合管理专家共识.pdf": "心血管疾病",
                "超声检查高血压心血管重构和功能临床应用指南(2024版).pdf": "心血管疾病",
                "高血压合并冠心病患者血压管理中国专家共识.pdf": "心血管疾病",
                "高血压合并糖尿病患者心血管-肾脏-代谢综合管理专家共识（2024）.pdf": "心血管疾病",
            }
            
            self.settings = {
                "extract_pages": 5,
                "filename_weight": 10,
                "content_weight": 5,
                "save_interval": 10,
                "max_issues_display": 20
            }
    
    def extract_text_from_pdf(self, pdf_path):
        """从PDF文件中提取文本"""
        try:
            reader = PdfReader(pdf_path)
            text = ""
            extract_pages = self.settings.get("extract_pages", 5)
            max_pages = min(extract_pages, len(reader.pages))
            for i in range(max_pages):
                page = reader.pages[i]
                text += page.extract_text() + "\n"
            return text
        except Exception as e:
            print(f"读取PDF文件 {pdf_path} 时出错: {e}")
            return ""
    
    def classify_medical_guide(self, filename, text_content):
        """根据文件名和内容对医学指南进行分类"""
        filename_lower = filename.lower()
        
        best_match = "其他"
        max_score = 0
        
        filename_weight = self.settings.get("filename_weight", 10)
        content_weight = self.settings.get("content_weight", 5)
        
        for disease, keywords in self.disease_keywords.items():
            score = 0
            for keyword in keywords:
                if keyword in filename:
                    score += filename_weight
                if keyword in text_content:
                    score += content_weight
            if score > max_score:
                max_score = score
                best_match = disease
        
        # 特殊处理优先级
        if "糖尿病" in filename_lower and max_score >= 10:
            return "糖尿病"
        if "高血压" in filename_lower and max_score >= 10:
            return "高血压"
        if "睡眠" in filename_lower and max_score >= 10:
            return "睡眠障碍"
        if "心血管" in filename_lower or "心" in filename_lower:
            if max_score >= 10:
                return "心血管疾病"
        
        return best_match
    
    def classify_all_files(self):
        """对所有医学指南文件进行分类"""
        pdf_files = [f for f in os.listdir(self.source_dir) 
                    if f.endswith('.pdf') and os.path.isfile(os.path.join(self.source_dir, f))]
        
        print(f"找到 {len(pdf_files)} 个PDF文件")
        
        classification_results = {}
        
        for i, filename in enumerate(pdf_files, 1):
            print(f"\n处理文件 {i}/{len(pdf_files)}: {filename}")
            
            file_path = os.path.join(self.source_dir, filename)
            text_content = self.extract_text_from_pdf(file_path)
            disease_type = self.classify_medical_guide(filename, text_content)
            
            print(f"  分类结果: {disease_type}")
            
            classification_results[filename] = {
                "disease_type": disease_type,
                "file_path": file_path
            }
            
            save_interval = self.settings.get("save_interval", 10)
            if i % save_interval == 0:
                self._save_results(classification_results)
                print(f"  已保存中间结果 ({i} 个文件)")
        
        self._save_results(classification_results)
        print(f"\n分类完成！结果已保存到 {self.result_file}")
        return classification_results
    
    def create_folders_and_move_files(self, classification_results):
        """按疾病类型创建文件夹并移动文件"""
        disease_stats = {}
        for filename, info in classification_results.items():
            disease_type = info["disease_type"]
            disease_stats[disease_type] = disease_stats.get(disease_type, 0) + 1
        
        print("\n疾病分类统计:")
        for disease, count in sorted(disease_stats.items()):
            print(f"  {disease}: {count} 个文件")
        
        moved_count = 0
        for filename, info in classification_results.items():
            disease_type = info["disease_type"]
            source_path = info["file_path"]
            
            disease_folder = os.path.join(self.source_dir, disease_type)
            if not os.path.exists(disease_folder):
                os.makedirs(disease_folder)
                print(f"\n创建文件夹: {disease_type}")
            
            target_path = os.path.join(disease_folder, filename)
            if not os.path.exists(target_path):
                shutil.move(source_path, target_path)
                moved_count += 1
                print(f"  移动: {filename} -> {disease_type}/")
        
        print(f"\n共移动 {moved_count} 个文件")
    
    def verify_classification(self, classification_results):
        """验证分类结果"""
        print("\n=== 分类结果验证 ===")
        
        issues = []
        for filename, info in classification_results.items():
            disease_type = info["disease_type"]
            filename_lower = filename.lower()
            
            if "糖尿病" in filename_lower and disease_type != "糖尿病":
                issues.append(f"可能错误: {filename} 分类为 {disease_type}，但文件名包含'糖尿病'")
            elif "高血压" in filename_lower and disease_type != "高血压":
                issues.append(f"可能错误: {filename} 分类为 {disease_type}，但文件名包含'高血压'")
            elif "睡眠" in filename_lower and "睡眠障碍" not in disease_type:
                issues.append(f"可能错误: {filename} 分类为 {disease_type}，但文件名包含'睡眠'")
            elif "心血管" in filename_lower or "心" in filename_lower:
                if "心血管" not in disease_type and "心" not in disease_type:
                    issues.append(f"可能错误: {filename} 分类为 {disease_type}，但文件名包含'心血管'或'心'")
        
        if issues:
            print("\n发现以下可能的问题:")
            max_issues = self.settings.get("max_issues_display", 20)
            for issue in issues[:max_issues]:
                print(f"  - {issue}")
            if len(issues) > max_issues:
                print(f"  ... 还有 {len(issues) - max_issues} 个问题")
            return False
        else:
            print("\n✓ 分类结果验证通过，未发现明显错误")
            return True
    
    def apply_corrections(self):
        """应用修正"""
        print("\n=== 开始修正分类错误 ===\n")
        
        with open(self.result_file, 'r', encoding='utf-8') as f:
            classification_results = json.load(f)
        
        correction_log = []
        
        for filename, correct_disease in self.corrections.items():
            current_disease = classification_results[filename]["disease_type"]
            
            if current_disease == correct_disease:
                print(f"✓ {filename} 已经在正确的分类: {correct_disease}")
                continue
            
            print(f"\n修正: {filename}")
            print(f"  当前分类: {current_disease}")
            print(f"  正确分类: {correct_disease}")
            
            current_folder = os.path.join(self.source_dir, current_disease)
            target_folder = os.path.join(self.source_dir, correct_disease)
            
            source_path = os.path.join(current_folder, filename)
            target_path = os.path.join(target_folder, filename)
            
            if not os.path.exists(target_folder):
                os.makedirs(target_folder)
                print(f"  创建文件夹: {correct_disease}")
            
            if os.path.exists(source_path):
                shutil.move(source_path, target_path)
                print(f"  ✓ 已移动到: {correct_disease}/")
                
                classification_results[filename]["disease_type"] = correct_disease
                
                correction_log.append({
                    "filename": filename,
                    "from": current_disease,
                    "to": correct_disease
                })
            else:
                print(f"  ✗ 文件未找到: {source_path}")
        
        self._save_results(classification_results)
        
        with open(self.correction_log_file, 'w', encoding='utf-8') as f:
            f.write("=== 分类修正日志 ===\n\n")
            for log in correction_log:
                f.write(f"文件: {log['filename']}\n")
                f.write(f"  从: {log['from']}\n")
                f.write(f"  到: {log['to']}\n\n")
        
        print(f"\n=== 修正完成 ===")
        print(f"共修正 {len(correction_log)} 个文件")
        print(f"修正日志已保存到: {self.correction_log_file}")
    
    def verify_final_classification(self):
        """最终验证分类结果"""
        print("\n=== 最终验证 ===\n")
        
        disease_stats = {}
        
        for item in os.listdir(self.source_dir):
            item_path = os.path.join(self.source_dir, item)
            if os.path.isdir(item_path) and not item.startswith('.'):
                files = [f for f in os.listdir(item_path) 
                         if f.endswith('.pdf') or f.endswith('.PDF')]
                if files:
                    disease_stats[item] = len(files)
        
        print("疾病分类统计:")
        total_files = 0
        for disease, count in sorted(disease_stats.items(), key=lambda x: x[1], reverse=True):
            print(f"  {disease}: {count} 个文件")
            total_files += count
        
        print(f"\n总计: {total_files} 个文件")
        
        print("\n检查文件位置:")
        issues = []
        for disease, count in disease_stats.items():
            disease_folder = os.path.join(self.source_dir, disease)
            files = os.listdir(disease_folder)
            for filename in files:
                if filename.endswith('.pdf') or filename.endswith('.PDF'):
                    expected_path = os.path.join(disease_folder, filename)
                    if not os.path.exists(expected_path):
                        issues.append(f"文件未找到: {filename} 应该在 {disease}/")
        
        if issues:
            print("\n发现以下问题:")
            for issue in issues[:10]:
                print(f"  ✗ {issue}")
            return False
        else:
            print("✓ 所有文件都已正确放置")
            return True
    
    def _save_results(self, results):
        """保存分类结果"""
        with open(self.result_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
    
    def run_full_classification(self):
        """运行完整的分类流程"""
        print("=== 医学指南自动分类系统 ===\n")
        
        print("步骤1: 开始分类医学指南...")
        classification_results = self.classify_all_files()
        
        print("\n步骤2: 验证分类结果...")
        is_valid = self.verify_classification(classification_results)
        
        print("\n步骤3: 创建文件夹并移动文件...")
        self.create_folders_and_move_files(classification_results)
        
        if not is_valid:
            print("\n步骤4: 修正分类错误...")
            self.apply_corrections()
        
        print("\n步骤5: 最终验证...")
        self.verify_final_classification()
        
        print("\n=== 分类完成 ===")
        print(f"分类结果已保存到: {self.result_file}")


def main():
    """主函数"""
    import sys
    
    source_dir = r"E:\Datas\KY_dataset\GUIDE\All_Guide"
    config_file = None
    
    if len(sys.argv) > 1:
        source_dir = sys.argv[1]
    if len(sys.argv) > 2:
        config_file = sys.argv[2]
    
    print(f"源目录: {source_dir}")
    if config_file:
        print(f"配置文件: {config_file}")
    
    classifier = MedicalGuideClassifier(source_dir, config_file)
    classifier.run_full_classification()


if __name__ == "__main__":
    main()
