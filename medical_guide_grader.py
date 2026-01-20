import os
import re
import csv
from datetime import datetime
from pathlib import Path
import json

class MedicalGuideGrader:
    def __init__(self, base_path):
        self.base_path = Path(base_path)
        self.results = []
        
    def extract_year_from_filename(self, filename):
        year_match = re.search(r'(19|20)\d{2}', filename)
        if year_match:
            return int(year_match.group())
        return None
    
    def identify_authority_level(self, filename):
        high_authority_keywords = [
            '中华医学会', '中国医师协会', '国家卫生健康委', 
            '国家老年医学中心', '中国高血压防治指南修订委员会',
            '中国抗癌协会', '中国疾病预防控制中心'
        ]
        
        medium_authority_keywords = [
            '专家共识', '指南', '诊疗指南', '诊治指南',
            '管理指南', '防治指南', '临床实践指南'
        ]
        
        low_authority_keywords = [
            '专家建议', '意见', '解读', '科普', '家庭合理用药'
        ]
        
        filename_lower = filename.lower()
        
        for keyword in high_authority_keywords:
            if keyword in filename:
                return 'A级（国家级权威机构）'
        
        for keyword in medium_authority_keywords:
            if keyword in filename:
                return 'B级（专业学会/专家共识）'
        
        for keyword in low_authority_keywords:
            if keyword in filename:
                return 'C级（专家建议/意见）'
        
        return 'B级（专业学会/专家共识）'
    
    def assess_evidence_quality(self, filename):
        high_evidence_keywords = [
            '循证', '系统评价', 'meta分析', '随机对照试验',
            'GRADE', '临床实践指南', '诊疗指南', '防治指南'
        ]
        
        medium_evidence_keywords = [
            '专家共识', '诊治指南', '管理指南', '诊断与治疗指南'
        ]
        
        low_evidence_keywords = [
            '专家建议', '意见', '解读', '科普', '家庭合理用药'
        ]
        
        for keyword in high_evidence_keywords:
            if keyword in filename:
                return '高质量（基于系统评价和随机对照试验）'
        
        for keyword in medium_evidence_keywords:
            if keyword in filename:
                return '中等质量（基于专家共识和观察性研究）'
        
        for keyword in low_evidence_keywords:
            if keyword in filename:
                return '低质量（基于专家意见和案例研究）'
        
        return '中等质量（基于专家共识和观察性研究）'
    
    def assess_recurrency(self, filename, year):
        if year is None:
            return '无法评估'
        
        current_year = datetime.now().year
        age = current_year - year
        
        if age <= 2:
            return '最新（近2年）'
        elif age <= 5:
            return '较新（2-5年）'
        elif age <= 10:
            return '一般（5-10年）'
        else:
            return '陈旧（超过10年）'
    
    def determine_overall_grade(self, authority, evidence, recurrency, year):
        score = 0
        
        if 'A级' in authority:
            score += 3
        elif 'B级' in authority:
            score += 2
        elif 'C级' in authority:
            score += 1
        
        if '高质量' in evidence:
            score += 3
        elif '中等质量' in evidence:
            score += 2
        elif '低质量' in evidence:
            score += 1
        
        if year is not None:
            current_year = datetime.now().year
            age = current_year - year
            if age <= 2:
                score += 3
            elif age <= 5:
                score += 2
            elif age <= 10:
                score += 1
        
        if score >= 8:
            return '1级（最高权威）'
        elif score >= 6:
            return '2级（高质量）'
        elif score >= 4:
            return '3级（中等质量）'
        elif score >= 2:
            return '4级（一般质量）'
        else:
            return '5级（低质量）'
    
    def process_file(self, category, filename):
        year = self.extract_year_from_filename(filename)
        authority = self.identify_authority_level(filename)
        evidence = self.assess_evidence_quality(filename)
        recurrency = self.assess_recurrency(filename, year)
        overall_grade = self.determine_overall_grade(authority, evidence, recurrency, year)
        
        result = {
            '疾病分类': category,
            '文件名': filename,
            '发布年份': year if year else '未知',
            '权威性等级': authority,
            '证据质量': evidence,
            '更新时效': recurrency,
            '综合分级': overall_grade
        }
        
        return result
    
    def process_directory(self):
        for category_dir in self.base_path.iterdir():
            if category_dir.is_dir():
                category_name = category_dir.name
                
                for file in category_dir.iterdir():
                    if file.is_file() and file.suffix.lower() == '.pdf':
                        result = self.process_file(category_name, file.name)
                        self.results.append(result)
                        print(f"已处理: {category_name}/{file.name}")
    
    def save_to_csv(self, output_path):
        if not self.results:
            print("没有可保存的结果")
            return
        
        fieldnames = [
            '疾病分类', '文件名', '发布年份', '权威性等级',
            '证据质量', '更新时效', '综合分级'
        ]
        
        with open(output_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.results)
        
        print(f"结果已保存到: {output_path}")
        print(f"共处理 {len(self.results)} 个文件")

def main():
    base_path = r'E:\Datas\KY_dataset\GUIDE\All_Guide'
    output_path = r'E:\KY_agent\MRC\scripts\Category_Guide\医学指南分级结果.csv'
    
    grader = MedicalGuideGrader(base_path)
    
    print("开始处理医学指南文件...")
    grader.process_directory()
    
    print("\n保存分级结果...")
    grader.save_to_csv(output_path)
    
    print("\n处理完成!")

if __name__ == "__main__":
    main()
