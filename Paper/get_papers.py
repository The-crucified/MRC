"""
PubMed 多疾病类型论文批量下载与循证医学分级系统 - 优化版
===============================================================
功能特点：
1. 支持19种疾病类型分类检索
2. 每类确保100篇论文（已存在+新下载）
3. 本地已存在的论文自动跳过下载但计入统计
4. 专门搜索有免费全文的论文
5. 循证医学证据等级分级
6. 保存完整摘要到CSV
7. 按疾病类型分文件夹存储
8. 断点续传支持

作者：AI Assistant
日期：2024
"""

import os
import re
import time
import json
import csv
import logging
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, asdict, field
from pathlib import Path
import requests
import xml.etree.ElementTree as ET

# ===================== 配置参数 =====================
CONFIG = {
    "email": "thecrucified001@gmail.com",
    "base_dir": r"E:\Datas\KY_dataset\GUIDE\All_Paper_0121",
    "papers_per_category": 200,  # 每个类别需要的论文数（已存在+新下载）
    "request_delay": 0.35,  # 请求间隔（秒）
    "max_retries": 3,  # 最大重试次数
    "timeout": 120,  # 下载超时时间（秒）
}

# 疾病类型及其英文检索词（新增19类）
DISEASE_CATEGORIES = {
    "妇科": {
        "en_name": "Gynecology",
        "search_terms": [
            "gynecology",
            "gynecological disease",
            "endometriosis",
            "uterine fibroids",
            "polycystic ovary syndrome",
            "ovarian cyst",
            "cervicitis",
            "vaginitis",
            "menstrual disorder",
            "pelvic inflammatory disease",
        ]
    },
    "产科": {
        "en_name": "Obstetrics",
        "search_terms": [
            "obstetrics",
            "pregnancy",
            "prenatal care",
            "gestational diabetes",
            "preeclampsia",
            "cesarean section",
            "preterm birth",
            "postpartum",
            "fetal development",
            "maternal health",
        ]
    },
    "流感": {
        "en_name": "Influenza",
        "search_terms": [
            "influenza",
            "flu virus",
            "influenza treatment",
            "influenza vaccine",
            "seasonal flu",
            "H1N1",
            "influenza prevention",
            "antiviral influenza",
            "influenza pandemic",
        ]
    },
    "肛肠科": {
        "en_name": "Anorectal_Disease",
        "search_terms": [
            "hemorrhoids",
            "anal fissure",
            "anal fistula",
            "rectal prolapse",
            "anorectal disease",
            "proctology",
            "perianal abscess",
            "colorectal surgery",
            "anal disorder",
        ]
    },
    "脑血管": {
        "en_name": "Cerebrovascular",
        "search_terms": [
            "cerebrovascular disease",
            "stroke",
            "cerebral infarction",
            "cerebral hemorrhage",
            "transient ischemic attack",
            "carotid stenosis",
            "intracranial aneurysm",
            "cerebral ischemia",
            "brain vascular",
        ]
    },
    "神经衰弱": {
        "en_name": "Neurasthenia",
        "search_terms": [
            "neurasthenia",
            "chronic fatigue syndrome",
            "nervous exhaustion",
            "mental fatigue",
            "nervous debility",
            "fatigue disorder",
            "psychasthenia",
            "nervous breakdown",
            "burnout syndrome",
        ]
    },
    "颈椎病": {
        "en_name": "Cervical_Spondylosis",
        "search_terms": [
            "cervical spondylosis",
            "cervical disc disease",
            "neck pain",
            "cervical radiculopathy",
            "cervical myelopathy",
            "cervical spine disorder",
            "cervical disc herniation",
            "cervical degenerative",
            "cervical vertebra",
        ]
    },
    "腰肌劳损": {
        "en_name": "Lumbar_Muscle_Strain",
        "search_terms": [
            "lumbar muscle strain",
            "lower back pain",
            "chronic low back pain",
            "lumbar strain",
            "back muscle injury",
            "lumbago",
            "myofascial pain lumbar",
            "lumbar sprain",
            "paraspinal muscle",
        ]
    },
    "肩周炎": {
        "en_name": "Frozen_Shoulder",
        "search_terms": [
            "frozen shoulder",
            "adhesive capsulitis",
            "periarthritis shoulder",
            "shoulder pain",
            "shoulder stiffness",
            "rotator cuff",
            "shoulder impingement",
            "shoulder arthritis",
            "glenohumeral",
        ]
    },
    "腰椎间盘突出": {
        "en_name": "Lumbar_Disc_Herniation",
        "search_terms": [
            "lumbar disc herniation",
            "herniated disc",
            "lumbar disc prolapse",
            "sciatica disc",
            "intervertebral disc",
            "disc degeneration lumbar",
            "spinal disc herniation",
            "lumbar radiculopathy",
            "discectomy",
        ]
    },
    "痛风": {
        "en_name": "Gout",
        "search_terms": [
            "gout",
            "gouty arthritis",
            "hyperuricemia",
            "uric acid",
            "gout treatment",
            "gout flare",
            "tophaceous gout",
            "urate crystal",
            "gout management",
        ]
    },
    "牙科": {
        "en_name": "Dentistry",
        "search_terms": [
            "dental disease",
            "periodontitis",
            "dental caries",
            "tooth decay",
            "gingivitis",
            "oral health",
            "dental treatment",
            "endodontics",
            "dental implant",
            "toothache",
        ]
    },
    "便秘": {
        "en_name": "Constipation",
        "search_terms": [
            "constipation",
            "chronic constipation",
            "functional constipation",
            "bowel movement disorder",
            "laxative",
            "constipation treatment",
            "irritable bowel constipation",
            "fecal impaction",
            "colonic transit",
        ]
    },
    "咽炎": {
        "en_name": "Pharyngitis",
        "search_terms": [
            "pharyngitis",
            "sore throat",
            "chronic pharyngitis",
            "acute pharyngitis",
            "streptococcal pharyngitis",
            "throat infection",
            "tonsillitis",
            "upper respiratory infection",
            "throat inflammation",
        ]
    },
    "鼻炎": {
        "en_name": "Rhinitis",
        "search_terms": [
            "rhinitis",
            "allergic rhinitis",
            "chronic rhinitis",
            "nasal congestion",
            "sinusitis",
            "rhinosinusitis",
            "nasal inflammation",
            "hay fever",
            "vasomotor rhinitis",
        ]
    },
    "溃疡": {
        "en_name": "Ulcer",
        "search_terms": [
            "peptic ulcer",
            "gastric ulcer",
            "duodenal ulcer",
            "stomach ulcer",
            "ulcer treatment",
            "helicobacter pylori ulcer",
            "ulcer healing",
            "gastrointestinal ulcer",
            "ulcer disease",
        ]
    },
    "腹泻": {
        "en_name": "Diarrhea",
        "search_terms": [
            "diarrhea",
            "acute diarrhea",
            "chronic diarrhea",
            "infectious diarrhea",
            "diarrhea treatment",
            "gastroenteritis",
            "traveler diarrhea",
            "antibiotic diarrhea",
            "watery stool",
        ]
    },
    "静脉曲张": {
        "en_name": "Varicose_Veins",
        "search_terms": [
            "varicose veins",
            "venous insufficiency",
            "chronic venous disease",
            "varicose vein treatment",
            "venous reflux",
            "leg veins",
            "venous ulcer",
            "sclerotherapy",
            "endovenous ablation",
        ]
    },
    "坐骨神经": {
        "en_name": "Sciatica",
        "search_terms": [
            "sciatica",
            "sciatic nerve pain",
            "lumbar radiculopathy",
            "sciatic neuralgia",
            "piriformis syndrome",
            "nerve root compression",
            "leg pain radiating",
            "sciatic nerve treatment",
            "radicular pain",
        ]
    },
}
# ==================================================


@dataclass
class PaperInfo:
    """论文信息数据类"""
    # 基本信息
    pmid: str = ""
    pmc_id: str = ""
    doi: str = ""
    title: str = ""
    authors: str = ""
    journal: str = ""
    year: str = ""
    month: str = ""
    
    # 分类信息
    disease_category_cn: str = ""
    disease_category_en: str = ""
    
    # 内容信息
    abstract: str = ""
    keywords: str = ""
    publication_type: str = ""
    
    # 研究特征
    is_meta_analysis: bool = False
    is_systematic_review: bool = False
    is_rct: bool = False
    is_cohort: bool = False
    is_case_control: bool = False
    is_guideline: bool = False
    is_review: bool = False
    study_type: str = ""
    sample_size: int = 0
    
    # 质量评估
    impact_factor: float = 0.0
    citation_count: int = 0
    evidence_level: str = ""
    evidence_grade: str = ""
    quality_score: float = 0.0
    
    # 下载状态
    is_free_fulltext: bool = False
    download_status: str = "pending"  # pending, success, exists, failed
    filename: str = ""
    download_source: str = ""
    file_size: int = 0
    local_path: str = ""


class EvidenceLevelClassifier:
    """循证医学证据等级分类器"""
    
    JOURNAL_IF = {
        # 顶级综合医学期刊
        "new england journal of medicine": 176.079,
        "lancet": 168.9,
        "jama": 120.7,
        "bmj": 93.6,
        "nature medicine": 82.9,
        "nature": 69.5,
        "science": 63.7,
        "cell": 66.8,
        "annals of internal medicine": 51.6,
        
        # 各专科顶级期刊
        "lancet oncology": 51.1,
        "lancet neurology": 48.0,
        "lancet infectious diseases": 36.4,
        "lancet respiratory medicine": 38.0,
        "lancet gastroenterology hepatology": 35.0,
        "lancet diabetes endocrinology": 44.8,
        "lancet psychiatry": 30.8,
        
        "jama internal medicine": 39.4,
        "jama oncology": 33.0,
        "jama neurology": 29.0,
        "jama surgery": 16.9,
        "jama psychiatry": 25.9,
        "jama dermatology": 11.8,
        "jama pediatrics": 16.2,
        
        # 专科期刊
        "gastroenterology": 33.8,
        "gut": 31.8,
        "hepatology": 17.4,
        "circulation": 37.8,
        "european heart journal": 39.3,
        "stroke": 10.2,
        "neurology": 12.3,
        "brain": 15.3,
        "pain": 7.9,
        "spine": 3.5,
        "journal of bone and joint surgery": 5.3,
        "arthritis rheumatology": 15.9,
        "annals of the rheumatic diseases": 27.9,
        "fertility and sterility": 7.3,
        "obstetrics gynecology": 7.2,
        "american journal of obstetrics gynecology": 9.8,
        "journal of dental research": 7.6,
        "journal of periodontology": 6.2,
        
        # 开放获取期刊
        "plos one": 3.7,
        "plos medicine": 15.9,
        "bmc medicine": 11.1,
        "scientific reports": 4.9,
        "frontiers": 3.5,
        "bmc": 3.0,
    }
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def classify(self, paper: PaperInfo) -> Tuple[str, str, float]:
        """对论文进行循证医学分级"""
        text = f"{paper.title} {paper.abstract} {paper.publication_type}".lower()
        
        # 识别研究类型
        study_type = self._identify_study_type(text)
        paper.study_type = study_type
        
        # 更新标志
        paper.is_meta_analysis = study_type == "meta_analysis"
        paper.is_systematic_review = study_type == "systematic_review"
        paper.is_rct = study_type == "rct"
        paper.is_cohort = study_type == "cohort"
        paper.is_case_control = study_type == "case_control"
        paper.is_guideline = study_type == "guideline"
        paper.is_review = study_type == "review"
        
        # 确定证据等级
        evidence_level = self._determine_evidence_level(study_type)
        
        # 获取影响因子
        paper.impact_factor = self._get_impact_factor(paper.journal.lower())
        
        # 提取样本量
        paper.sample_size = self._extract_sample_size(paper.abstract)
        
        # 计算质量分数
        quality_score = self._calculate_quality_score(paper, study_type)
        
        # 确定GRADE分级
        evidence_grade = self._determine_grade(quality_score)
        
        return evidence_level, evidence_grade, quality_score
    
    def _identify_study_type(self, text: str) -> str:
        """识别研究类型"""
        patterns = {
            "meta_analysis": [
                r"meta-analysis", r"meta analysis", r"metaanalysis",
                r"systematic review and meta", r"pooled analysis"
            ],
            "systematic_review": [
                r"systematic review", r"cochrane review", r"umbrella review"
            ],
            "guideline": [
                r"guideline", r"clinical practice guideline", r"consensus statement",
                r"position statement", r"recommendation"
            ],
            "rct": [
                r"randomized controlled trial", r"randomised controlled trial",
                r"\brct\b", r"randomized trial", r"randomised trial",
                r"double-blind", r"placebo-controlled", r"controlled clinical trial"
            ],
            "cohort": [
                r"cohort study", r"prospective study", r"longitudinal study",
                r"follow-up study", r"prospective cohort"
            ],
            "case_control": [
                r"case-control", r"case control study"
            ],
            "cross_sectional": [
                r"cross-sectional", r"cross sectional study"
            ],
            "case_report": [
                r"case report", r"case series"
            ],
            "review": [
                r"\breview\b", r"narrative review", r"literature review"
            ],
        }
        
        priority = [
            "meta_analysis", "systematic_review", "guideline", "rct",
            "cohort", "case_control", "cross_sectional", "case_report", "review"
        ]
        
        for study_type in priority:
            if study_type in patterns:
                for pattern in patterns[study_type]:
                    if re.search(pattern, text, re.IGNORECASE):
                        return study_type
        
        return "other"
    
    def _determine_evidence_level(self, study_type: str) -> str:
        """确定证据等级"""
        mapping = {
            "meta_analysis": "1a",
            "systematic_review": "1a",
            "guideline": "1a",
            "rct": "1b",
            "cohort": "2b",
            "case_control": "3b",
            "cross_sectional": "4",
            "case_report": "4",
            "review": "5",
            "other": "5"
        }
        return mapping.get(study_type, "5")
    
    def _get_impact_factor(self, journal: str) -> float:
        """获取期刊影响因子"""
        journal = journal.lower().strip()
        
        for name, if_val in self.JOURNAL_IF.items():
            if name in journal or journal in name:
                return if_val
        
        # 估算
        if "lancet" in journal:
            return 30.0
        elif "nature" in journal:
            return 25.0
        elif "jama" in journal:
            return 20.0
        elif "bmj" in journal:
            return 15.0
        elif "frontiers" in journal:
            return 3.5
        elif "plos" in journal:
            return 4.0
        elif "bmc" in journal:
            return 3.0
        
        return 2.0
    
    def _extract_sample_size(self, abstract: str) -> int:
        """提取样本量"""
        if not abstract:
            return 0
        
        patterns = [
            r"n\s*=\s*(\d+(?:,\d+)?)",
            r"(\d+(?:,\d+)?)\s*(?:patients|participants|subjects|individuals|cases|women|men|adults|children)",
            r"(?:included|enrolled|recruited|analyzed|analysed|studied)\s*(\d+(?:,\d+)?)",
            r"(?:total of|sample of)\s*(\d+(?:,\d+)?)",
        ]
        
        max_size = 0
        for pattern in patterns:
            matches = re.findall(pattern, abstract, re.IGNORECASE)
            for match in matches:
                try:
                    size = int(match.replace(",", "").replace(" ", ""))
                    if 10 < size < 50000000:
                        max_size = max(max_size, size)
                except:
                    continue
        
        return max_size
    
    def _calculate_quality_score(self, paper: PaperInfo, study_type: str) -> float:
        """计算质量分数 (0-100)"""
        score = 0.0
        
        # 研究类型 (35分)
        type_scores = {
            "meta_analysis": 35, "systematic_review": 33, "guideline": 32,
            "rct": 28, "cohort": 22, "case_control": 18, "cross_sectional": 15,
            "review": 12, "case_report": 8, "other": 6
        }
        score += type_scores.get(study_type, 6)
        
        # 期刊影响因子 (20分)
        if_score = paper.impact_factor
        if if_score >= 50:
            score += 20
        elif if_score >= 30:
            score += 18
        elif if_score >= 20:
            score += 16
        elif if_score >= 10:
            score += 13
        elif if_score >= 5:
            score += 10
        elif if_score >= 3:
            score += 7
        else:
            score += 4
        
        # 发表年份 (15分)
        try:
            year = int(paper.year) if paper.year else 2020
            years_ago = datetime.now().year - year
            if years_ago <= 1:
                score += 15
            elif years_ago <= 2:
                score += 13
            elif years_ago <= 3:
                score += 11
            elif years_ago <= 5:
                score += 9
            elif years_ago <= 10:
                score += 6
            else:
                score += 3
        except:
            score += 5
        
        # 样本量 (15分)
        sample = paper.sample_size
        if sample >= 50000:
            score += 15
        elif sample >= 10000:
            score += 13
        elif sample >= 1000:
            score += 11
        elif sample >= 500:
            score += 9
        elif sample >= 100:
            score += 6
        elif sample > 0:
            score += 3
        else:
            score += 2
        
        # 摘要质量 (10分)
        if paper.abstract:
            abstract_len = len(paper.abstract)
            if abstract_len >= 2000:
                score += 10
            elif abstract_len >= 1500:
                score += 8
            elif abstract_len >= 1000:
                score += 6
            elif abstract_len >= 500:
                score += 4
            else:
                score += 2
        
        # PMC收录 (5分)
        if paper.pmc_id or paper.is_free_fulltext:
            score += 5
        
        return round(score, 1)
    
    def _determine_grade(self, score: float) -> str:
        """确定GRADE分级"""
        if score >= 75:
            return "A"
        elif score >= 55:
            return "B"
        elif score >= 35:
            return "C"
        else:
            return "D"


class MultiDiseasePubMedDownloader:
    """多疾病类型PubMed论文下载器"""
    
    def __init__(self, config: dict):
        self.email = config["email"]
        self.base_dir = config["base_dir"]
        self.papers_per_category = config.get("papers_per_category", 100)
        self.delay = config.get("request_delay", 0.35)
        self.max_retries = config.get("max_retries", 3)
        self.timeout = config.get("timeout", 120)
        
        # 创建基础目录
        os.makedirs(self.base_dir, exist_ok=True)
        
        # HTTP会话
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
        
        # 分类器
        self.classifier = EvidenceLevelClassifier()
        
        # 设置日志
        self._setup_logging()
        
        # 全局已处理的PMID（防止跨类别重复）
        self.global_processed_pmids: Set[str] = set()
        
        # 全局统计
        self.global_stats = {
            "total_papers": 0,
            "total_new_downloaded": 0,
            "total_existing": 0,
            "total_failed": 0,
            "by_category": {}
        }
    
    def _setup_logging(self):
        """设置日志"""
        log_file = os.path.join(self.base_dir, "download_log.txt")
        
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8', mode='a'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def scan_existing_papers(self, category_dir: str) -> Dict[str, str]:
        """
        扫描本地已存在的论文
        
        Returns:
            {pmid: filepath} 字典
        """
        existing = {}
        
        if not os.path.exists(category_dir):
            return existing
        
        for filename in os.listdir(category_dir):
            if filename.endswith('.pdf'):
                filepath = os.path.join(category_dir, filename)
                
                # 检查文件是否有效（大于5KB）
                if os.path.getsize(filepath) > 5000:
                    # 从文件名提取PMID
                    match = re.search(r'PMID(\d+)', filename)
                    if match:
                        pmid = match.group(1)
                        existing[pmid] = filepath
        
        return existing
    
    def load_existing_csv(self, category_dir: str, category_en: str) -> List[PaperInfo]:
        """
        加载已存在的CSV数据
        """
        csv_path = os.path.join(category_dir, f"{category_en}_papers.csv")
        papers = []
        
        if not os.path.exists(csv_path):
            return papers
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    paper = PaperInfo(
                        pmid=row.get("PMID", ""),
                        pmc_id=row.get("PMC_ID", ""),
                        doi=row.get("DOI", ""),
                        title=row.get("标题", ""),
                        authors=row.get("作者", ""),
                        journal=row.get("期刊", ""),
                        year=row.get("年份", ""),
                        month=row.get("月份", ""),
                        disease_category_cn=row.get("疾病类型_中文", ""),
                        disease_category_en=row.get("疾病类型_英文", ""),
                        abstract=row.get("完整摘要", ""),
                        keywords=row.get("关键词", ""),
                        publication_type=row.get("发表类型", ""),
                        study_type=row.get("研究类型", ""),
                        evidence_level=row.get("证据等级", ""),
                        evidence_grade=row.get("GRADE分级", ""),
                        download_status="exists",
                        filename=row.get("文件名", ""),
                    )
                    
                    # 解析质量分数
                    try:
                        paper.quality_score = float(row.get("质量分数", "0"))
                    except:
                        paper.quality_score = 0.0
                    
                    # 解析影响因子
                    try:
                        paper.impact_factor = float(row.get("预估影响因子", "0"))
                    except:
                        paper.impact_factor = 0.0
                    
                    if paper.pmid:
                        papers.append(paper)
                        
        except Exception as e:
            self.logger.warning(f"  加载已有CSV失败: {e}")
        
        return papers
    
    def search_pubmed_free_fulltext(self, search_terms: List[str], 
                                     max_results: int,
                                     exclude_pmids: Set[str]) -> List[str]:
        """搜索PubMed中有免费全文的论文"""
        all_pmids = []
        
        for term in search_terms:
            if len(all_pmids) >= max_results:
                break
            
            # 构建搜索查询（带免费全文过滤）
            query = f'({term}) AND "free full text"[filter]'
            
            try:
                url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
                params = {
                    "db": "pubmed",
                    "term": query,
                    "retmax": min(80, max_results - len(all_pmids) + 50),
                    "retmode": "json",
                    "sort": "relevance",
                    "email": self.email
                }
                
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    pmids = data.get("esearchresult", {}).get("idlist", [])
                    
                    # 过滤已有的和全局已处理的
                    for pmid in pmids:
                        if pmid not in exclude_pmids and pmid not in self.global_processed_pmids:
                            all_pmids.append(pmid)
                
                time.sleep(self.delay)
                
            except Exception as e:
                self.logger.debug(f"    搜索'{term}'出错: {e}")
        
        return list(dict.fromkeys(all_pmids))[:max_results]  # 去重保持顺序
    
    def search_pmc(self, search_terms: List[str], max_results: int, 
                   exclude_pmids: Set[str]) -> List[str]:
        """搜索PMC数据库"""
        all_pmcids = []
        
        for term in search_terms:
            if len(all_pmcids) >= max_results:
                break
            
            try:
                url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
                params = {
                    "db": "pmc",
                    "term": f'({term}) AND "open access"[filter]',
                    "retmax": min(60, max_results - len(all_pmcids) + 30),
                    "retmode": "json",
                    "sort": "relevance",
                    "email": self.email
                }
                
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    pmcids = data.get("esearchresult", {}).get("idlist", [])
                    all_pmcids.extend(pmcids)
                
                time.sleep(self.delay)
                
            except Exception as e:
                self.logger.debug(f"    搜索PMC'{term}'出错: {e}")
        
        return list(set(all_pmcids))[:max_results]
    
    def fetch_paper_details(self, pmids: List[str], category_cn: str, 
                            category_en: str) -> List[PaperInfo]:
        """获取论文详细信息"""
        papers = []
        batch_size = 50
        
        for i in range(0, len(pmids), batch_size):
            batch = pmids[i:i+batch_size]
            
            try:
                url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
                params = {
                    "db": "pubmed",
                    "id": ",".join(batch),
                    "rettype": "xml",
                    "retmode": "xml",
                    "email": self.email
                }
                
                response = self.session.get(url, params=params, timeout=60)
                
                if response.status_code == 200:
                    batch_papers = self._parse_pubmed_xml(
                        response.text, category_cn, category_en
                    )
                    papers.extend(batch_papers)
                
                time.sleep(self.delay)
                
            except Exception as e:
                self.logger.error(f"    获取详情失败: {e}")
        
        return papers
    
    def _parse_pubmed_xml(self, xml_text: str, category_cn: str, 
                          category_en: str) -> List[PaperInfo]:
        """解析PubMed XML"""
        papers = []
        
        try:
            root = ET.fromstring(xml_text)
            
            for article in root.findall('.//PubmedArticle'):
                paper = self._parse_single_article(article, category_cn, category_en)
                if paper and paper.pmid:
                    papers.append(paper)
                    
        except Exception as e:
            self.logger.debug(f"    XML解析失败: {e}")
        
        return papers
    
    def _parse_single_article(self, article, category_cn: str, 
                               category_en: str) -> Optional[PaperInfo]:
        """解析单篇文章"""
        try:
            # PMID
            pmid_elem = article.find('.//PMID')
            pmid = pmid_elem.text if pmid_elem is not None else ""
            
            if not pmid:
                return None
            
            # 标题
            title_elem = article.find('.//ArticleTitle')
            title = ""
            if title_elem is not None:
                title = "".join(title_elem.itertext())
            
            # 作者
            authors = []
            for author in article.findall('.//Author')[:6]:
                lastname = author.find('LastName')
                initials = author.find('Initials')
                if lastname is not None and lastname.text:
                    name = lastname.text
                    if initials is not None and initials.text:
                        name += f" {initials.text}"
                    authors.append(name)
            authors_str = ", ".join(authors)
            if len(article.findall('.//Author')) > 6:
                authors_str += " et al."
            
            # 期刊
            journal_elem = article.find('.//Journal/Title')
            journal = journal_elem.text if journal_elem is not None else ""
            
            # 年份和月份
            year = ""
            month = ""
            year_elem = article.find('.//PubDate/Year')
            if year_elem is not None:
                year = year_elem.text
            month_elem = article.find('.//PubDate/Month')
            if month_elem is not None:
                month = month_elem.text
            
            if not year:
                medline_date = article.find('.//PubDate/MedlineDate')
                if medline_date is not None and medline_date.text:
                    year_match = re.search(r'(\d{4})', medline_date.text)
                    if year_match:
                        year = year_match.group(1)
            
            # 完整摘要
            abstract_parts = []
            for abstract_text in article.findall('.//AbstractText'):
                label = abstract_text.get('Label', '')
                text = "".join(abstract_text.itertext())
                if text:
                    if label:
                        abstract_parts.append(f"【{label}】{text}")
                    else:
                        abstract_parts.append(text)
            abstract = " ".join(abstract_parts)
            
            # DOI
            doi = ""
            for article_id in article.findall('.//ArticleId'):
                if article_id.get('IdType') == 'doi':
                    doi = article_id.text or ""
                    break
            if not doi:
                for eloc in article.findall('.//ELocationID'):
                    if eloc.get('EIdType') == 'doi':
                        doi = eloc.text or ""
                        break
            
            # PMC ID
            pmc_id = ""
            for article_id in article.findall('.//ArticleId'):
                if article_id.get('IdType') == 'pmc':
                    pmc_id = article_id.text or ""
                    break
            
            # 发表类型
            pub_types = []
            for pt in article.findall('.//PublicationType'):
                if pt.text:
                    pub_types.append(pt.text)
            
            # 关键词
            keywords = []
            for kw in article.findall('.//Keyword'):
                if kw.text:
                    keywords.append(kw.text)
            for mesh in article.findall('.//MeshHeading/DescriptorName'):
                if mesh.text:
                    keywords.append(mesh.text)
            
            paper = PaperInfo(
                pmid=pmid,
                pmc_id=pmc_id,
                doi=doi,
                title=title.strip(),
                authors=authors_str,
                journal=journal,
                year=year,
                month=month,
                disease_category_cn=category_cn,
                disease_category_en=category_en,
                abstract=abstract.strip(),
                keywords="; ".join(keywords[:15]),
                publication_type="; ".join(pub_types),
                is_free_fulltext=bool(pmc_id),
            )
            
            return paper
            
        except Exception as e:
            return None
    
    def download_paper(self, paper: PaperInfo, save_dir: str) -> bool:
        """下载单篇论文"""
        filename = self._generate_filename(paper)
        filepath = os.path.join(save_dir, filename)
        
        # 检查是否已存在
        if os.path.exists(filepath) and os.path.getsize(filepath) > 5000:
            paper.download_status = "exists"
            paper.filename = filename
            paper.file_size = os.path.getsize(filepath)
            paper.local_path = filepath
            return True
        
        # 方法1: PMC下载
        if paper.pmc_id:
            success = self._download_from_pmc(paper.pmc_id, filepath)
            if success:
                paper.download_status = "success"
                paper.filename = filename
                paper.download_source = "PMC"
                paper.file_size = os.path.getsize(filepath)
                paper.local_path = filepath
                return True
        
        # 方法2: PMID转PMCID
        if not paper.pmc_id:
            pmc_id = self._pmid_to_pmcid(paper.pmid)
            if pmc_id:
                paper.pmc_id = pmc_id
                success = self._download_from_pmc(pmc_id, filepath)
                if success:
                    paper.download_status = "success"
                    paper.filename = filename
                    paper.download_source = "PMC"
                    paper.file_size = os.path.getsize(filepath)
                    paper.local_path = filepath
                    return True
        
        # 方法3: Europe PMC
        if paper.pmc_id:
            success = self._download_from_europepmc(paper.pmc_id, filepath)
            if success:
                paper.download_status = "success"
                paper.filename = filename
                paper.download_source = "EuropePMC"
                paper.file_size = os.path.getsize(filepath)
                paper.local_path = filepath
                return True
        
        # 方法4: Unpaywall
        if paper.doi:
            success = self._download_from_unpaywall(paper.doi, filepath)
            if success:
                paper.download_status = "success"
                paper.filename = filename
                paper.download_source = "Unpaywall"
                paper.file_size = os.path.getsize(filepath)
                paper.local_path = filepath
                return True
        
        paper.download_status = "failed"
        return False
    
    def _generate_filename(self, paper: PaperInfo) -> str:
        """生成安全的文件名"""
        title = re.sub(r'[<>:"/\\|?*\n\r\t\[\]{}()\'"]', '', paper.title)
        title = re.sub(r'\s+', '_', title.strip())[:45]
        
        identifier = paper.pmid or paper.pmc_id or hashlib.md5(paper.title.encode()).hexdigest()[:8]
        year = paper.year or "XXXX"
        
        return f"PMID{identifier}_{year}_{title}.pdf"
    
    def _pmid_to_pmcid(self, pmid: str) -> Optional[str]:
        """PMID转PMCID"""
        try:
            url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
            params = {"ids": pmid, "format": "json", "email": self.email}
            
            response = self.session.get(url, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json()
                records = data.get("records", [])
                if records and "pmcid" in records[0]:
                    return records[0]["pmcid"]
        except:
            pass
        return None
    
    def _pmcid_to_pmid(self, pmc_id: str) -> Optional[str]:
        """PMCID转PMID"""
        try:
            if not pmc_id.startswith("PMC"):
                pmc_id = f"PMC{pmc_id}"
            
            url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
            params = {"ids": pmc_id, "format": "json", "email": self.email}
            
            response = self.session.get(url, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json()
                records = data.get("records", [])
                if records and "pmid" in records[0]:
                    return records[0]["pmid"]
        except:
            pass
        return None
    
    def _download_from_pmc(self, pmc_id: str, filepath: str) -> bool:
        """从PMC下载PDF"""
        if not pmc_id.startswith("PMC"):
            pmc_id = f"PMC{pmc_id}"
        
        urls = [
            f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc_id}/pdf/",
            f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc_id}/pdf/main.pdf",
        ]
        
        for url in urls:
            for retry in range(self.max_retries):
                try:
                    response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
                    
                    if response.status_code == 200:
                        content = response.content
                        if len(content) > 5000 and (content[:4] == b'%PDF' or b'%PDF' in content[:1024]):
                            with open(filepath, 'wb') as f:
                                f.write(content)
                            return True
                    break
                except requests.exceptions.Timeout:
                    if retry < self.max_retries - 1:
                        time.sleep(2)
                    continue
                except Exception:
                    break
        
        # 尝试从页面提取PDF链接
        try:
            page_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc_id}/"
            response = self.session.get(page_url, timeout=30)
            
            if response.status_code == 200:
                patterns = [
                    r'href="(/pmc/articles/PMC\d+/pdf/[^"]+\.pdf)"',
                    r'href="([^"]+\.pdf)"[^>]*class="[^"]*pdf',
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, response.text, re.IGNORECASE)
                    if match:
                        pdf_path = match.group(1)
                        if not pdf_path.startswith('http'):
                            pdf_url = f"https://www.ncbi.nlm.nih.gov{pdf_path}"
                        else:
                            pdf_url = pdf_path
                        
                        pdf_response = self.session.get(pdf_url, timeout=self.timeout)
                        if pdf_response.status_code == 200:
                            content = pdf_response.content
                            if len(content) > 5000 and b'%PDF' in content[:1024]:
                                with open(filepath, 'wb') as f:
                                    f.write(content)
                                return True
        except:
            pass
        
        return False
    
    def _download_from_europepmc(self, pmc_id: str, filepath: str) -> bool:
        """从Europe PMC下载"""
        if not pmc_id.startswith("PMC"):
            pmc_id = f"PMC{pmc_id}"
        
        try:
            url = f"https://europepmc.org/articles/{pmc_id}?pdf=render"
            response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            
            if response.status_code == 200:
                content = response.content
                if len(content) > 5000 and b'%PDF' in content[:1024]:
                    with open(filepath, 'wb') as f:
                        f.write(content)
                    return True
        except:
            pass
        
        return False
    
    def _download_from_unpaywall(self, doi: str, filepath: str) -> bool:
        """从Unpaywall下载"""
        try:
            url = f"https://api.unpaywall.org/v2/{doi}"
            params = {"email": self.email}
            
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get("is_oa"):
                    oa_locations = data.get("oa_locations", [])
                    
                    for location in oa_locations:
                        pdf_url = location.get("url_for_pdf")
                        if pdf_url:
                            try:
                                pdf_response = self.session.get(
                                    pdf_url, timeout=self.timeout, allow_redirects=True
                                )
                                
                                if pdf_response.status_code == 200:
                                    content = pdf_response.content
                                    if len(content) > 5000 and b'%PDF' in content[:1024]:
                                        with open(filepath, 'wb') as f:
                                            f.write(content)
                                        return True
                            except:
                                continue
        except:
            pass
        
        return False
    
    def process_category(self, category_cn: str, category_info: dict) -> Tuple[List[PaperInfo], dict]:
        """处理单个疾病类别"""
        category_en = category_info["en_name"]
        search_terms = category_info["search_terms"]
        
        # 创建类别文件夹
        category_dir = os.path.join(self.base_dir, category_en)
        os.makedirs(category_dir, exist_ok=True)
        
        self.logger.info(f"\n{'='*70}")
        self.logger.info(f"处理类别: {category_cn} ({category_en})")
        self.logger.info(f"{'='*70}")
        
        # 统计
        stats = {
            "category_cn": category_cn,
            "category_en": category_en,
            "target": self.papers_per_category,
            "existing_valid": 0,
            "new_downloaded": 0,
            "failed": 0,
            "total": 0,
            "pmc": 0,
            "europepmc": 0,
            "unpaywall": 0
        }
        
        # 1. 扫描本地已存在的PDF
        self.logger.info(f"  [1/5] 扫描本地已有论文...")
        existing_pdfs = self.scan_existing_papers(category_dir)
        self.logger.info(f"       找到 {len(existing_pdfs)} 个已有PDF文件")
        
        # 2. 加载已有CSV数据
        self.logger.info(f"  [2/5] 加载已有CSV数据...")
        existing_papers = self.load_existing_csv(category_dir, category_en)
        self.logger.info(f"       找到 {len(existing_papers)} 条已有记录")
        
        # 匹配已有PDF和CSV数据
        final_papers = []
        existing_pmids = set()
        
        for paper in existing_papers:
            if paper.pmid in existing_pdfs:
                paper.download_status = "exists"
                paper.local_path = existing_pdfs[paper.pmid]
                paper.filename = os.path.basename(existing_pdfs[paper.pmid])
                paper.file_size = os.path.getsize(existing_pdfs[paper.pmid])
                final_papers.append(paper)
                existing_pmids.add(paper.pmid)
                self.global_processed_pmids.add(paper.pmid)
        
        # 检查CSV中没有但PDF存在的
        for pmid, filepath in existing_pdfs.items():
            if pmid not in existing_pmids:
                existing_pmids.add(pmid)
                self.global_processed_pmids.add(pmid)
        
        stats["existing_valid"] = len(final_papers)
        self.logger.info(f"       有效已存在论文: {len(final_papers)} 篇")
        
        # 3. 计算还需要多少篇
        need_count = self.papers_per_category - len(final_papers)
        
        if need_count <= 0:
            self.logger.info(f"  ✓ 已有 {len(final_papers)} 篇，达到目标 {self.papers_per_category} 篇")
            stats["total"] = len(final_papers)
            return final_papers[:self.papers_per_category], stats
        
        self.logger.info(f"  [3/5] 需要再获取 {need_count} 篇新论文...")
        
        # 4. 搜索新论文
        search_count = need_count + 80  # 多搜索一些以应对下载失败
        
        self.logger.info(f"       搜索PubMed免费全文...")
        pmids = self.search_pubmed_free_fulltext(search_terms, search_count, existing_pmids)
        self.logger.info(f"       找到 {len(pmids)} 篇候选论文")
        
        # 如果不够，搜索PMC补充
        if len(pmids) < search_count:
            self.logger.info(f"       补充搜索PMC...")
            pmc_ids = self.search_pmc(search_terms, search_count - len(pmids) + 30, existing_pmids)
            
            for pmc_id in pmc_ids:
                pmid = self._pmcid_to_pmid(pmc_id)
                if pmid and pmid not in existing_pmids and pmid not in self.global_processed_pmids:
                    pmids.append(pmid)
                    if len(pmids) >= search_count:
                        break
        
        self.logger.info(f"       总共 {len(pmids)} 篇候选论文")
        
        # 5. 获取详情并下载
        self.logger.info(f"  [4/5] 获取论文详情...")
        new_papers = self.fetch_paper_details(pmids, category_cn, category_en)
        self.logger.info(f"       获取到 {len(new_papers)} 篇详情")
        
        # 分级
        for paper in new_papers:
            level, grade, score = self.classifier.classify(paper)
            paper.evidence_level = level
            paper.evidence_grade = grade
            paper.quality_score = score
        
        # 按质量分数排序
        new_papers.sort(key=lambda x: x.quality_score, reverse=True)
        
        # 下载
        self.logger.info(f"  [5/5] 下载PDF文件...")
        download_count = 0
        
        for paper in new_papers:
            if len(final_papers) >= self.papers_per_category:
                break
            
            if paper.pmid in existing_pmids or paper.pmid in self.global_processed_pmids:
                continue
            
            title_short = paper.title[:35] + "..." if len(paper.title) > 35 else paper.title
            progress = f"[{len(final_papers)+1}/{self.papers_per_category}]"
            
            self.logger.info(f"       {progress} PMID:{paper.pmid}")
            self.logger.info(f"              {title_short}")
            
            success = self.download_paper(paper, category_dir)
            
            if success:
                final_papers.append(paper)
                self.global_processed_pmids.add(paper.pmid)
                existing_pmids.add(paper.pmid)
                
                if paper.download_status == "exists":
                    self.logger.info(f"              ○ 已存在")
                    stats["existing_valid"] += 1
                else:
                    self.logger.info(f"              ✓ 下载成功 [{paper.download_source}]")
                    stats["new_downloaded"] += 1
                    download_count += 1
                    
                    if paper.download_source == "PMC":
                        stats["pmc"] += 1
                    elif paper.download_source == "EuropePMC":
                        stats["europepmc"] += 1
                    elif paper.download_source == "Unpaywall":
                        stats["unpaywall"] += 1
            else:
                self.logger.info(f"              ✗ 下载失败")
                stats["failed"] += 1
            
            time.sleep(self.delay)
        
        stats["total"] = len(final_papers)
        
        self.logger.info(f"\n  类别完成统计:")
        self.logger.info(f"    - 目标: {self.papers_per_category} 篇")
        self.logger.info(f"    - 已有: {stats['existing_valid']} 篇")
        self.logger.info(f"    - 新下载: {stats['new_downloaded']} 篇")
        self.logger.info(f"    - 总计: {stats['total']} 篇")
        
        return final_papers, stats
    
    def save_category_csv(self, papers: List[PaperInfo], category_en: str):
        """保存单个类别的CSV（含完整摘要）"""
        category_dir = os.path.join(self.base_dir, category_en)
        filepath = os.path.join(category_dir, f"{category_en}_papers.csv")
        
        fieldnames = [
            "序号", "PMID", "PMC_ID", "DOI", 
            "标题", "作者", "期刊", "年份", "月份",
            "疾病类型_中文", "疾病类型_英文",
            "发表类型", "研究类型",
            "是否Meta分析", "是否系统评价", "是否RCT", 
            "是否队列研究", "是否病例对照", "是否指南", "是否综述",
            "样本量", "预估影响因子",
            "证据等级", "GRADE分级", "质量分数",
            "下载状态", "文件名", "下载来源", "文件大小(KB)",
            "关键词", "完整摘要"
        ]
        
        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(fieldnames)
            
            for i, paper in enumerate(papers, 1):
                row = [
                    i,
                    paper.pmid,
                    paper.pmc_id,
                    paper.doi,
                    paper.title,
                    paper.authors,
                    paper.journal,
                    paper.year,
                    paper.month,
                    paper.disease_category_cn,
                    paper.disease_category_en,
                    paper.publication_type[:100] if paper.publication_type else "",
                    paper.study_type,
                    "是" if paper.is_meta_analysis else "否",
                    "是" if paper.is_systematic_review else "否",
                    "是" if paper.is_rct else "否",
                    "是" if paper.is_cohort else "否",
                    "是" if paper.is_case_control else "否",
                    "是" if paper.is_guideline else "否",
                    "是" if paper.is_review else "否",
                    paper.sample_size if paper.sample_size > 0 else "",
                    f"{paper.impact_factor:.1f}",
                    paper.evidence_level,
                    paper.evidence_grade,
                    f"{paper.quality_score:.1f}",
                    paper.download_status,
                    paper.filename,
                    paper.download_source,
                    f"{paper.file_size/1024:.1f}" if paper.file_size > 0 else "",
                    paper.keywords,
                    paper.abstract,  # 完整摘要
                ]
                writer.writerow(row)
        
        self.logger.info(f"  CSV已保存: {filepath}")
    
    def save_all_papers_csv(self, all_papers: List[PaperInfo]):
        """保存所有论文的汇总CSV"""
        filepath = os.path.join(self.base_dir, "all_papers_classification.csv")
        
        fieldnames = [
            "序号", "疾病类型_中文", "疾病类型_英文",
            "PMID", "PMC_ID", "DOI", 
            "标题", "作者", "期刊", "年份", "月份",
            "发表类型", "研究类型",
            "是否Meta分析", "是否系统评价", "是否RCT", 
            "是否队列研究", "是否病例对照", "是否指南", "是否综述",
            "样本量", "预估影响因子",
            "证据等级", "GRADE分级", "质量分数",
            "下载状态", "文件名", "下载来源", "文件大小(KB)",
            "关键词", "完整摘要"
        ]
        
        # 按疾病类型和质量分数排序
        sorted_papers = sorted(
            all_papers, 
            key=lambda x: (x.disease_category_cn, -x.quality_score)
        )
        
        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(fieldnames)
            
            for i, paper in enumerate(sorted_papers, 1):
                row = [
                    i,
                    paper.disease_category_cn,
                    paper.disease_category_en,
                    paper.pmid,
                    paper.pmc_id,
                    paper.doi,
                    paper.title,
                    paper.authors,
                    paper.journal,
                    paper.year,
                    paper.month,
                    paper.publication_type[:100] if paper.publication_type else "",
                    paper.study_type,
                    "是" if paper.is_meta_analysis else "否",
                    "是" if paper.is_systematic_review else "否",
                    "是" if paper.is_rct else "否",
                    "是" if paper.is_cohort else "否",
                    "是" if paper.is_case_control else "否",
                    "是" if paper.is_guideline else "否",
                    "是" if paper.is_review else "否",
                    paper.sample_size if paper.sample_size > 0 else "",
                    f"{paper.impact_factor:.1f}",
                    paper.evidence_level,
                    paper.evidence_grade,
                    f"{paper.quality_score:.1f}",
                    paper.download_status,
                    paper.filename,
                    paper.download_source,
                    f"{paper.file_size/1024:.1f}" if paper.file_size > 0 else "",
                    paper.keywords,
                    paper.abstract,
                ]
                writer.writerow(row)
        
        self.logger.info(f"\n汇总CSV已保存: {filepath}")
        return filepath
    
    def save_summary_report(self, all_stats: List[dict], all_papers: List[PaperInfo]):
        """保存统计摘要报告"""
        filepath = os.path.join(self.base_dir, "download_summary_report.txt")
        
        # 统计
        level_counts = {}
        grade_counts = {"A": 0, "B": 0, "C": 0, "D": 0}
        study_type_counts = {}
        
        for paper in all_papers:
            level_counts[paper.evidence_level] = level_counts.get(paper.evidence_level, 0) + 1
            if paper.evidence_grade in grade_counts:
                grade_counts[paper.evidence_grade] += 1
            study_type_counts[paper.study_type] = study_type_counts.get(paper.study_type, 0) + 1
        
        total_existing = sum(s["existing_valid"] for s in all_stats)
        total_new = sum(s["new_downloaded"] for s in all_stats)
        total_failed = sum(s["failed"] for s in all_stats)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("=" * 75 + "\n")
            f.write("          PubMed 多疾病类型论文下载与循证医学分级报告\n")
            f.write("=" * 75 + "\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"邮箱: {self.email}\n")
            f.write(f"保存目录: {self.base_dir}\n")
            f.write(f"每类目标: {self.papers_per_category} 篇\n")
            f.write("\n")
            
            # 总体统计
            f.write("【总体统计】\n")
            f.write("-" * 50 + "\n")
            f.write(f"  疾病类型数: {len(all_stats)}\n")
            f.write(f"  论文总数: {len(all_papers)}\n")
            f.write(f"  本地已有: {total_existing}\n")
            f.write(f"  新下载: {total_new}\n")
            f.write(f"  下载失败: {total_failed}\n")
            f.write("\n")
            
            # 各类别统计
            f.write("【各类别统计】\n")
            f.write("-" * 75 + "\n")
            f.write(f"{'类别':<15} {'目标':>6} {'已有':>6} {'新下载':>6} {'失败':>6} {'总计':>6}\n")
            f.write("-" * 75 + "\n")
            for stat in all_stats:
                f.write(f"{stat['category_cn']:<14} {stat['target']:>6} {stat['existing_valid']:>6} ")
                f.write(f"{stat['new_downloaded']:>6} {stat['failed']:>6} {stat['total']:>6}\n")
            f.write("\n")
            
            # 证据等级分布
            f.write("【证据等级分布】(牛津循证医学中心标准)\n")
            f.write("-" * 50 + "\n")
            level_desc = {
                "1a": "系统评价/Meta分析",
                "1b": "高质量RCT",
                "2a": "队列研究系统评价",
                "2b": "队列研究",
                "3a": "病例对照系统评价",
                "3b": "病例对照研究",
                "4": "病例系列",
                "5": "专家意见/综述"
            }
            for level in sorted(level_counts.keys()):
                count = level_counts[level]
                pct = 100 * count / len(all_papers) if all_papers else 0
                desc = level_desc.get(level, "")
                bar = "█" * int(pct / 2)
                f.write(f"  Level {level}: {count:>5} ({pct:>5.1f}%) {bar} {desc}\n")
            f.write("\n")
            
            # GRADE分级分布
            f.write("【GRADE分级分布】\n")
            f.write("-" * 50 + "\n")
            grade_desc = {"A": "高质量", "B": "中等质量", "C": "低质量", "D": "极低质量"}
            for grade in ["A", "B", "C", "D"]:
                count = grade_counts[grade]
                pct = 100 * count / len(all_papers) if all_papers else 0
                bar = "█" * int(pct / 2)
                f.write(f"  Grade {grade} ({grade_desc[grade]}): {count:>5} ({pct:>5.1f}%) {bar}\n")
            f.write("\n")
            
            # 研究类型分布
            f.write("【研究类型分布】\n")
            f.write("-" * 50 + "\n")
            type_names = {
                "meta_analysis": "Meta分析",
                "systematic_review": "系统评价",
                "guideline": "临床指南",
                "rct": "随机对照试验",
                "cohort": "队列研究",
                "case_control": "病例对照",
                "cross_sectional": "横断面研究",
                "review": "综述",
                "case_report": "病例报告",
                "other": "其他"
            }
            for stype, count in sorted(study_type_counts.items(), key=lambda x: -x[1]):
                name = type_names.get(stype, stype)
                pct = 100 * count / len(all_papers) if all_papers else 0
                f.write(f"  {name}: {count} ({pct:.1f}%)\n")
            f.write("\n")
            
            # 高质量论文TOP20
            f.write("【高质量论文TOP20】\n")
            f.write("-" * 75 + "\n")
            top_papers = sorted(all_papers, key=lambda x: x.quality_score, reverse=True)[:20]
            
            for i, p in enumerate(top_papers, 1):
                f.write(f"\n{i}. [{p.disease_category_cn}] Level {p.evidence_level} / Grade {p.evidence_grade}\n")
                f.write(f"   分数: {p.quality_score:.1f} | 影响因子: {p.impact_factor:.1f} | 类型: {p.study_type}\n")
                f.write(f"   PMID: {p.pmid}\n")
                f.write(f"   标题: {p.title[:70]}...\n")
                f.write(f"   期刊: {p.journal}\n")
                f.write(f"   年份: {p.year}\n")
        
        self.logger.info(f"统计报告已保存: {filepath}")
    
    def run(self):
        """运行主流程"""
        start_time = time.time()
        
        print("""
╔═══════════════════════════════════════════════════════════════════════════════╗
║              PubMed 多疾病类型论文批量下载与循证医学分级系统                  ║
║                              （优化版 v2.0）                                  ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║  疾病类型: 19类                                                               ║
║  每类目标: 200篇（已存在+新下载）                                             ║
║  总计: 约3800篇论文                                                           ║
║                                                                               ║
║  包含类型:                                                                    ║
║  妇科、产科、流感、肛肠科、脑血管、神经衰弱、颈椎病、腰肌劳损、               ║
║  肩周炎、腰椎间盘突出、痛风、牙科、便秘、咽炎、鼻炎、溃疡、                   ║
║  腹泻、静脉曲张、坐骨神经                                                     ║
║                                                                               ║
║  功能特点:                                                                    ║
║  ✓ 本地已存在的论文自动跳过下载但计入统计                                     ║
║  ✓ 确保每类达到100篇目标                                                      ║
║  ✓ 按疾病类型分文件夹存储                                                     ║
║  ✓ 循证医学证据等级分级                                                       ║
║  ✓ 保存完整摘要到CSV                                                          ║
╚═══════════════════════════════════════════════════════════════════════════════╝
        """)
        
        self.logger.info(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"邮箱: {self.email}")
        self.logger.info(f"保存目录: {self.base_dir}")
        self.logger.info(f"疾病类型数: {len(DISEASE_CATEGORIES)}")
        self.logger.info(f"每类目标: {self.papers_per_category} 篇")
        
        all_papers = []
        all_stats = []
        
        # 处理每个疾病类别
        for idx, (category_cn, category_info) in enumerate(DISEASE_CATEGORIES.items(), 1):
            self.logger.info(f"\n\n{'#'*75}")
            self.logger.info(f"# 进度: [{idx}/{len(DISEASE_CATEGORIES)}] - {category_cn}")
            self.logger.info(f"{'#'*75}")
            
            try:
                papers, stats = self.process_category(category_cn, category_info)
                
                # 保存该类别的CSV
                if papers:
                    self.save_category_csv(papers, category_info["en_name"])
                    all_papers.extend(papers)
                    all_stats.append(stats)
                
            except Exception as e:
                self.logger.error(f"处理类别 {category_cn} 时出错: {e}")
                continue
        
        # 保存汇总结果
        self.logger.info("\n\n" + "=" * 75)
        self.logger.info("保存汇总结果...")
        
        if all_papers:
            self.save_all_papers_csv(all_papers)
            self.save_summary_report(all_stats, all_papers)
            
            # JSON备份
            json_path = os.path.join(self.base_dir, "all_papers_data.json")
            with open(json_path, 'w', encoding='utf-8') as f:
                papers_dict = [asdict(p) for p in all_papers]
                json.dump(papers_dict, f, ensure_ascii=False, indent=2)
            self.logger.info(f"JSON备份已保存: {json_path}")
        
        # 最终统计
        elapsed = time.time() - start_time
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        
        total_existing = sum(s["existing_valid"] for s in all_stats)
        total_new = sum(s["new_downloaded"] for s in all_stats)
        total_failed = sum(s["failed"] for s in all_stats)
        
        self.logger.info("\n" + "=" * 75)
        self.logger.info("🎉 全部完成！")
        self.logger.info("=" * 75)
        self.logger.info(f"  疾病类型: {len(all_stats)} 类")
        self.logger.info(f"  论文总数: {len(all_papers)} 篇")
        self.logger.info(f"  本地已有: {total_existing} 篇")
        self.logger.info(f"  新下载: {total_new} 篇")
        self.logger.info(f"  下载失败: {total_failed} 篇")
        self.logger.info(f"  耗时: {hours}小时{minutes}分{seconds}秒")
        self.logger.info(f"\n  📁 保存目录: {self.base_dir}")
        self.logger.info(f"  📊 汇总CSV: all_papers_classification.csv")
        self.logger.info(f"  📋 统计报告: download_summary_report.txt")
        
        print(f"\n\n{'='*75}")
        print("✅ 任务完成！")
        print(f"   总计: {len(all_papers)} 篇论文")
        print(f"   保存位置: {self.base_dir}")
        print(f"{'='*75}\n")


def main():
    """主函数"""
    downloader = MultiDiseasePubMedDownloader(CONFIG)
    downloader.run()


if __name__ == "__main__":
    main()