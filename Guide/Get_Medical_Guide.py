#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
医学指南文献下载爬虫
从https://www.hnysfww.com/mobile/article_cat.php?id=61下载指南文献
"""
import requests
from bs4 import BeautifulSoup
import os
import time
import re
import random
from urllib.parse import urljoin, urlparse
import logging
from typing import List, Dict, Optional
import threading
from queue import Queue
import hashlib
import json


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('medical_guide_scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MedicalGuideScraper:
    def __init__(self, base_url: str, output_dir: str, max_guides: int = 400):
        self.base_url = base_url
        self.output_dir = output_dir
        self.max_guides = max_guides  # 需要下载的新文件数量
        self.session = requests.Session()
        # 禁用代理
        self.session.trust_env = False
        self.session.proxies = {'http': None, 'https': None}
        self.downloaded_count = 0  # 仅计数新下载的文件
        self.failed_count = 0
        self.skipped_count = 0  # 新增：计数跳过的已存在文件
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15'
        ]

        # 设置请求头
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)

    def get_random_user_agent(self) -> str:
        """随机获取User-Agent"""
        return random.choice(self.user_agents)

    def make_request(self, url: str, retries: int = 3) -> Optional[BeautifulSoup]:
        """发送HTTP请求，带有重试机制"""
        for attempt in range(retries):
            try:
                headers = {'User-Agent': self.get_random_user_agent()}
                # 确保请求不使用代理
                response = self.session.get(url, headers=headers, timeout=30, proxies={'http': None, 'https': None})
                response.raise_for_status()

                # 尝试检测编码
                if response.encoding == 'ISO-8859-1':
                    response.encoding = response.apparent_encoding or 'utf-8'

                return BeautifulSoup(response.text, 'html.parser')

            except Exception as e:
                logger.warning(f"请求失败 (尝试 {attempt + 1}/{retries}): {url} - {e}")
                if attempt < retries - 1:
                    wait_time = (attempt + 1) * 2
                    logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"请求最终失败: {url}")
                    return None

    def extract_guide_links_from_page(self, page_url: str) -> List[Dict]:
        """从页面提取指南链接"""
        logger.info(f"正在解析页面: {page_url}")
        soup = self.make_request(page_url)
        if not soup:
            return []
        guides = []

        # 尝试多种可能的CSS选择器
        selectors = [
            'article a', 'div.content a', 'div.list a', 'div.item a',
            'tr a', 'td a', 'ul li a', 'ol li a',
            'a[href*="article"]', 'a[href*="doc"]', 'a[href*="pdf"]'
        ]

        for selector in selectors:
            links = soup.select(selector)
            if links:
                logger.info(f"找到 {len(links)} 个链接，使用选择器: {selector}")
                break

        if not links:
            logger.warning(f"未在页面找到任何链接: {page_url}")
            return guides

        for link in links:
            try:
                href = link.get('href', '')
                title = link.get_text(strip=True)
                # 过滤链接
                if not href or not title or len(title) < 2:
                    continue
                # 只处理包含文章ID的链接
                if 'article.php?id=' in href or 'article.php?id=' in href:
                    full_url = urljoin(self.base_url, href)
                    # 获取文件类型
                    file_type = self.detect_file_type(link, href)

                    guides.append({
                        'title': title,
                        'url': full_url,
                        'page_url': page_url,
                        'file_type': file_type
                    })

            except Exception as e:
                logger.warning(f"处理链接时出错: {e}")
                continue

        logger.info(f"从页面提取到 {len(guides)} 个指南")
        return guides

    def detect_file_type(self, link_element, href: str) -> str:
        """检测文件类型"""
        # 检查href中的文件扩展名
        if '.pdf' in href.lower():
            return 'pdf'
        elif any(ext in href.lower() for ext in ['.doc', '.docx']):
            return 'doc'
        elif any(ext in href.lower() for ext in ['.ppt', '.pptx']):
            return 'ppt'
        else:
            # 根据链接文本判断
            text = link_element.get_text(strip=True).lower()
            if 'pdf' in text:
                return 'pdf'
            elif 'doc' in text:
                return 'doc'
            elif 'ppt' in text:
                return 'ppt'
            else:
                return 'unknown'

    def get_download_url(self, guide_url: str) -> Optional[str]:
        """从指南页面获取实际下载链接"""
        logger.info(f"正在获取下载链接: {guide_url}")

        soup = self.make_request(guide_url)
        if not soup:
            return None

        # 尝试多种下载链接选择器
        download_selectors = [
            'a[href*="download"]', 'a[href*="file"]', 'a[href*="down"]',
            'a[href*="upload"]', 'a[href*="attach"]',
            'a[href*=".pdf"]', 'a[href*=".doc"]', 'a[href*=".ppt"]'
        ]

        for selector in download_selectors:
            download_links = soup.select(selector)
            if download_links:
                logger.info(f"找到下载链接，使用选择器: {selector}")
                for dl_link in download_links:
                    href = dl_link.get('href', '')
                    if href and ('http' in href or 'download' in href.lower() or '.pdf' in href.lower()):
                        return urljoin(self.base_url, href)

        # 如果没有找到下载链接，尝试原始链接
        return guide_url

    def clean_title(self, title: str) -> str:
        """清理标题，移除时间、浏览次数等无关内容"""
        # 移除时间相关内容（如：2026-01-20、时间：2026-01-20）
        title = re.sub(r'时间[:：]?\s*\d{4}-\d{2}-\d{2}', '', title)
        title = re.sub(r'\d{4}-\d{2}-\d{2}', '', title)
        # 移除浏览次数相关内容（如：浏览次数：84、浏览量：100）
        title = re.sub(r'浏览[次数量]{0,2}[:：]?\s*\d+', '', title)
        # 移除[查看详情]、【查看详情】等标记
        title = re.sub(r'[【\[](查看详情|详情|查看)[】\]]', '', title)
        # 移除多余的空格、制表符、换行符
        title = re.sub(r'\s+', '', title)
        # 移除末尾的特殊字符
        title = re.sub(r'[：:；;，,。.、]$', '', title)

        return title.strip()

    def download_guide(self, guide_info: Dict) -> bool:
        """下载单个指南"""
        # 清理标题
        original_title = guide_info['title']
        clean_title = self.clean_title(original_title)

        if not clean_title:
            clean_title = f"未知标题_{hashlib.md5(original_title.encode()).hexdigest()[:8]}"

        download_url = self.get_download_url(guide_info['url'])

        if not download_url:
            logger.error(f"无法获取下载链接: {clean_title}")
            return False

        file_ext = guide_info['file_type']
        if file_ext == 'unknown':
            # 根据URL推断扩展名
            if '.pdf' in download_url:
                file_ext = 'pdf'
            elif '.doc' in download_url:
                file_ext = 'doc'
            elif '.ppt' in download_url:
                file_ext = 'ppt'
            else:
                file_ext = 'html'

        filename = self._get_safe_filename(clean_title, file_ext)
        filepath = os.path.join(self.output_dir, filename)

        # 检查文件是否已存在
        if os.path.exists(filepath):
            logger.info(f"文件已存在，跳过: {filename}")
            self.skipped_count += 1  # 计数跳过的文件
            return False  # 返回False表示未下载新文件

        try:
            logger.info(f"正在下载: {filename}")

            # 添加随机延迟
            time.sleep(random.uniform(1, 3))

            headers = {
                'User-Agent': self.get_random_user_agent(),
                'Referer': guide_info['url']
            }

            response = self.session.get(download_url, headers=headers, stream=True, timeout=60,
                                        proxies={'http': None, 'https': None})
            response.raise_for_status()

            # 获取文件大小
            file_size = int(response.headers.get('content-length', 0))

            # 下载文件
            with open(filepath, 'wb') as f:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        # 显示进度
                        if file_size > 0:
                            progress = (downloaded / file_size) * 100
                            if downloaded % (1024 * 1024) == 0:  # 每MB显示一次
                                logger.info(f"下载进度: {filename} - {progress:.1f}%")

            # 验证文件
            if os.path.exists(filepath):
                actual_size = os.path.getsize(filepath)
                if file_size > 0 and actual_size < file_size * 0.9:  # 文件大小不匹配
                    logger.warning(f"文件大小可能不完整: {filename} (预期: {file_size}, 实际: {actual_size})")
                    os.remove(filepath)
                    return False

                logger.info(f"下载成功: {filename} ({actual_size} 字节)")
                self.downloaded_count += 1  # 仅计数新下载的文件
                return True
            else:
                logger.error(f"文件创建失败: {filename}")
                return False

        except Exception as e:
            logger.error(f"下载失败: {filename} - {e}")
            if os.path.exists(filepath):
                os.remove(filepath)
            self.failed_count += 1
            return False

    def scrape_all_guides(self):
        """爬取所有指南（确保下载到指定数量的新文件）"""
        logger.info(f"开始爬取医学指南，目标新文件数量: {self.max_guides}")
        logger.info(f"输出目录: {self.output_dir}")

        # 从第1页开始爬取
        page = 1

        # 持续爬取直到达到目标数量
        while self.downloaded_count < self.max_guides:
            # 构建页面URL
            page_url = f"{self.base_url}&page={page}" if '?' in self.base_url else f"{self.base_url}?page={page}"

            # 提取页面中的指南
            guides = self.extract_guide_links_from_page(page_url)

            if not guides:
                logger.warning(f"第 {page} 页没有找到指南，尝试下一页")
                page += 1
                time.sleep(random.uniform(2, 5))
                continue

            logger.info(
                f"第 {page} 页找到 {len(guides)} 个指南，当前已下载 {self.downloaded_count}/{self.max_guides} 个新文件")

            # 下载当前页的指南
            for guide in guides:
                # 如果已达到目标数量，停止下载
                if self.downloaded_count >= self.max_guides:
                    break

                self.download_guide(guide)

                # 延迟，避免请求过于频繁
                time.sleep(random.uniform(3, 6))

            # 保存进度
            self.save_progress(page, self.downloaded_count, self.skipped_count, self.failed_count)

            page += 1

            # 延迟，避免请求过于频繁
            time.sleep(random.uniform(2, 5))

        # 总结
        logger.info("下载完成！")
        logger.info(f"成功下载新文件: {self.downloaded_count}")
        logger.info(f"跳过已存在文件: {self.skipped_count}")
        logger.info(f"下载失败: {self.failed_count}")
        logger.info(f"总计处理文件: {self.downloaded_count + self.skipped_count + self.failed_count}")

    def _get_safe_filename(self, title: str, file_type: str) -> str:
        """生成安全的文件名"""
        safe_title = re.sub(r'[\\/*?:"<>|]', '_', title)[:100]
        return f"{safe_title}.{file_type}"

    def save_progress(self, current_page: int, downloaded: int, skipped: int, failed: int):
        """保存下载进度"""
        progress_file = os.path.join(self.output_dir, 'download_progress.json')

        progress_data = {
            'timestamp': time.time(),
            'current_page': current_page,
            'target_guides': self.max_guides,
            'downloaded_count': downloaded,
            'skipped_count': skipped,
            'failed_count': failed,
            'total_processed': downloaded + skipped + failed
        }

        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, ensure_ascii=False, indent=2)

        logger.info(f"进度已保存到: {progress_file}")


def main():
    # 配置参数
    base_url = "https://www.hnysfww.com/mobile/article_cat.php?id=61"
    output_dir = r"E:\Datas\KY_dataset\GUIDE\Guides"
    max_guides = 400

    logger.info("医学指南爬虫启动")
    logger.info(f"目标网站: {base_url}")
    logger.info(f"输出目录: {output_dir}")
    logger.info(f"目标新文件数量: {max_guides}")

    # 创建爬虫实例
    scraper = MedicalGuideScraper(base_url, output_dir, max_guides)

    try:
        # 开始爬取
        scraper.scrape_all_guides()
    except KeyboardInterrupt:
        logger.info("用户中断，正在保存进度...")
        scraper.save_progress(
            0,
            scraper.downloaded_count,
            scraper.skipped_count,
            scraper.failed_count
        )
    except Exception as e:
        logger.error(f"程序出错: {e}")
        raise
    finally:
        logger.info("爬虫结束")


if __name__ == "__main__":
    main()