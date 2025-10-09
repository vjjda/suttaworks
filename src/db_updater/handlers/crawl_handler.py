# Path: /src/db_updater/handlers/crawl_handler.py
import requests
from urllib.parse import urljoin, urlparse
from pathlib import Path
import time
import logging
import random
from bs4 import BeautifulSoup
import os

log = logging.getLogger(__name__)

class Crawler:
    def __init__(self, start_url, root_url, destination_dir):
        self.start_url = start_url
        self.root_url = root_url.rstrip('/') + '/' # Đảm bảo root_url luôn kết thúc bằng '/'
        self.domain_root = '{uri.scheme}://{uri.netloc}'.format(uri=urlparse(self.root_url))
        self.destination_dir = Path(destination_dir)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.queue = {self.start_url}
        self.visited_log_path = self.destination_dir / "visited_urls.log"
        self.visited = self._load_visited()

    # --- CÁC HÀM MỚI CHO LOGIC GIỚI HẠN THÔNG MINH ---
    def _get_resource_type(self, url: str) -> str:
        """Phân loại tài nguyên dựa trên đuôi file của URL."""
        path = urlparse(url).path.lower()
        if path.endswith('.html') or path.endswith('/'):
            return 'html'
        if path.endswith('.css'):
            return 'css'
        if any(path.endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp']):
            return 'image'
        if any(path.endswith(ext) for ext in ['.mp3', '.zip', '.pdf', '.epub', '.7z']):
            return 'media'
        return 'other'

    def _is_in_scope(self, url: str) -> bool:
        """Kiểm tra xem URL có nằm trong phạm vi cho phép hay không."""
        # Bước 1: Loại bỏ ngay lập tức nếu link trỏ ra ngoài domain
        if not url.startswith(self.domain_root):
            return False

        resource_type = self._get_resource_type(url)

        # Bước 2: Áp dụng luật lệ "dễ tính" cho CSS và Ảnh
        if resource_type in ['css', 'image']:
            return True # Miễn là cùng domain thì cho phép

        # Bước 3: Áp dụng luật lệ "nghiêm ngặt" cho HTML và các media khác
        if resource_type in ['html', 'media', 'other']:
            return url.startswith(self.root_url)
        
        return False

    # ... (giữ nguyên _load_visited, _save_visited, _fetch_and_save_resource, _extract_links) ...
    def _load_visited(self) -> set:
        if not self.visited_log_path.exists(): return set()
        log.info(f"Phát hiện file log, đang tải lại các URL đã xử lý từ: {self.visited_log_path}")
        with open(self.visited_log_path, 'r') as f:
            return {line.strip() for line in f if line.strip()}

    def _save_visited(self, url: str):
        self.destination_dir.mkdir(parents=True, exist_ok=True)
        with open(self.visited_log_path, 'a') as f:
            f.write(url + '\n')

    def _fetch_and_save_resource(self, url: str) -> tuple[bool, bytes | None]:
        try:
            parsed_url = urlparse(url)
            resource_path = parsed_url.path.lstrip('/')
            if not resource_path or resource_path.endswith('/'):
                resource_path = resource_path + 'index.html'
            dest_path = self.destination_dir / resource_path
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            
            time.sleep(random.uniform(1, 2))

            log.info(f"Đang tải: {url}")
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            with open(dest_path, 'wb') as f:
                f.write(response.content)
            log.info(f"Đã lưu vào: {dest_path}")
            return True, response.content
        except requests.exceptions.RequestException as e:
            log.error(f"Lỗi khi tải {url}: {e}")
            return False, None

    def _extract_links(self, html_content: bytes, base_url: str) -> set[str]:
        found_urls = set()
        soup = BeautifulSoup(html_content, 'html.parser')
        tags_with_links = soup.find_all(href=True) + soup.find_all(src=True)
        for tag in tags_with_links:
            link = tag.get('href') or tag.get('src')
            if not link or link.startswith('#') or link.startswith('mailto:'):
                continue
            absolute_link = urljoin(base_url, link).split('#')[0] # Bỏ fragment
            found_urls.add(absolute_link)
        return found_urls
    
    # ... (giữ nguyên _rewrite_all_links và _get_next_url_with_priority) ...
    def _get_next_url_with_priority(self) -> str | None:
        for url in self.queue:
            if url.endswith('.css'):
                self.queue.remove(url)
                return url
        for url in self.queue:
            if any(url.endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.gif', '.svg']):
                self.queue.remove(url)
                return url
        if self.queue:
            return self.queue.pop()
        return None
        
    def _rewrite_all_links(self):
        """
        Duyệt qua tất cả các file HTML đã tải và viết lại các link thành dạng tương đối.
        Phiên bản cuối cùng: xử lý query strings, meta tags, và các thẻ khác.
        """
        log.info("Bắt đầu quá trình viết lại link cho các file HTML...")
        html_files = list(self.destination_dir.rglob("*.html"))
        log.info(f"Tìm thấy {len(html_files)} file HTML cần xử lý.")

        for html_path in html_files:
            log.debug(f"Đang xử lý file: {html_path.relative_to(self.destination_dir)}")
            made_changes = False
            try:
                with open(html_path, 'r', encoding='utf-8', errors='ignore') as f:
                    soup = BeautifulSoup(f, 'html.parser')

                # Tìm tất cả các thẻ có khả năng chứa link
                tags_to_process = []
                tags_to_process.extend(soup.find_all(href=True))
                tags_to_process.extend(soup.find_all(src=True))
                tags_to_process.extend(soup.find_all('meta', property='og:image')) # Thêm thẻ meta

                for tag in tags_to_process:
                    # Xác định thuộc tính chứa link ('href', 'src', hoặc 'content')
                    if tag.has_attr('href'):
                        attr = 'href'
                    elif tag.has_attr('src'):
                        attr = 'src'
                    elif tag.has_attr('content'):
                        attr = 'content'
                    else:
                        continue
                    
                    link = tag[attr]

                    if not link or link.startswith('#') or link.startswith('mailto:') or '://' in link:
                        continue
                    
                    # --- LOGIC SỬA LỖI MỚI ---
                    # 1. Dùng urlparse để tách URL thành các thành phần
                    parsed_link = urlparse(link)
                    path_part = parsed_link.path
                    query_part = parsed_link.query
                    fragment_part = parsed_link.fragment

                    # 2. Xác định đường dẫn file đích trên máy tính (chỉ dùng path_part)
                    if path_part.startswith('/'):
                        target_path = self.destination_dir.joinpath(path_part.lstrip('/')).resolve()
                    else:
                        target_path = html_path.parent.joinpath(path_part).resolve()

                    # 3. Kiểm tra file tồn tại và tính toán đường dẫn tương đối
                    if target_path.exists():
                        relative_path = os.path.relpath(target_path, html_path.parent)
                        
                        # 4. Ghép lại đường dẫn mới, bảo toàn query và fragment
                        new_link = relative_path
                        if query_part:
                            new_link += f"?{query_part}"
                        if fragment_part:
                            new_link += f"#{fragment_part}"
                        
                        if tag[attr] != new_link:
                            tag[attr] = new_link
                            made_changes = True

                if made_changes:
                    with open(html_path, 'w', encoding='utf-8') as f:
                        f.write(str(soup))
            except Exception as e:
                log.error(f"Lỗi khi viết lại link cho file {html_path}: {e}")

    # --- CẬP NHẬT run() ĐỂ SỬ DỤNG HÀM KIỂM TRA SCOPE MỚI ---
    def run(self):
        log.info("Bắt đầu crawl với logic giới hạn thông minh...")
        while self.queue:
            url = self._get_next_url_with_priority()
            if not url: break

            if url in self.visited:
                continue
            
            # Sử dụng "người gác cổng thông minh"
            if not self._is_in_scope(url):
                log.debug(f"Link nằm ngoài phạm vi quy định, bỏ qua: {url}")
                continue
            
            success, content = self._fetch_and_save_resource(url)
            
            if success:
                self.visited.add(url)
                self._save_visited(url)
                if content and self._get_resource_type(url) == 'html':
                    new_links = self._extract_links(content, url)
                    for link in new_links:
                        if link not in self.visited:
                            self.queue.add(link)
        
        log.info(f"Crawl hoàn tất! Đã xử lý tổng cộng {len(self.visited)} links.")
        self._rewrite_all_links()
        log.info("Toàn bộ quá trình đã hoàn tất!")

# --- CẬP NHẬT PHẦN KIỂM THỬ VỚI GIỚI HẠN CHẶT CHẼ ---
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    PROJECT_ROOT_TEST = Path(__file__).resolve().parents[3]
    RAW_DATA_PATH_TEST = PROJECT_ROOT_TEST / "data/raw"
    
    TEST_URL_START = "https://www.dhammatalks.org/vinaya/bmc/Section0000.html"
    
    test_crawler = Crawler(
        start_url=TEST_URL_START,
        root_url="https://www.dhammatalks.org/vinaya/bmc/", # Giới hạn nghiêm ngặt
        destination_dir=RAW_DATA_PATH_TEST / "dhammatalks_test"
    )

    print("--- Bắt đầu kiểm thử với giới hạn thông minh ---")
    test_crawler.run()
    print("--- Hoàn tất kiểm thử ---")