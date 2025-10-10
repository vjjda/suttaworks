# Path: src/db_builder/processors/hierarchy_processor.py
#!/usr/bin/env python3

import json
import logging
from pathlib import Path
from typing import Any, Dict, List
from collections import defaultdict

# --- THAY ĐỔI DUY NHẤT: Import PROJECT_ROOT ---
from src.config.constants import PROJECT_ROOT

logger = logging.getLogger(__name__)

class HierarchyProcessor:
    """Xử lý các file JSON tree để xây dựng dữ liệu cho bảng Hierarchy."""
    CANONICAL_PARENTS = {
        'dhp': 'kn'
        # Thêm các quy tắc khác ở đây nếu cần trong tương lai
    }
    
    def __init__(self, tree_config: List[Dict[str, Any]]):
        self.tree_config = tree_config
        self.nodes: List[Dict[str, Any]] = []
        self.node_lookup: Dict[str, Dict[str, Any]] = {}
        self.book_parents: Dict[str, str] = {}
        self.pitaka_map: Dict[str, str] = {}
        self.ignore_list = set()
        for entry in self.tree_config:
            if 'ignore' in entry and isinstance(entry['ignore'], list):
                self.ignore_list.update(entry['ignore'])
        if self.ignore_list:
            logger.info(f"Đã khởi tạo danh sách bỏ qua với các file: {self.ignore_list}")

    def _learn_super_tree(self, data: Any, parent_uid: str | None, pitaka_root: str | None):
        # ... (giữ nguyên logic học)
        if isinstance(data, list):
            for item in data: self._learn_super_tree(item, parent_uid, pitaka_root)
        elif isinstance(data, dict):
            for key, value in data.items():
                current_pitaka_root = pitaka_root or (key if key in ['sutta', 'vinaya', 'abhidhamma'] else None)
                if current_pitaka_root: self.pitaka_map[key] = current_pitaka_root
                if parent_uid: self.book_parents[key] = parent_uid
                self._learn_super_tree(value, key, current_pitaka_root)
        elif isinstance(data, str):
            if pitaka_root: self.pitaka_map[data] = pitaka_root
            if parent_uid: self.book_parents[data] = parent_uid

    def _apply_canonical_rules(self):
        """Ghi đè lại các parent đã học theo bản đồ quy tắc."""
        logger.info("Áp dụng các quy tắc cha-con chính tắc...")
        for child, parent in self.CANONICAL_PARENTS.items():
            if child in self.book_parents and self.book_parents[child] != parent:
                logger.warning(
                    f"QUY TẮC GHI ĐÈ: Thay đổi cha của '{child}' "
                    f"từ '{self.book_parents[child]}' thành '{parent}'."
                )
            self.book_parents[child] = parent
            
    def process_trees(self) -> List[Dict[str, Any]]:
        """Bắt đầu quá trình xử lý."""
        # --- Bước 1: "Học" từ super-tree.json ---
        relative_super_tree_path = self.tree_config[0]['super-tree']
        super_tree_path = PROJECT_ROOT / relative_super_tree_path
        with open(super_tree_path, 'r', encoding='utf-8') as f:
            super_tree_data = json.load(f)
        self._learn_super_tree(super_tree_data, parent_uid=None, pitaka_root=None)
        self._apply_canonical_rules()
        
        # --- Bước 2: Xử lý tất cả các tree để tạo node ---
        all_files = [super_tree_path]
        # ... (giữ nguyên logic tập hợp file)
        file_entries = [e for e in self.tree_config if 'ignore' not in e and 'super-tree' not in e]
        for entry in file_entries:
            for _, path_str in entry.items():
                path = Path(path_str)
                if path.is_dir(): all_files.extend(sorted(path.glob('*.json')))
        
        for file_path in all_files:
            if file_path.name in self.ignore_list:
                logger.warning(f"⏩ Bỏ qua file '{file_path.name}' do có trong danh sách ignore.")
                continue
            self._process_file(file_path)

        logger.info("Tính toán global_position cho các node...")
        for i, node in enumerate(self.nodes):
            node['global_position'] = i
        
        # --- Bước cuối: Liên kết các node và tính depth_position ---
        self._link_nodes_within_books()
        return self.nodes

    def _process_file(self, file_path: Path):
        """Đọc file và bắt đầu đệ quy với depth = 0 và position = 0."""
        logger.debug(f"Đang xử lý file: {file_path.name}")
        with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
        is_super_tree = 'super-tree' in file_path.name
        if is_super_tree:
            # Bắt đầu với position = 0
            self._recursive_parse(data, parent_uid=None, pitaka_root=None, book_root='buddha', depth=0, position=0)
        else:
            if isinstance(data, dict) and len(data) == 1:
                book_root = list(data.keys())[0]
                parent_uid = self.book_parents.get(book_root)
                pitaka_root = self.pitaka_map.get(book_root)
                # Lấy position của node cha
                parent_node = self.node_lookup.get(parent_uid, {})
                parent_depth = parent_node.get('depth', -1)
                # Bắt đầu với position = 0
                self._recursive_parse(data, parent_uid, pitaka_root, book_root, depth=parent_depth + 1, position=0)
            else:
                logger.warning(f"Bỏ qua file {file_path.name} vì cấu trúc không hợp lệ.")

    def _recursive_parse(self, data: Any, parent_uid: str | None, pitaka_root: str | None, book_root: str | None, depth: int, position: int):
        """Hàm đệ quy, sử dụng 'position' làm 'sibling_position'."""
        if isinstance(data, list):
            for i, item in enumerate(data):
                self._recursive_parse(item, parent_uid, pitaka_root, book_root, depth, position=i)
        
        elif isinstance(data, dict):
            if len(data) == 1:
                key = list(data.keys())[0]
                value = data[key]
                
                node_pitaka_root = pitaka_root or self.pitaka_map.get(key)
                node = {
                    "uid": key, "parent_uid": parent_uid,
                    "sibling_position": position, # <-- THAY ĐỔI: Đổi tên key
                    "pitaka_root": node_pitaka_root, "book_root": book_root,
                    "type": 'branch' if value else 'leaf', "depth": depth
                }
                if parent_uid is None: node['type'] = 'root'
                
                self.nodes.append(node)
                self.node_lookup[key] = node
                self._recursive_parse(value, key, node_pitaka_root, book_root, depth + 1, position=0)

        elif isinstance(data, str):
            node = {
                "uid": data, "parent_uid": parent_uid,
                "sibling_position": position, # <-- THAY ĐỔI: Đổi tên key
                "pitaka_root": pitaka_root, "book_root": book_root,
                "type": 'leaf', "depth": depth
            }
            self.nodes.append(node)
            self.node_lookup[data] = node

    def _link_nodes_within_books(self):
        """Nhóm node theo book_root và depth, sau đó tạo liên kết và tính depth_position."""
        logger.info("Đang liên kết các node và tính depth_position...")
        
        nodes_by_book = defaultdict(list)
        for node in self.nodes:
            book_root_key = node.get('book_root') or 'unknown'
            nodes_by_book[book_root_key].append(node)

        for book_root, nodes_in_book in nodes_by_book.items():
            nodes_by_depth_in_book = defaultdict(list)
            for node in nodes_in_book:
                nodes_by_depth_in_book[node['depth']].append(node)

            for depth, nodes_in_depth in nodes_by_depth_in_book.items():
                for i, node in enumerate(nodes_in_depth):
                    # --- THAY ĐỔI: Gán giá trị cho depth_position và prev/next_uid ---
                    node['depth_position'] = i
                    node['prev_uid'] = nodes_in_depth[i-1]['uid'] if i > 0 else None
                    node['next_uid'] = nodes_in_depth[i+1]['uid'] if i < len(nodes_in_depth) - 1 else None
        
        logger.info("✅ Liên kết và tính toán position thành công.")