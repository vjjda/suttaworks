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

    def process_trees(self) -> List[Dict[str, Any]]:
        """Bắt đầu quá trình xử lý."""
        # --- Bước 1: "Học" từ super-tree.json ---
        relative_super_tree_path = self.tree_config[0]['super-tree']
        super_tree_path = PROJECT_ROOT / relative_super_tree_path
        with open(super_tree_path, 'r', encoding='utf-8') as f:
            super_tree_data = json.load(f)
        self._learn_super_tree(super_tree_data, parent_uid=None, pitaka_root=None)

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

        # --- Bước 3: Liên kết các node theo depth level ---
        self._link_nodes_by_depth()
        return self.nodes

    def _process_file(self, file_path: Path):
        """Đọc file và bắt đầu đệ quy với depth = 0."""
        # ... (logic tương tự, nhưng bắt đầu với depth=0)
        logger.debug(f"Đang xử lý file: {file_path.name}")
        with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
        is_super_tree = 'super-tree' in file_path.name
        if is_super_tree:
            self._recursive_parse(data, parent_uid=None, pitaka_root=None, book_root='buddha', depth=0)
        else:
            if isinstance(data, dict) and len(data) == 1:
                book_root = list(data.keys())[0]
                parent_uid = self.book_parents.get(book_root)
                pitaka_root = self.pitaka_map.get(book_root)
                parent_depth = self.node_lookup.get(parent_uid, {}).get('depth', -1)
                self._recursive_parse(data, parent_uid, pitaka_root, book_root, depth=parent_depth + 1)
            else:
                logger.warning(f"Bỏ qua file {file_path.name} vì cấu trúc không hợp lệ.")

    def _recursive_parse(self, data: Any, parent_uid: str | None, pitaka_root: str | None, book_root: str | None, depth: int):
        """Hàm đệ quy, truyền và tăng dần depth."""
        if isinstance(data, list):
            for i, item in enumerate(data):
                self._recursive_parse(item, parent_uid, pitaka_root, book_root, depth)
        elif isinstance(data, dict):
            for i, (key, value) in enumerate(data.items()):
                node_pitaka_root = pitaka_root or self.pitaka_map.get(key)
                node = {
                    "uid": key, "parent_uid": parent_uid, "position": i,
                    "pitaka_root": node_pitaka_root, "book_root": book_root,
                    "type": 'branch' if value else 'leaf', "depth": depth
                }
                if parent_uid is None: node['type'] = 'root'
                self.nodes.append(node)
                self.node_lookup[key] = node
                self._recursive_parse(value, key, node_pitaka_root, book_root, depth + 1)
        elif isinstance(data, str):
            node = {
                "uid": data, "parent_uid": parent_uid, "position": len(self.nodes),
                "pitaka_root": pitaka_root, "book_root": book_root,
                "type": 'leaf', "depth": depth
            }
            self.nodes.append(node)
            self.node_lookup[data] = node

    def _link_nodes_by_depth(self):
        """
        --- LOGIC HOÀN TOÀN MỚI ---
        Nhóm các node theo depth, sau đó tạo liên kết bên trong mỗi nhóm.
        """
        logger.info("Đang liên kết các node theo từng cấp độ (depth)...")
        nodes_by_depth = defaultdict(list)
        for node in self.nodes:
            nodes_by_depth[node['depth']].append(node)

        for depth, nodes_in_depth in nodes_by_depth.items():
            logger.debug(f"Đang liên kết {len(nodes_in_depth)} node ở depth {depth}...")
            for i, node in enumerate(nodes_in_depth):
                node['prev_uid'] = nodes_in_depth[i-1]['uid'] if i > 0 else None
                node['next_uid'] = nodes_in_depth[i+1]['uid'] if i < len(nodes_in_depth) - 1 else None
        
        logger.info("✅ Liên kết các node theo cấp độ thành công.")