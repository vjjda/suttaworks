# Path: src/db_builder/processors/hierarchy_processor.py
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple
from collections import defaultdict
from src.config.constants import PROJECT_ROOT

logger = logging.getLogger(__name__)


class HierarchyProcessor:

    def __init__(
        self,
        tree_config: List[Dict[str, Any]],
        valid_uids: set,
        uid_to_type_map: Dict[str, str],
    ):
        self.tree_config = tree_config
        self.valid_uids = valid_uids
        self.uid_to_type_map = uid_to_type_map
        self.nodes: List[Dict[str, Any]] = []
        self.node_lookup: Dict[str, Dict[str, Any]] = {}
        self.book_parents: Dict[str, str] = {}
        self.pitaka_map: Dict[str, str] = {}
        self.ignore_list = set()
        self.CANONICAL_PARENTS = {"dhp": "kn"}
        for entry in self.tree_config:
            if "ignore" in entry and isinstance(entry["ignore"], list):
                self.ignore_list.update(entry["ignore"])
        if self.ignore_list:
            logger.info(
                f"Đã khởi tạo danh sách bỏ qua với các file: {self.ignore_list}"
            )

    def _learn_super_tree(
        self, data: Any, parent_uid: str | None, pitaka_root: str | None
    ):

        if isinstance(data, list):
            for item in data:
                self._learn_super_tree(item, parent_uid, pitaka_root)
        elif isinstance(data, dict):
            for key, value in data.items():
                current_pitaka_root = pitaka_root or (
                    key if key in ["sutta", "vinaya", "abhidhamma"] else None
                )
                if current_pitaka_root:
                    self.pitaka_map[key] = current_pitaka_root
                if parent_uid:
                    self.book_parents[key] = parent_uid
                self._learn_super_tree(value, key, current_pitaka_root)
        elif isinstance(data, str):
            if pitaka_root:
                self.pitaka_map[data] = pitaka_root
            if parent_uid:
                self.book_parents[data] = parent_uid

    def _apply_canonical_rules(self):
        logger.info("Áp dụng các quy tắc cha-con chính tắc...")
        for child, parent in self.CANONICAL_PARENTS.items():
            if child in self.book_parents and self.book_parents[child] != parent:
                logger.warning(
                    f"QUY TẮC GHI ĐÈ: Thay đổi cha của '{child}' "
                    f"từ '{self.book_parents[child]}' thành '{parent}'."
                )
            self.book_parents[child] = parent

    def process_trees(self) -> List[Dict[str, Any]]:
        logger.info("Bắt đầu xử lý các file JSON tree...")

        super_tree_path = PROJECT_ROOT / self.tree_config[0]["super-tree"]
        with open(super_tree_path, "r", encoding="utf-8") as f:
            super_tree_data = json.load(f)
        self._learn_super_tree(super_tree_data, parent_uid=None, pitaka_root=None)
        self._apply_canonical_rules()

        all_files = [super_tree_path]
        file_entries = [
            e for e in self.tree_config if "ignore" not in e and "super-tree" not in e
        ]
        for entry in file_entries:
            for _, path_str in entry.items():
                path = PROJECT_ROOT / path_str
                if path.is_dir():
                    all_files.extend(sorted(path.glob("*.json")))

        for file_path in all_files:
            if file_path.name in self.ignore_list:
                logger.warning(
                    f"⏩ Bỏ qua file '{file_path.name}' do có trong danh sách ignore."
                )
                continue
            self._process_file(file_path)

        logger.info(
            f"Tổng số node ban đầu: {len(self.nodes)}. Bắt đầu lọc 'dead leaves'..."
        )
        filtered_nodes = [node for node in self.nodes if node["uid"] in self.valid_uids]
        logger.info(f"Tổng số node sau khi lọc dead leaves: {len(filtered_nodes)}.")
        self.nodes = filtered_nodes

        logger.info("Hiệu đính các nhánh trở thành lá (sách rỗng)...")
        current_parents = {
            node["parent_uid"] for node in self.nodes if node.get("parent_uid")
        }
        for node in self.nodes:

            if node["type"] == "branch" and node["uid"] not in current_parents:
                logger.warning(
                    f"Node '{node['uid']}' là một nhánh rỗng. "
                    f"Chuyển type thành 'leaf' và book_root thành chính nó."
                )
                node["type"] = "leaf"
                node["book_root"] = node["uid"]

        logger.info("Bắt đầu tỉa các nhánh rỗng...")
        while True:
            node_count_before_pruning = len(self.nodes)

            current_parents = {
                node["parent_uid"] for node in self.nodes if node.get("parent_uid")
            }

            nodes_after_pruning = [
                node
                for node in self.nodes
                if node["type"] != "branch" or node["uid"] in current_parents
            ]

            self.nodes = nodes_after_pruning
            node_count_after_pruning = len(self.nodes)

            if node_count_before_pruning == node_count_after_pruning:
                break
            else:
                logger.info(
                    f"Đã tỉa {node_count_before_pruning - node_count_after_pruning} nhánh rỗng..."
                )

        logger.info(f"Tổng số node cuối cùng sau khi tỉa cành: {len(self.nodes)}.")

        logger.info("Tính toán global_position cho các node...")
        for i, node in enumerate(self.nodes):
            node["global_position"] = i

        self._link_nodes_within_books()
        return self.nodes

    def _process_file(self, file_path: Path):

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        is_super_tree = "super-tree" in file_path.name
        if is_super_tree:

            self._recursive_parse(
                data, None, None, "buddha", pitaka_depth=0, book_depth=-1, position=0
            )
        else:
            if isinstance(data, dict) and len(data) == 1:
                book_root = list(data.keys())[0]
                parent_uid = self.book_parents.get(book_root)
                pitaka_root = self.pitaka_map.get(book_root)
                parent_node = self.node_lookup.get(parent_uid, {})
                parent_pitaka_depth = parent_node.get("pitaka_depth", -1)

                self._recursive_parse(
                    data,
                    parent_uid,
                    pitaka_root,
                    book_root,
                    pitaka_depth=parent_pitaka_depth + 1,
                    book_depth=0,
                    position=0,
                )
            else:
                logger.warning(
                    f"Bỏ qua file {file_path.name} vì cấu trúc không hợp lệ."
                )

    def _recursive_parse(
        self,
        data: Any,
        parent_uid: str | None,
        pitaka_root: str | None,
        book_root: str | None,
        pitaka_depth: int,
        book_depth: int,
        position: int,
    ):
        if isinstance(data, list):
            for i, item in enumerate(data):
                self._recursive_parse(
                    item,
                    parent_uid,
                    pitaka_root,
                    book_root,
                    pitaka_depth,
                    book_depth,
                    position=i,
                )

        elif isinstance(data, dict):
            if len(data) == 1:
                key = list(data.keys())[0]
                value = data[key]

                node_type = self.uid_to_type_map.get(key)
                if not node_type:
                    node_type = "branch" if value else "leaf"
                    if parent_uid is None:
                        node_type = "root"

                node = {
                    "uid": key,
                    "parent_uid": parent_uid,
                    "pitaka_root": pitaka_root or self.pitaka_map.get(key),
                    "book_root": book_root,
                    "type": node_type,
                    "pitaka_depth": pitaka_depth,
                    "book_depth": book_depth,
                    "sibling_position": position,
                }

                self.nodes.append(node)
                self.node_lookup[key] = node

                new_book_depth = book_depth + 1 if book_depth != -1 else -1
                self._recursive_parse(
                    value,
                    key,
                    pitaka_root or self.pitaka_map.get(key),
                    book_root,
                    pitaka_depth + 1,
                    new_book_depth,
                    position=0,
                )

        elif isinstance(data, str):

            node_type = self.uid_to_type_map.get(data, "leaf")

            node = {
                "uid": data,
                "parent_uid": parent_uid,
                "pitaka_root": pitaka_root,
                "book_root": book_root,
                "type": node_type,
                "pitaka_depth": pitaka_depth,
                "book_depth": book_depth,
                "sibling_position": position,
            }
            self.nodes.append(node)
            self.node_lookup[data] = node

    def _link_nodes_within_books(self):
        logger.info("Đang liên kết các node và tính depth_position...")

        nodes_by_book = defaultdict(list)
        for node in self.nodes:
            book_root_key = node.get("book_root") or "unknown"
            nodes_by_book[book_root_key].append(node)

        for book_root, nodes_in_book in nodes_by_book.items():

            nodes_by_depth_in_book = defaultdict(list)
            for node in nodes_in_book:
                nodes_by_depth_in_book[node["pitaka_depth"]].append(node)

            for pitaka_depth, nodes_in_depth in nodes_by_depth_in_book.items():

                for i, node in enumerate(nodes_in_depth):
                    node["depth_position"] = i
                    node["prev_uid"] = nodes_in_depth[i - 1]["uid"] if i > 0 else None
                    node["next_uid"] = (
                        nodes_in_depth[i + 1]["uid"]
                        if i < len(nodes_in_depth) - 1
                        else None
                    )

        logger.info("✅ Liên kết và tính toán position thành công.")