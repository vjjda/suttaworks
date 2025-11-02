# Path: src/db_builder/processors/biblio_processor.py
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

from src.config.constants import PROJECT_ROOT

logger = logging.getLogger(__name__)


class BiblioProcessor:

    def __init__(self, biblio_path: str):
        self.biblio_path = PROJECT_ROOT / biblio_path

    def process(self) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
        logger.info(f"Bắt đầu xử lý file bibliography từ: {self.biblio_path}")
        biblio_data: List[Dict[str, Any]] = []
        text_to_uid_map: Dict[str, str] = {}

        if not self.biblio_path.exists():
            logger.error(f"Không tìm thấy file bibliography tại: {self.biblio_path}")
            return [], {}

        try:
            with open(self.biblio_path, "r", encoding="utf-8") as f:
                data_list = json.load(f)

            for biblio_entry in data_list:
                if not isinstance(biblio_entry, dict):
                    continue

                uid = biblio_entry.get("uid")
                if not uid:
                    continue

                text_content = biblio_entry.get("text")

                biblio_data.append(
                    {
                        "biblio_uid": uid,
                        "citation_key": biblio_entry.get("name"),
                        "full_citation": text_content,
                    }
                )

                if text_content:
                    text_to_uid_map[text_content] = uid

            logger.info(
                f"✅ Đã trích xuất {len(biblio_data)} mục bibliography và tạo map tra cứu."
            )
            return biblio_data, text_to_uid_map
        except Exception as e:
            logger.error(f"Lỗi khi xử lý file {self.biblio_path.name}: {e}")
            return [], {}
