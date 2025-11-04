# Path: src/db_builder/processors/suttaplex_processor.py
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

from src.config.constants import PROJECT_ROOT

from .blurb_processor import BlurbSupplementProcessor
from .html_processor import HtmlFileProcessor
from .json_path_processor import JsonPathProcessor
from .suttaplex_extractor import SuttaplexExtractor

logger = logging.getLogger(__name__)


class SuttaplexProcessor:

    def __init__(self, suttaplex_config: Dict[str, Any], biblio_map: Dict[str, str]):
        self.suttaplex_config = suttaplex_config
        self.biblio_map = biblio_map

        self.suttaplex_file = PROJECT_ROOT / suttaplex_config.get("data", "")

    def _parse_config_paths(self):
        json_config, html_manifest_path, blurb_paths = {}, None, []

        item = self.suttaplex_config

        if "translation_files" in item:
            tf_config = item["translation_files"]

            if "json_segment" in tf_config and isinstance(
                tf_config["json_segment"], dict
            ):
                js_config = tf_config["json_segment"]
                manifest_path = PROJECT_ROOT / js_config.get("path", "")
                groups = js_config.get("groups", [])
                if manifest_path.exists() and groups:
                    json_config = {"path": manifest_path, "groups": groups}
                else:
                    logger.warning("Cấu hình 'json_segment' không hợp lệ.")

            if "html_text" in tf_config and isinstance(tf_config["html_text"], dict):
                path_str = tf_config["html_text"].get("path")
                if path_str:
                    path_obj = PROJECT_ROOT / path_str
                    if path_obj.exists():
                        html_manifest_path = path_obj
                    else:
                        logger.warning(f"File manifest HTML không tồn tại: {path_obj}")

        if "blurb_supplement" in item:
            blurb_paths.extend([PROJECT_ROOT / p for p in item["blurb_supplement"]])

        return json_config, html_manifest_path, blurb_paths

    def process(self) -> Tuple[List[Dict[str, Any]], ...]:

        extractor = SuttaplexExtractor(self.suttaplex_file, self.biblio_map).execute()

        json_config, html_manifest_path, blurb_paths = self._parse_config_paths()

        blurb_processor = BlurbSupplementProcessor(blurb_paths)
        suttaplex_data = blurb_processor.execute(extractor.suttaplex_data)

        filepath_map = {}
        if json_config:
            json_processor = JsonPathProcessor(
                manifest_path=json_config["path"], groups=json_config["groups"]
            )
            filepath_map = json_processor.execute()

        if html_manifest_path:
            known_uids = {
                item["translation_uid"] for item in extractor.translations_data
            }
            html_processor = HtmlFileProcessor(
                html_manifest_path, extractor.authors_map, known_uids
            )
            html_filepath_map = html_processor.execute()
            filepath_map.update(html_filepath_map)
        else:
            logger.warning("Không có cấu hình manifest cho HTML. Bỏ qua.")

        logger.info("Cập nhật file_path cho các bản dịch...")
        for translation in extractor.translations_data:
            trans_uid = translation.get("translation_uid")
            if trans_uid in filepath_map:
                translation["file_path"] = filepath_map[trans_uid]

        logger.info("✅ Hoàn tất xử lý suttaplex.")

        return (
            suttaplex_data,
            extractor.sutta_references_data,
            list(extractor.authors_map.values()),
            list(extractor.languages_map.values()),
            extractor.translations_data,
            extractor.valid_uids,
            extractor.uid_to_type_map,
        )
