# Path: src/db_builder/processors/suttaplex_extractor.py
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class SuttaplexExtractor:

    def __init__(self, suttaplex_file: Path, biblio_map: Dict[str, str]):
        self.suttaplex_file = suttaplex_file
        self.biblio_map = biblio_map
        self.suttaplex_data: List[Dict[str, Any]] = []
        self.sutta_references_data: List[Dict[str, Any]] = []
        self.translations_data: List[Dict[str, Any]] = []
        self.authors_map: Dict[str, Dict[str, Any]] = {}
        self.languages_map: Dict[str, Dict[str, Any]] = {}
        self.valid_uids = set()
        self.uid_to_type_map: Dict[str, str] = {}

    def _clean_value(self, value):
        return (
            value.strip()
            if isinstance(value, str) and value.strip()
            else (value if not isinstance(value, str) else None)
        )

    def _roman_to_int(self, s: str) -> int:
        s = s.lower()
        roman_map = {"i": 1, "v": 5, "x": 10, "l": 50, "c": 100, "d": 500, "m": 1000}
        result = 0
        for i in range(len(s)):
            if i > 0 and roman_map[s[i]] > roman_map[s[i - 1]]:
                result += roman_map[s[i]] - 2 * roman_map[s[i - 1]]
            else:
                result += roman_map[s[i]]
        return result

    def _clean_volpage_string(self, text: Any) -> str | None:
        if not text or not isinstance(text, str):
            return None
        items = [item.strip() for item in text.split(",")]
        processed_items = []
        for item in items:
            if not item:
                continue
            cleaned_item = re.sub(r"\b(SN|AN|Ud|Iti)\b\s*", "", item).strip()
            match = re.match(r"^(.*?)\s+([ivxlcdmIVXLCDM]+)\s+(\d+.*)", cleaned_item)
            if match:
                prefix, roman, rest = (
                    match.group(1).strip(),
                    match.group(2),
                    match.group(3),
                )
                try:
                    arabic_num = self._roman_to_int(roman)
                    cleaned_item = (
                        f"{prefix} {arabic_num}.{rest}"
                        if prefix
                        else f"{arabic_num}.{rest}"
                    )
                except KeyError:
                    pass
            processed_items.append(cleaned_item)
        return ", ".join(filter(None, processed_items))

    def _add_author(self, author_data: Dict[str, Any]):
        author_uid = self._clean_value(author_data.get("author_uid"))
        if author_uid and author_uid not in self.authors_map:
            self.authors_map[author_uid] = {
                "author_uid": author_uid,
                "author_name": self._clean_value(author_data.get("author")),
                "author_short": self._clean_value(author_data.get("author_short")),
            }

    def _add_language(self, lang_code: str | None, lang_name: str | None):
        code = self._clean_value(lang_code)
        if code and code not in self.languages_map:
            self.languages_map[code] = {
                "lang_code": code,
                "lang_name": self._clean_value(lang_name),
            }

    def execute(self):
        logger.info(f"Bắt đầu trích xuất dữ liệu từ file: {self.suttaplex_file}")

        if not self.suttaplex_file.exists():
            logger.error(f"Không tìm thấy file suttaplex tại: {self.suttaplex_file}")
            return self

        with open(self.suttaplex_file, "r", encoding="utf-8") as f:
            suttaplex_dict = json.load(f)

        for uid, card in suttaplex_dict.items():
            if not isinstance(card, dict):
                logger.warning(
                    f"Bỏ qua mục không hợp lệ với key '{uid}' trong suttaplex.json"
                )
                continue

            self.valid_uids.add(uid)
            self.uid_to_type_map[uid] = card.get("type")

            priority_author = card.get("priority_author_uid")
            final_priority_author = (
                priority_author[0]
                if isinstance(priority_author, list) and priority_author
                else priority_author
            )

            self.suttaplex_data.append(
                {
                    "uid": uid,
                    "root_lang": self._clean_value(card.get("root_lang")),
                    "acronym": self._clean_value(card.get("acronym")),
                    "translated_title": self._clean_value(card.get("translated_title")),
                    "original_title": self._clean_value(card.get("original_title")),
                    "blurb": self._clean_value(card.get("blurb")),
                    "priority_author_uid": self._clean_value(final_priority_author),
                }
            )
            self._add_language(card.get("root_lang"), card.get("root_lang_name"))

            biblio_text = self._clean_value(card.get("biblio"))
            reference_entry = {
                "uid": uid,
                "volpages": self._clean_volpage_string(card.get("volpages")),
                "alt_volpages": self._clean_volpage_string(card.get("alt_volpages")),
                "biblio_uid": self.biblio_map.get(biblio_text) if biblio_text else None,
                "verseNo": self._clean_value(card.get("verseNo")),
            }
            if any(
                v is not None and v != ""
                for k, v in reference_entry.items()
                if k != "uid"
            ):
                self.sutta_references_data.append(reference_entry)

            for trans in card.get("translations", []):
                self._add_author(trans)
                self._add_language(trans.get("lang"), trans.get("lang_name"))
                self.translations_data.append(
                    {
                        "translation_uid": self._clean_value(trans.get("id")),
                        "sc_uid": uid,
                        "author_uid": self._clean_value(trans.get("author_uid")),
                        "lang": self._clean_value(trans.get("lang")),
                        "title": self._clean_value(trans.get("title")),
                        "publication_date": self._clean_value(
                            trans.get("publication_date")
                        ),
                        "segmented": 1 if trans.get("segmented") else 0,
                        "has_comment": 1 if trans.get("has_comment") else 0,
                        "is_root": 1 if trans.get("is_root") else 0,
                        "file_path": None,
                    }
                )

        return self
