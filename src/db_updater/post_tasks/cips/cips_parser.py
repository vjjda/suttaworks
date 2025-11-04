# Path: src/db_updater/post_tasks/cips/cips_parser.py
import logging
from dataclasses import dataclass
from typing import List, Optional

__all__ = ["ParsedRow", "parse_row"]

log = logging.getLogger(__name__)


@dataclass
class ParsedRow:
    row_type: str
    main_topic: str
    context: Optional[str] = None
    sutta_uid: Optional[str] = None
    segment: Optional[str] = None
    xref_topic: Optional[str] = None


def parse_row(row: List[str]) -> ParsedRow:
    if not row or not row[0].strip():
        return ParsedRow(row_type="empty", main_topic="")

    main_topic = row[0].strip()
    raw_col3 = row[2].strip() if len(row) > 2 else ""

    if raw_col3.startswith("xref "):
        xref_text = raw_col3.replace("xref ", "", 1).strip()
        if xref_text.lower() == main_topic.lower():
            log.warning(
                f"⚠️  Phát hiện xref tự tham chiếu: Chủ đề '{main_topic}' có xref trỏ về chính nó."
            )
        return ParsedRow(row_type="xref", main_topic=main_topic, xref_topic=xref_text)

    elif raw_col3.startswith("CUSTOM:"):
        parts = raw_col3.split(":")
        if len(parts) >= 4:
            context = f"-- {parts[2].strip()}"
            url_part = parts[-1]
            path_after_domain = url_part.split("/", 1)[-1]
            sutta_uid = path_after_domain.split("/")[0].lower()
            return ParsedRow(
                row_type="custom",
                main_topic=main_topic,
                context=context,
                sutta_uid=sutta_uid,
                segment="",
            )
        else:
            log.warning(f"Dòng CUSTOM không đúng định dạng, bỏ qua: {row}")
            return ParsedRow(row_type="invalid", main_topic=main_topic)

    elif len(row) > 2 and row[1].strip():
        context = row[1].strip()
        sutta_ref = raw_col3
        parts = sutta_ref.split(":", 1)
        sutta_uid = parts[0].lower()
        segment = parts[1] if len(parts) > 1 else ""
        return ParsedRow(
            row_type="sutta",
            main_topic=main_topic,
            context=context,
            sutta_uid=sutta_uid,
            segment=segment,
        )

    return ParsedRow(row_type="topic", main_topic=main_topic)
