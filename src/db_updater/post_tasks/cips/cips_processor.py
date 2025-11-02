# Path: src/db_updater/post_tasks/cips/cips_processor.py
import csv
import logging
from pathlib import Path
from typing import Dict, Tuple

from .cips_parser import parse_row

__all__ = ["process_tsv"]

log = logging.getLogger(__name__)


def process_tsv(tsv_path: Path) -> Tuple[Dict, Dict]:
    topic_index = {}
    sutta_index = {}

    try:
        with open(tsv_path, mode="r", encoding="utf-8") as tsvfile:
            reader = csv.reader(tsvfile, delimiter="\t")
            for row in reader:
                parsed = parse_row(row)

                if parsed.row_type == "empty" or parsed.row_type == "invalid":
                    continue

                topic_index.setdefault(
                    parsed.main_topic, {"contexts": {}, "also_see": []}
                )

                if parsed.row_type == "xref":
                    topic_index[parsed.main_topic]["also_see"].append(parsed.xref_topic)

                elif parsed.row_type in ["sutta", "custom"]:
                    if not all([parsed.context, parsed.sutta_uid]):
                        continue

                    context_dict = topic_index[parsed.main_topic][
                        "contexts"
                    ].setdefault(parsed.context, {})
                    segment_list = context_dict.setdefault(parsed.sutta_uid, [])
                    if parsed.segment:
                        segment_list.append(parsed.segment)

                    sutta_index.setdefault(parsed.sutta_uid, {}).setdefault(
                        parsed.main_topic, {}
                    ).setdefault(parsed.context, [])
                    if parsed.segment:
                        sutta_index[parsed.sutta_uid][parsed.main_topic][
                            parsed.context
                        ].append(parsed.segment)

    except Exception as e:
        log.error(f"Lỗi khi xử lý file TSV: {e}", exc_info=True)
        return {}, {}

    return topic_index, sutta_index
