# Path: src/db_updater/post_tasks/cips_task.py
import logging
import json
import csv
from pathlib import Path
from typing import Dict, List
from natsort import natsorted, natsort_keygen

log = logging.getLogger(__name__)


def _write_json_file(data: Dict, output_file: Path, file_type: str):
    if not data:
        log.warning(f"Không có dữ liệu để ghi cho file {file_type}.")
        return

    log.info(f"Đang ghi {len(data)} mục vào file {file_type}: {output_file}")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        log.info(f"✅ Đã tạo file {file_type} thành công.")
    except IOError as e:
        log.error(f"Không thể ghi file {file_type}: {e}")


def process_cips_csv_to_json(config: Dict, project_root: Path):

    try:
        tsv_path = project_root / config["path"]

        output_data = config.get("output", {})
        output_configs = {
            key: project_root / value for key, value in output_data.items()
        }

        topic_output_file = output_configs.get("topic-index")
        sutta_output_file = output_configs.get("sutta-index")

        if not topic_output_file or not sutta_output_file:
            log.error("Cấu hình output thiếu 'topic-index' hoặc 'sutta-index'.")
            return

    except (KeyError, TypeError) as e:
        log.error(f"Lỗi cấu hình 'cips-json': {e}")
        return

    if not tsv_path.is_file():
        log.error(f"File TSV nguồn không tồn tại: {tsv_path}")
        return

    log.info(f"Bắt đầu xử lý file TSV để tạo 2 chỉ mục từ: {tsv_path}")

    topic_index_data = {}
    sutta_index_data = {}

    try:
        with open(tsv_path, mode="r", encoding="utf-8") as tsvfile:
            reader = csv.reader(tsvfile, delimiter="\t")
            for row in reader:

                if not row or not row[0].strip():
                    continue

                main_topic = row[0].strip()
                topic_index_data.setdefault(
                    main_topic, {"contexts": {}, "also_see": []}
                )

                raw_col3 = row[2].strip() if len(row) > 2 else ""

                is_sutta_ref = False
                context, sutta_uid, segment = None, None, None

                if raw_col3.startswith("xref "):
                    xref_text = raw_col3.replace("xref ", "", 1).strip()
                    if xref_text.lower() == main_topic.lower():
                        log.warning(
                            f"⚠️  Phát hiện xref tự tham chiếu: Chủ đề '{main_topic}' có xref trỏ về chính nó."
                        )
                    topic_index_data[main_topic]["also_see"].append(xref_text)

                elif raw_col3.startswith("CUSTOM:"):
                    parts = raw_col3.split(":")
                    if len(parts) >= 4:
                        is_sutta_ref = True
                        context = f"-- {parts[2].strip()}"
                        url_part = parts[-1]
                        path_after_domain = url_part.split("/", 1)[-1]
                        sutta_uid = path_after_domain.split("/")[0].lower()
                        segment = ""
                    else:
                        log.warning(f"Dòng CUSTOM không đúng định dạng, bỏ qua: {row}")

                elif len(row) > 2 and row[1].strip():
                    is_sutta_ref = True
                    context = row[1].strip()
                    sutta_ref = raw_col3

                    parts = sutta_ref.split(":", 1)
                    sutta_uid = parts[0].lower()
                    segment = parts[1] if len(parts) > 1 else ""

                if is_sutta_ref and all([context, sutta_uid]):

                    context_dict = topic_index_data[main_topic]["contexts"].setdefault(
                        context, {}
                    )

                    segment_list = context_dict.setdefault(sutta_uid, [])
                    if segment:
                        segment_list.append(segment)

                    if segment:
                        sutta_index_data.setdefault(sutta_uid, {}).setdefault(
                            main_topic, {}
                        ).setdefault(context, []).append(segment)
                    else:
                        sutta_index_data.setdefault(sutta_uid, {}).setdefault(
                            main_topic, {}
                        ).setdefault(context, [])

    except Exception as e:
        log.error(f"Lỗi khi xử lý file TSV: {e}", exc_info=True)
        return

    sorted_topic_index = {}
    for topic in natsorted(topic_index_data.keys()):
        original_topic_data = topic_index_data[topic]
        new_topic_data = {}
        for key in natsorted(original_topic_data.keys()):
            if key == "also_see":
                new_topic_data[key] = natsorted(
                    list(dict.fromkeys(original_topic_data[key]))
                )

            elif key == "contexts":
                original_contexts = original_topic_data[key]
                sorted_contexts = {}

                for context_name in natsorted(original_contexts.keys()):
                    sutta_data = original_contexts[context_name]

                    sorted_sutta_data = {}
                    for sutta_uid in natsorted(sutta_data.keys()):

                        sorted_segments = natsorted(
                            list(dict.fromkeys(sutta_data[sutta_uid]))
                        )
                        sorted_sutta_data[sutta_uid] = sorted_segments
                    sorted_contexts[context_name] = sorted_sutta_data
                new_topic_data[key] = sorted_contexts

        sorted_topic_index[topic] = new_topic_data

    sorted_sutta_index = {}
    natural_key_gen = natsort_keygen()

    for uid in natsorted(sutta_index_data.keys()):
        uid_data = sutta_index_data[uid]

        def get_topic_sort_key(topic_name):
            contexts_data = uid_data[topic_name]
            all_segments = [
                seg for seg_list in contexts_data.values() for seg in seg_list
            ]

            if not all_segments:
                return (1, natural_key_gen(topic_name), ())

            first_segment = natsorted(all_segments)[0]

            return (0, natural_key_gen(first_segment), natural_key_gen(topic_name))

        sorted_topic_names = sorted(uid_data.keys(), key=get_topic_sort_key)

        sorted_topics_in_sutta = {}
        for topic in sorted_topic_names:
            topic_data = uid_data[topic]
            sorted_contexts_in_sutta = {
                ctx: topic_data[ctx] for ctx in natsorted(topic_data.keys())
            }
            for seg_list in sorted_contexts_in_sutta.values():
                seg_list.sort(key=natural_key_gen)
            sorted_topics_in_sutta[topic] = sorted_contexts_in_sutta

        sorted_sutta_index[uid] = sorted_topics_in_sutta

    _write_json_file(sorted_topic_index, topic_output_file, "topic-index")
    _write_json_file(sorted_sutta_index, sutta_output_file, "sutta-index")