# Path: src/db_updater/post_tasks/cips_csv_task.py
import logging
import csv
from pathlib import Path
from typing import Dict, List, Any
from natsort import natsorted

log = logging.getLogger(__name__)

# ... (hàm _write_csv_file không đổi) ...
def _write_csv_file(data: List[Dict[str, Any]], output_file: Path, file_type: str):
    if not data:
        log.warning(f"Không có dữ liệu để ghi cho file CSV {file_type}.")
        return
    log.info(f"Đang ghi {len(data)} dòng vào file {file_type}: {output_file}")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        log.info(f"✅ Đã tạo file CSV {file_type} thành công.")
    except (IOError, IndexError) as e:
        log.error(f"Không thể ghi file CSV {file_type}: {e}")

def process_cips_to_csv(config: Dict, project_root: Path):
    # ... (phần đọc và xử lý file không đổi) ...
    try:
        tsv_path = project_root / config['path']
        output_paths = {key: project_root / value for item in config.get('output', []) for key, value in item.items()}
    except (KeyError, TypeError) as e:
        log.error(f"Lỗi cấu hình 'cips-csv': {e}")
        return

    if not tsv_path.is_file():
        log.error(f"File TSV nguồn không tồn tại: {tsv_path}")
        return

    log.info(f"Bắt đầu xử lý file TSV để tạo các file CSV từ: {tsv_path}")
    
    topic_index_data, sutta_index_data = {}, {}
    try:
        with open(tsv_path, mode='r', encoding='utf-8') as tsvfile:
            reader = csv.reader(tsvfile, delimiter='\t')
            for row in reader:
                if not row or not row[0].strip(): continue
                main_topic = row[0].strip()
                topic_index_data.setdefault(main_topic, {'contexts': {}, 'also_see': []})
                raw_col3 = row[2].strip() if len(row) > 2 else ""
                is_sutta_ref, context, sutta_uid, segment = False, None, None, None
                if raw_col3.startswith('xref '):
                    topic_index_data[main_topic]['also_see'].append(raw_col3.replace('xref ', '', 1).strip())
                elif raw_col3.startswith('CUSTOM:'):
                    parts = raw_col3.split(':')
                    if len(parts) >= 4:
                        is_sutta_ref = True
                        context, sutta_uid, segment = f"-- {parts[2].strip()}", parts[-1].split('/', 1)[-1].split('/')[0].lower(), ""
                elif len(row) > 2 and row[1].strip():
                    is_sutta_ref = True
                    context = row[1].strip()
                    parts = raw_col3.split(':', 1)
                    sutta_uid, segment = parts[0].lower(), parts[1] if len(parts) > 1 else ""
                if is_sutta_ref and all([context, sutta_uid]):
                    context_dict = topic_index_data[main_topic]['contexts'].setdefault(context, {})
                    segment_list = context_dict.setdefault(sutta_uid, [])
                    if segment: segment_list.append(segment)
                    if segment:
                        sutta_index_data.setdefault(sutta_uid, {}).setdefault(main_topic, {}).setdefault(context, []).append(segment)
                    else:
                        sutta_index_data.setdefault(sutta_uid, {}).setdefault(main_topic, {}).setdefault(context, [])
    except Exception as e:
        log.error(f"Lỗi khi xử lý file TSV: {e}", exc_info=True)
        return

    # --- Chuyển đổi và ghi ra các file CSV ---

    if output_paths.get('topics'):
        _write_csv_file([{"topic_name": topic} for topic in natsorted(topic_index_data.keys())], output_paths['topics'], 'topics')
    if output_paths.get('suttas'):
        _write_csv_file([{"sutta_uid": uid} for uid in natsorted(sutta_index_data.keys())], output_paths['suttas'], 'suttas')
    if output_paths.get('segments'):
        unique_segments = set()
        for topic_data in topic_index_data.values():
            for context_data in topic_data['contexts'].values():
                for sutta_uid, segments in context_data.items():
                    if segments:
                        for seg in segments: unique_segments.add((sutta_uid, seg))
                    else: unique_segments.add((sutta_uid, ""))
        segments_list = []
        for i, (sutta_uid, segment_id) in enumerate(sorted(list(unique_segments)), 1):
            segments_list.append({"ID": f"Seg-{i}", "sutta_uid": sutta_uid, "segment_id": segment_id})
        _write_csv_file(segments_list, output_paths['segments'], 'segments')

    if output_paths.get('links'):
        links_list = []
        for sutta_uid, topics_data in sutta_index_data.items():
            for topic_name in topics_data.keys():
                links_list.append({"sutta_uid": sutta_uid, "topic_name": topic_name})
        sorted_links = sorted(links_list, key=lambda x: (x['sutta_uid'], x['topic_name']))
        final_links_list = [{"ID": i, "sutta_uid": r['sutta_uid'], "topic_name": r['topic_name']} for i, r in enumerate(sorted_links, 1)]
        _write_csv_file(final_links_list, output_paths['links'], 'links')

    # --- THÊM MỚI: Tạo file CSV ánh xạ ngược ---
    if output_paths.get('reverse_links'):
        reverse_links_list = []
        for sutta_uid, topics_data in sutta_index_data.items():
            for topic_name in topics_data.keys():
                reverse_links_list.append({"topic_name": topic_name, "sutta_uid": sutta_uid})
        # Sắp xếp theo topic trước, sau đó là sutta
        sorted_reverse_links = sorted(reverse_links_list, key=lambda x: (x['topic_name'], x['sutta_uid']))
        _write_csv_file(sorted_reverse_links, output_paths['reverse_links'], 'reverse_links')