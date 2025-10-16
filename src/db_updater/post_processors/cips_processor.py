# Path: src/db_updater/post_processors/cips_processor.py
import logging
import json
import csv
from pathlib import Path
from typing import Dict, List

log = logging.getLogger(__name__)

def _write_json_file(data: Dict, output_file: Path, file_type: str):
    """Hàm phụ trợ để ghi một file JSON."""
    if not data:
        log.warning(f"Không có dữ liệu để ghi cho file {file_type}.")
        return

    log.info(f"Đang ghi {len(data)} mục vào file {file_type}: {output_file}")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            # Ghi file JSON với indent=2 và sort_keys=True để sắp xếp
            json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        log.info(f"✅ Đã tạo file {file_type} thành công.")
    except IOError as e:
        log.error(f"Không thể ghi file {file_type}: {e}")

def process_cips_csv_to_json(config: Dict, project_root: Path):
    """
    Đọc file TSV từ CIPS, phân tích và tạo ra hai file JSON:
    1. topic-index: Chỉ mục tra cứu theo chủ đề.
    2. sutta-index: Chỉ mục đảo ngược tra cứu theo sutta_uid.
    """
    try:
        tsv_path = project_root / config['path']
        # --- THAY ĐỔI 1: Đọc cấu hình output mới ---
        output_configs = {key: project_root / value
                          for item in config.get('output', [])
                          for key, value in item.items()}
        
        topic_output_file = output_configs.get('topic-index')
        sutta_output_file = output_configs.get('sutta-index')

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
        with open(tsv_path, mode='r', encoding='utf-8') as tsvfile:
            reader = csv.reader(tsvfile, delimiter='\t')
            for row in reader:
                if not row or not row[0].strip():
                    continue
                
                main_topic = row[0].strip()
                
                # --- Xử lý cho topic-index ---
                topic_index_data.setdefault(main_topic, {'contexts': {}, 'xref': []})

                if len(row) > 2 and row[2].strip().startswith('xref '):
                    xref_text = row[2].strip().replace('xref ', '', 1).strip()
                    topic_index_data[main_topic]['xref'].append(xref_text)

                elif len(row) > 2:
                    context = row[1].strip()
                    sutta_ref = row[2].strip()

                    if not context or not sutta_ref:
                        continue

                    parts = sutta_ref.split(':', 1)
                    sutta_uid = parts[0].lower()
                    segment = parts[1] if len(parts) > 1 else ""
                    
                    # --- Cập nhật cả 2 chỉ mục ---
                    # 1. Thêm vào topic-index
                    ref_obj = {"sutta_uid": sutta_uid, "segment": segment}
                    topic_index_data[main_topic]['contexts'].setdefault(context, []).append(ref_obj)
                    
                    # 2. Thêm vào sutta-index (chỉ mục đảo ngược)
                    sutta_index_data.setdefault(sutta_uid, {}).setdefault(main_topic, {}).setdefault(context, []).append(segment)

    except Exception as e:
        log.error(f"Lỗi khi xử lý file TSV: {e}", exc_info=True)
        return
    
    # --- Ghi cả hai file output ---
    _write_json_file(topic_index_data, topic_output_file, "topic-index")
    _write_json_file(sutta_index_data, sutta_output_file, "sutta-index")