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
            # --- THAY ĐỔI: Bỏ sort_keys=True vì chúng ta đã tự sort trước đó ---
            json.dump(data, f, ensure_ascii=False, indent=2)
        log.info(f"✅ Đã tạo file {file_type} thành công.")
    except IOError as e:
        log.error(f"Không thể ghi file {file_type}: {e}")

# --- THAY ĐỔI: Thêm hàm sắp xếp tùy chỉnh ---
def _custom_context_sort_key(context_str: str):
    """
    Tạo khóa sắp xếp để ưu tiên các context bắt đầu bằng '—' lên đầu.
    Trả về một tuple, Python sẽ sắp xếp theo từng phần tử của tuple.
    ('—Abc') -> (0, '—Abc')
    ('Xyz')   -> (1, 'Xyz')
    """
    if context_str.startswith('—'):
        return (0, context_str)
    return (1, context_str)

def process_cips_csv_to_json(config: Dict, project_root: Path):
    # ... (phần đọc config và xử lý file TSV vẫn giữ nguyên) ...
    try:
        tsv_path = project_root / config['path']
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
                topic_index_data.setdefault(main_topic, {'contexts': {}, 'xref': []})

                raw_col3 = row[2].strip() if len(row) > 2 else ""
                
                is_sutta_ref = False
                context, sutta_uid, segment = None, None, None

                if raw_col3.startswith('xref '):
                    xref_text = raw_col3.replace('xref ', '', 1).strip()
                    
                    # --- THAY ĐỔI: Thêm khối kiểm tra self-referencing xref ---
                    if xref_text.lower() == main_topic.lower():
                        log.warning(f"⚠️  Phát hiện xref tự tham chiếu (self-referencing): Chủ đề '{main_topic}' có xref trỏ về chính nó.")
                    
                    topic_index_data[main_topic]['xref'].append(xref_text)

                elif raw_col3.startswith('CUSTOM:'):
                    parts = raw_col3.split(':')
                    if len(parts) >= 4:
                        is_sutta_ref = True
                        context = f"—{parts[2].strip()}"
                        url_part = parts[-1]
                        path_after_domain = url_part.split('/', 1)[-1]
                        sutta_uid = path_after_domain.split('/')[0].lower()
                        segment = ""
                    else:
                        log.warning(f"Dòng CUSTOM không đúng định dạng, bỏ qua: {row}")

                elif len(row) > 2 and row[1].strip():
                    is_sutta_ref = True
                    context = row[1].strip()
                    sutta_ref = raw_col3
                    
                    parts = sutta_ref.split(':', 1)
                    sutta_uid = parts[0].lower()
                    segment = parts[1] if len(parts) > 1 else ""

                if is_sutta_ref and all([context, sutta_uid]):
                    ref_obj = {"sutta_uid": sutta_uid, "segment": segment}
                    topic_index_data[main_topic]['contexts'].setdefault(context, []).append(ref_obj)
                    
                    sutta_index_data.setdefault(sutta_uid, {}).setdefault(main_topic, {}).setdefault(context, []).append(segment)

    except Exception as e:
        log.error(f"Lỗi khi xử lý file TSV: {e}", exc_info=True)
        return
    
    # --- THAY ĐỔI: Sắp xếp dữ liệu trước khi ghi file ---
    
    # 1. Sắp xếp topic_index_data
    # Sắp xếp các topic chính (cấp 1)
    sorted_topic_index = {topic: topic_index_data[topic] for topic in sorted(topic_index_data.keys())}
    # Sắp xếp các context (cấp 2) bằng hàm tùy chỉnh
    for topic_data in sorted_topic_index.values():
        contexts = topic_data['contexts']
        sorted_contexts = {ctx: contexts[ctx] for ctx in sorted(contexts.keys(), key=_custom_context_sort_key)}
        topic_data['contexts'] = sorted_contexts
        
    # 2. Sắp xếp sutta_index_data (sắp xếp alphabet thông thường ở mọi cấp)
    sorted_sutta_index = {uid: sutta_index_data[uid] for uid in sorted(sutta_index_data.keys())}
    for uid_data in sorted_sutta_index.values():
        for topic, topic_data in uid_data.items():
            sorted_contexts_in_sutta = {ctx: topic_data[ctx] for ctx in sorted(topic_data.keys())}
            uid_data[topic] = sorted_contexts_in_sutta

    # Ghi file với dữ liệu đã được sắp xếp
    _write_json_file(sorted_topic_index, topic_output_file, "topic-index")
    _write_json_file(sorted_sutta_index, sutta_output_file, "sutta-index")