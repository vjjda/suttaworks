import json
import logging
from pathlib import Path
from itertools import combinations
from collections import defaultdict

log = logging.getLogger(__name__)

def _parse_sutta_id(full_id: str) -> tuple[str, str]:
    """Tách mã kinh gốc ra khỏi mã định danh đầy đủ."""
    # Ví dụ: 'mn10#47.1' -> 'mn10'
    # Ví dụ: '~an8.1#4.1-#7.1' -> 'an8.1'
    cleaned_id = full_id.lstrip('~')
    return cleaned_id.split('#')[0]

def process_parallels_data(task_config: dict, project_root: Path):
    """
    Đọc file parallels.json gốc, xử lý và chuyển đổi nó thành một từ điển tra cứu
    với các mối quan hệ một chiều và hai chiều được áp dụng đúng.
    """
    try:
        input_path_str = task_config['path']
        output_path_str = task_config['output']
        
        input_path = project_root / input_path_str
        output_path = project_root / output_path_str
        
        log.info(f"Bắt đầu xử lý file parallels: {input_path}")

        if not input_path.exists():
            log.error(f"Không tìm thấy file input: {input_path}")
            return

        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Sử dụng defaultdict để việc thêm key mới dễ dàng hơn
        sutta_map = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

        for group in data:
            relation_type = list(group.keys())[0]
            id_list = group[relation_type]

            if relation_type == "parallels":
                # Tách thành 2 nhóm: full (không có ~) và resembling (có ~)
                full_list = [i for i in id_list if not i.startswith('~')]
                resembling_list = [i for i in id_list if i.startswith('~')]

                # 1. Xử lý full parallels (quan hệ hai chiều)
                for source_id, target_id in combinations(full_list, 2):
                    base_source = _parse_sutta_id(source_id)
                    base_target = _parse_sutta_id(target_id)
                    sutta_map[base_source]['full_parallel'][source_id].append(target_id)
                    sutta_map[base_target]['full_parallel'][target_id].append(source_id)

                # 2. Xử lý resembling parallels (quan hệ một chiều)
                # Kinh gốc (full_list) sẽ trỏ đến kinh tương tự (resembling_list)
                if resembling_list:
                    # Bỏ dấu ~ khỏi các mục resembling
                    cleaned_resembling_list = [i.lstrip('~') for i in resembling_list]
                    for source_id in full_list:
                        base_source = _parse_sutta_id(source_id)
                        sutta_map[base_source]['resembling'][source_id].extend(cleaned_resembling_list)

            elif relation_type in ["mentions", "retells"]:
                # Xử lý mentions/retells (quan hệ hai chiều)
                for source_id, target_id in combinations(id_list, 2):
                    base_source = _parse_sutta_id(source_id)
                    base_target = _parse_sutta_id(target_id)
                    sutta_map[base_source][relation_type][source_id].append(target_id)
                    sutta_map[base_target][relation_type][target_id].append(source_id)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(sutta_map, f, ensure_ascii=False, indent=2)
        
        log.info(f"✅ Đã xử lý và lưu thành công file parallels vào: {output_path}")

    except KeyError as e:
        log.error(f"Lỗi cấu hình cho tác vụ 'parallels': thiếu key '{e}'")
    except Exception as e:
        log.exception(f"Đã xảy ra lỗi không mong muốn khi xử lý parallels: {e}")