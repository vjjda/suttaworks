# Path: /src/db_updater/handlers/api_handler.py
import httpx
from pathlib import Path
import time
import logging
import json

log = logging.getLogger(__name__)

def process_api_data(api_config: dict, destination_dir: Path):
    """
    Tải về và làm đẹp các file JSON từ API bằng cách kết hợp base_url và danh sách keywords.
    """
    log.info(f"Thư mục gốc cho suttaplex: {destination_dir}")

    # --- THAY ĐỔI 1: Lấy base_url và các nhóm từ config ---
    base_url = api_config.get("base_url")
    if not base_url:
        log.error("Cấu hình API cho suttaplex thiếu 'base_url'. Vui lòng kiểm tra updater_config.yaml.")
        return
        
    groups = api_config.get("groups", {})
    if not groups:
        log.warning("Không tìm thấy 'groups' nào trong cấu hình API cho suttaplex.")
        return

    total_files = sum(len(keywords) for keywords in groups.values())
    count = 0

    # --- THAY ĐỔI 2: Vòng lặp đơn giản hơn ---
    # Lặp qua các nhóm như 'sutta', 'kn-late', 'vinaya'
    for group_name, keywords in groups.items():
        # Tên nhóm trong config giờ đã là tên thư mục mong muốn
        group_destination_dir = destination_dir / group_name
        group_destination_dir.mkdir(parents=True, exist_ok=True)
        
        log.info(f"Đang xử lý nhóm: '{group_name}'. Lưu vào: {group_destination_dir}")

        # Lặp qua danh sách các keyword ('dn', 'mn', ...)
        for name in keywords:
            count += 1
            # Tạo URL động
            url = f"{base_url}{name}"
            log.debug(f"({count}/{total_files}) Đang tải {name} từ {url}")
            
            dest_file = group_destination_dir / f"{name}.json"
            
            try:
                response = httpx.get(url, follow_redirects=True, timeout=30.0)
                response.raise_for_status()
                
                raw_json = json.loads(response.text)
                
                with open(dest_file, 'w', encoding='utf-8') as f:
                    json.dump(raw_json, f, indent=2, ensure_ascii=False)
                
                log.debug(f"  -> Đã lưu thành công và làm đẹp file {dest_file.name}")

            except json.JSONDecodeError:
                log.error(f"  -> LỖI: Nội dung tải về từ {url} không phải là JSON hợp lệ. Đã lưu file thô để kiểm tra.")
                with open(dest_file, 'w', encoding='utf-8') as f:
                    f.write(response.text)
            except httpx.HTTPStatusError as e:
                log.error(f"  -> LỖI HTTP khi tải {name}. Mã lỗi: {e.response.status_code}", exc_info=True)
            except httpx.RequestError as e:
                log.error(f"  -> LỖI MẠNG khi kết nối tới {e.request.url}", exc_info=True)
            except Exception:
                log.exception(f"  -> LỖI KHÔNG XÁC ĐỊNH khi tải {name}")
            
            time.sleep(0.1)
        log.info(f"Hoàn tất xử lý nhóm: {group_name}.")