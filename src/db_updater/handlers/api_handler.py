# Path: /src/db_updater/handlers/api_handler.py
import httpx
from pathlib import Path
import time
import logging
import json

log = logging.getLogger(__name__)

def process_api_data(api_config: dict, destination_dir: Path):
    """
    Tải về và làm đẹp các file JSON từ API, lưu vào các thư mục con
    dựa trên nhóm (sutta, vinaya).
    """
    # Không cần tạo thư mục đích chính ở đây nữa,
    # vì chúng ta sẽ tạo các thư mục con bên trong nó.
    log.info(f"Thư mục gốc cho suttaplex: {destination_dir}")

    total_files = sum(len(items) for group in api_config.values() for items in group)
    count = 0

    # Lặp qua các nhóm như 'sutta-json', 'vinaya-json'
    for group_name, items_list in api_config.items():
        # ---- THAY ĐỔI: Tạo thư mục con cho mỗi nhóm ----
        # Chuyển 'sutta-json' thành 'sutta', 'vinaya-json' thành 'vinaya'
        subdirectory_name = group_name.replace('-json', '')
        group_destination_dir = destination_dir / subdirectory_name
        group_destination_dir.mkdir(parents=True, exist_ok=True)
        # ------------------------------------------------

        log.info(f"Đang xử lý nhóm: {group_name}. Lưu vào: {group_destination_dir}")

        for item in items_list:
            for name, url in item.items():
                count += 1
                log.debug(f"({count}/{total_files}) Đang tải {name} từ {url}")
                # ---- THAY ĐỔI: Sử dụng đường dẫn thư mục con ----
                dest_file = group_destination_dir / f"{name}.json"
                # ---------------------------------------------
                
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