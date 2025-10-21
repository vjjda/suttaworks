# Path: scripts/update_path_comments.py
import argparse
import logging
from pathlib import Path

# Xác định thư mục gốc của dự án
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger(__name__)

def run_update(target_dir: Path):
    """Quét và cập nhật các comment # Path: trong một thư mục mục tiêu được chỉ định."""
    if not target_dir.is_dir():
        log.error(f"Lỗi: Không tìm thấy thư mục mục tiêu: {target_dir}")
        return

    log.info(f"🔎 Bắt đầu quét các file .py trong: {target_dir}")
    
    files_to_update = list(target_dir.rglob("*.py"))
    updated_count = 0

    if not files_to_update:
        log.warning("Không tìm thấy file Python nào để xử lý.")
        return

    for file_path in files_to_update:
        try:
            # Bỏ qua chính file script này
            if file_path.samefile(PROJECT_ROOT / __file__):
                continue

            with file_path.open('r', encoding='utf-8') as f:
                lines = f.readlines()

            if not lines or not lines[0].strip().startswith("# Path:"):
                continue
            
            # --- THAY ĐỔI 1: Bỏ dấu "/" ở đầu ---
            relative_path = file_path.relative_to(PROJECT_ROOT)
            correct_comment = f"# Path: {relative_path.as_posix()}\n"

            if lines[0] != correct_comment:
                log.info(f"  -> Đang cập nhật: {relative_path.as_posix()}")
                lines[0] = correct_comment
                with file_path.open('w', encoding='utf-8') as f:
                    f.writelines(lines)
                updated_count += 1
                
        except Exception as e:
            log.error(f"  -> Lỗi khi xử lý file {file_path.name}: {e}")

    log.info("-" * 20)
    if updated_count > 0:
        log.info(f"✅ Hoàn tất! Đã cập nhật {updated_count} file.")
    else:
        log.info("✅ Tất cả các comment đường dẫn đã chính xác. Không cần cập nhật.")

def main():
    """Thiết lập và phân tích các tham số dòng lệnh."""
    parser = argparse.ArgumentParser(
        description="Tự động cập nhật comment '# Path:' ở đầu các file Python trong một thư mục."
    )
    # --- THAY ĐỔI 2: Đặt default là "." (thư mục hiện tại) ---
    parser.add_argument(
        "target_directory",
        nargs='?',
        default=".",
        help="Đường dẫn thư mục cần quét, tính từ gốc dự án (mặc định: toàn bộ project)."
    )
    args = parser.parse_args()
    
    scan_path = PROJECT_ROOT / args.target_directory
    run_update(scan_path)

if __name__ == "__main__":
    main()