# Path: scripts/update_path_comments.py
import argparse
import configparser
import logging
from pathlib import Path

# Xác định thư mục gốc của dự án
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger(__name__)

def get_submodule_paths(root: Path) -> set:
    """Đọc file .gitmodules và trả về một set các đường dẫn submodule."""
    submodule_paths = set()
    gitmodules_path = root / ".gitmodules"
    if gitmodules_path.exists():
        try:
            config = configparser.ConfigParser()
            config.read(gitmodules_path)
            for section in config.sections():
                if config.has_option(section, "path"):
                    submodule_paths.add((root / config.get(section, "path")).resolve())
        except configparser.Error as e:
            log.warning(f"Không thể đọc file .gitmodules: {e}")
    return submodule_paths

def run_update(target_dir: Path):
    """
    Quét và cập nhật hoặc thêm comment # Path:, bỏ qua các thư mục được chỉ định,
    submodules, và bất kỳ file .py rỗng nào.
    """
    if not target_dir.is_dir():
        log.error(f"Lỗi: Không tìm thấy thư mục mục tiêu: {target_dir}")
        return

    submodule_paths = get_submodule_paths(PROJECT_ROOT)
    excluded_dirs = {".venv", "venv", "__pycache__", ".git"}
    
    log.info(f"🔎 Bắt đầu quét các file .py trong: {target_dir}")
    log.info(f"🚫 Sẽ bỏ qua các thư mục: {', '.join(excluded_dirs)}")
    if submodule_paths:
        relative_sub_paths = [p.relative_to(PROJECT_ROOT).as_posix() for p in submodule_paths]
        log.info(f"🚫 Sẽ bỏ qua các submodule: {', '.join(relative_sub_paths)}")

    all_files = list(target_dir.rglob("*.py"))
    files_to_process = []
    for file_path in all_files:
        abs_file_path = file_path.resolve()
        is_in_excluded_dir = any(part in excluded_dirs for part in file_path.relative_to(PROJECT_ROOT).parts)
        if is_in_excluded_dir:
            continue
        is_in_submodule = any(abs_file_path.is_relative_to(p) for p in submodule_paths)
        if is_in_submodule:
            continue
        files_to_process.append(file_path)

    processed_count = 0
    if not files_to_process:
        log.warning("Không tìm thấy file Python nào để xử lý (sau khi đã loại trừ).")
        return

    for file_path in files_to_process:
        relative_path = file_path.relative_to(PROJECT_ROOT)
        try:
            if file_path.samefile(PROJECT_ROOT / __file__):
                continue

            with file_path.open('r', encoding='utf-8') as f:
                lines = f.readlines()

            # --- BẮT ĐẦU THAY ĐỔI: Bỏ qua BẤT KỲ file .py rỗng nào ---
            is_empty_or_whitespace = all(not line.strip() for line in lines)
            if is_empty_or_whitespace:
                log.info(f"  -> Bỏ qua file rỗng: {relative_path.as_posix()}")
                continue
            # --- KẾT THÚC THAY ĐỔI ---
            
            correct_comment = f"# Path: {relative_path.as_posix()}\n"
            action = None
            
            # Logic cũ cho file rỗng đã được loại bỏ, vì nó được xử lý ở trên
            if lines[0].strip().startswith("# Path:"):
                if lines[0] != correct_comment:
                    action = "Cập nhật"
                    lines[0] = correct_comment
            else:
                action = "Thêm"
                lines.insert(0, correct_comment)

            if action:
                log.info(f"  -> {action} path trong: {relative_path.as_posix()}")
                with file_path.open('w', encoding='utf-8') as f:
                    f.writelines(lines)
                processed_count += 1
                
        except Exception as e:
            log.error(f"  -> Lỗi khi xử lý file {relative_path.as_posix()}: {e}")

    log.info("-" * 20)
    if processed_count > 0:
        log.info(f"✅ Hoàn tất! Đã xử lý {processed_count} file.")
    else:
        log.info("✅ Tất cả các file đã tuân thủ quy ước. Không cần thay đổi.")

def main():
    """Thiết lập và phân tích các tham số dòng lệnh."""
    parser = argparse.ArgumentParser(
        description="Tự động cập nhật hoặc thêm comment '# Path:' ở đầu các file Python."
    )
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