# Path: scripts/update_path_comments.py
import argparse
import configparser
import logging
from pathlib import Path
import fnmatch

# Xác định thư mục gốc của dự án
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger(__name__)

def get_submodule_paths(root: Path) -> set:
    # ... (Hàm này không thay đổi)
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

# --- BẮT ĐẦU THAY ĐỔI 1: Hàm đọc .gitignore ---
def parse_gitignore(root: Path) -> list:
    """Đọc và phân tích file .gitignore, trả về danh sách các pattern."""
    gitignore_path = root / ".gitignore"
    patterns = []
    if gitignore_path.exists():
        with open(gitignore_path, 'r', encoding='utf-8') as f:
            for line in f:
                stripped_line = line.strip()
                if stripped_line and not stripped_line.startswith('#'):
                    # Đơn giản hóa: xử lý các pattern cơ bản, thêm / vào cuối nếu là thư mục
                    patterns.append(stripped_line)
    return patterns

def is_path_ignored(path: Path, gitignore_patterns: list, root: Path) -> bool:
    """Kiểm tra xem một đường dẫn có khớp với pattern nào trong .gitignore không."""
    relative_path_str = str(path.relative_to(root).as_posix())
    for pattern in gitignore_patterns:
        # Xử lý trường hợp pattern là thư mục (e.g., "node_modules/")
        is_dir_pattern = pattern.endswith('/')
        clean_pattern = pattern.rstrip('/')
        
        # Nếu là pattern thư mục, kiểm tra xem đường dẫn có nằm trong thư mục đó không
        if is_dir_pattern:
            if fnmatch.fnmatch(relative_path_str + '/', clean_pattern + '/') or \
               relative_path_str.startswith(clean_pattern + '/'):
                return True
        # Nếu là pattern file/tổng quát
        else:
            if fnmatch.fnmatch(path.name, pattern) or fnmatch.fnmatch(relative_path_str, pattern):
                return True
    return False
# --- KẾT THÚC THAY ĐỔI 1 ---

def run_update(files_to_scan: list):
    # ... (Hàm này không thay đổi nhiều, chỉ logic xử lý bên trong)
    processed_count = 0
    if not files_to_scan:
        log.warning("Không tìm thấy file nào để xử lý (sau khi đã loại trừ).")
        return

    for file_path in files_to_scan:
        relative_path = file_path.relative_to(PROJECT_ROOT)
        try:
            comment_prefix = None
            if file_path.suffix == '.py': comment_prefix = '#'
            elif file_path.suffix == '.js': comment_prefix = '//'
            if not comment_prefix: continue

            with file_path.open('r', encoding='utf-8') as f: lines = f.readlines()

            is_empty_or_whitespace = all(not line.strip() for line in lines)
            if is_empty_or_whitespace:
                log.info(f"  -> Bỏ qua file rỗng: {relative_path.as_posix()}")
                continue
            
            correct_comment = f"{comment_prefix} Path: {relative_path.as_posix()}\n"
            action = None
            
            if lines[0].strip().startswith(f"{comment_prefix} Path:"):
                if lines[0] != correct_comment:
                    action = "Cập nhật"
                    lines[0] = correct_comment
            else:
                action = "Thêm"
                lines.insert(0, correct_comment)

            if action:
                log.info(f"  -> {action} path trong: {relative_path.as_posix()}")
                with file_path.open('w', encoding='utf-8') as f: f.writelines(lines)
                processed_count += 1
                
        except Exception as e:
            log.error(f"  -> Lỗi khi xử lý file {relative_path.as_posix()}: {e}")

    log.info("-" * 20)
    if processed_count > 0: log.info(f"✅ Hoàn tất! Đã xử lý {processed_count} file.")
    else: log.info("✅ Tất cả các file đã tuân thủ quy ước. Không cần thay đổi.")

def main():
    parser = argparse.ArgumentParser(description="Tự động cập nhật hoặc thêm comment 'Path:' ở đầu các file mã nguồn.")
    parser.add_argument("target_directory", nargs='?', default=None, help="Đường dẫn thư mục cần quét (mặc định: toàn bộ project, tôn trọng .gitignore).")
    parser.add_argument("-e", "--extensions", default="py,js", help="Các đuôi file cần quét (mặc định: 'py,js').")
    args = parser.parse_args()
    
    # --- BẮT ĐẦU THAY ĐỔI 2: Logic xử lý .gitignore có điều kiện ---
    use_gitignore = args.target_directory is None
    scan_path = PROJECT_ROOT / (args.target_directory or ".")
    
    submodule_paths = get_submodule_paths(PROJECT_ROOT)
    excluded_dirs = {".venv", "venv", "__pycache__", ".git", "node_modules", "dist", "build", "out"}
    extensions_to_scan = [ext.strip() for ext in args.extensions.split(',')]
    
    gitignore_patterns = []
    if use_gitignore:
        gitignore_patterns = parse_gitignore(PROJECT_ROOT)
        log.info("ℹ️ Chế độ mặc định: Sẽ tôn trọng các quy tắc trong .gitignore.")
    else:
        log.info(f"ℹ️ Chế độ chỉ định đường dẫn: Sẽ KHÔNG sử dụng .gitignore cho '{args.target_directory}'.")

    log.info(f"🔎 Bắt đầu quét các file *.{', *.'.join(extensions_to_scan)} trong: {scan_path}")
    log.info(f"🚫 Sẽ bỏ qua các thư mục cứng: {', '.join(excluded_dirs)}")

    all_files = []
    for ext in extensions_to_scan: all_files.extend(scan_path.rglob(f"*.{ext}"))
    
    files_to_process = []
    for file_path in all_files:
        if file_path.samefile(PROJECT_ROOT / __file__): continue
            
        abs_file_path = file_path.resolve()
        
        # Quy tắc loại trừ cứng luôn được áp dụng
        is_in_excluded_dir = any(part in excluded_dirs for part in file_path.relative_to(PROJECT_ROOT).parts)
        if is_in_excluded_dir: continue
        is_in_submodule = any(abs_file_path.is_relative_to(p) for p in submodule_paths)
        if is_in_submodule: continue
            
        # Áp dụng .gitignore chỉ khi ở chế độ mặc định
        if use_gitignore and is_path_ignored(file_path, gitignore_patterns, PROJECT_ROOT):
            continue
            
        files_to_process.append(file_path)
    # --- KẾT THÚC THAY ĐỔI 2 ---
        
    run_update(files_to_process)

if __name__ == "__main__":
    main()