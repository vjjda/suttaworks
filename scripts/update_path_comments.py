#!/usr/bin/env python3
import argparse
import configparser
import fnmatch
from pathlib import Path

# --- CẤU HÌNH ---
PROJECT_ROOT = Path.cwd()

# --- CÁC HÀM TRỢ GIÚP (Không thay đổi) ---
def get_submodule_paths(root: Path) -> set:
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
            print(f"Warning: Could not parse .gitmodules file: {e}")
    return submodule_paths

def parse_gitignore(root: Path) -> list:
    gitignore_path = root / ".gitignore"
    patterns = []
    if gitignore_path.exists():
        with open(gitignore_path, 'r', encoding='utf-8') as f:
            for line in f:
                stripped_line = line.strip()
                if stripped_line and not stripped_line.startswith('#'):
                    patterns.append(stripped_line)
    return patterns

def is_path_ignored(path: Path, gitignore_patterns: list, root: Path) -> bool:
    relative_path_str = str(path.relative_to(root).as_posix())
    for pattern in gitignore_patterns:
        is_dir_pattern = pattern.endswith('/')
        clean_pattern = pattern.rstrip('/')
        if is_dir_pattern:
            if fnmatch.fnmatch(relative_path_str + '/', clean_pattern + '/') or \
               relative_path_str.startswith(clean_pattern + '/'):
                return True
        else:
            if fnmatch.fnmatch(path.name, pattern) or fnmatch.fnmatch(relative_path_str, pattern):
                return True
    return False

# --- BẮT ĐẦU HÀM run_update ĐÃ ĐƯỢC VIẾT LẠI HOÀN TOÀN ---
def run_update(files_to_scan: list):
    """
    Quét và sắp xếp/cập nhật/thêm comment Path, ưu tiên cho dòng shebang.
    """
    processed_count = 0
    if not files_to_scan:
        print("Warning: No files to process (after exclusions).")
        return

    for file_path in files_to_scan:
        relative_path = file_path.relative_to(PROJECT_ROOT)
        try:
            comment_prefix = None
            if file_path.suffix == '.py': comment_prefix = '#'
            elif file_path.suffix == '.js': comment_prefix = '//'
            if not comment_prefix: continue

            with file_path.open('r', encoding='utf-8') as f:
                lines = f.readlines()

            is_empty_or_whitespace = all(not line.strip() for line in lines)
            if is_empty_or_whitespace:
                print(f"  -> Skipping empty file: {relative_path.as_posix()}")
                continue
            
            original_lines = list(lines)
            correct_path_comment = f"{comment_prefix} Path: {relative_path.as_posix()}\n"
            
            # Xác định trạng thái của 2 dòng đầu
            line1_is_shebang = lines[0].startswith('#!')
            line1_is_path = lines[0].startswith(f"{comment_prefix} Path:")
            
            line2_is_path = False
            if len(lines) > 1 and lines[1].startswith(f"{comment_prefix} Path:"):
                line2_is_path = True

            # Áp dụng các quy tắc
            if line1_is_shebang:
                if line2_is_path:
                    # Đúng trật tự, chỉ cập nhật nếu cần
                    if lines[1] != correct_path_comment:
                        lines[1] = correct_path_comment
                else:
                    # Thiếu Path, chèn vào dòng 2
                    lines.insert(1, correct_path_comment)
            elif line1_is_path:
                # Dòng 1 là Path, kiểm tra xem dòng 2 có phải shebang không
                if len(lines) > 1 and lines[1].startswith('#!'):
                    # Sai trật tự, đảo vị trí
                    lines[0], lines[1] = lines[1], lines[0]
                    # Sau khi đảo, dòng 2 là Path, cập nhật nếu cần
                    if lines[1] != correct_path_comment:
                        lines[1] = correct_path_comment
                else:
                    # Chỉ có Path ở dòng 1, cập nhật nếu cần
                    if lines[0] != correct_path_comment:
                        lines[0] = correct_path_comment
            else:
                # Không có cả hai, chèn Path vào dòng 1
                lines.insert(0, correct_path_comment)

            # Chỉ ghi lại file nếu có sự thay đổi
            if lines != original_lines:
                print(f"  -> Fixing header for: {relative_path.as_posix()}")
                with file_path.open('w', encoding='utf-8') as f:
                    f.writelines(lines)
                processed_count += 1
                
        except Exception as e:
            print(f"  -> Error processing file {relative_path.as_posix()}: {e}")

    print("-" * 20)
    if processed_count > 0:
        print(f"✅ Done! Processed {processed_count} files.")
    else:
        print("✅ All files already conform to the convention. No changes needed.")
# --- KẾT THÚC HÀM run_update ---

def main():
    # ... (Hàm main không thay đổi, giữ nguyên như phiên bản cuối cùng)
    parser = argparse.ArgumentParser(description="A smart directory tree generator with support for a .treeconfig file.")
    parser.add_argument("start_path", nargs='?', default=None, help="Starting path (file or directory). Defaults to the entire project, respecting .gitignore.")
    parser.add_argument("-e", "--extensions", default="py,js", help="File extensions to scan (default: 'py,js').")
    args = parser.parse_args()
    use_gitignore = args.start_path is None
    scan_path = PROJECT_ROOT / (args.start_path or ".")
    submodule_paths = get_submodule_paths(PROJECT_ROOT)
    excluded_dirs = {".venv", "venv", "__pycache__", ".git", "node_modules", "dist", "build", "out"}
    extensions_to_scan = [ext.strip() for ext in args.extensions.split(',')]
    gitignore_patterns = []
    if use_gitignore:
        gitignore_patterns = parse_gitignore(PROJECT_ROOT)
        print("ℹ️ Default mode: Respecting .gitignore rules.")
    else:
        print(f"ℹ️ Specific path mode: Not using .gitignore for '{args.start_path}'.")
    print(f"🔎 Scanning for *.{', *.'.join(extensions_to_scan)} in: {scan_path}")
    print(f"🚫 Ignoring hard-coded directories: {', '.join(excluded_dirs)}")
    all_files = []
    for ext in extensions_to_scan: all_files.extend(scan_path.rglob(f"*.{ext}"))
    files_to_process = []
    for file_path in all_files:
        if file_path.samefile(PROJECT_ROOT / __file__): continue
        abs_file_path = file_path.resolve()
        is_in_excluded_dir = any(part in excluded_dirs for part in file_path.relative_to(PROJECT_ROOT).parts)
        if is_in_excluded_dir: continue
        is_in_submodule = any(abs_file_path.is_relative_to(p) for p in submodule_paths)
        if is_in_submodule: continue
        if use_gitignore and is_path_ignored(file_path, gitignore_patterns, PROJECT_ROOT):
            continue
        files_to_process.append(file_path)
    run_update(files_to_process)

if __name__ == "__main__":
    main()