#!/usr/bin/env python3
import argparse
import configparser
import fnmatch
from pathlib import Path

# --- CẤU HÌNH ---
PROJECT_ROOT = Path.cwd()
DEFAULT_IGNORE = {
    ".venv", "venv", "__pycache__", ".git", 
    "node_modules", "dist", "build", "out"
}

# --- CÁC HÀM TRỢ GIÚP ---
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
            print(f"Warning: Could not parse .gitmodules file: {e}")
    return submodule_paths

def parse_gitignore(root: Path) -> list:
    # ... (Hàm này không thay đổi)
    gitignore_path = root / ".gitignore"
    patterns = []
    if gitignore_path.exists():
        with open(gitignore_path, 'r', encoding='utf-8') as f:
            for line in f:
                stripped_line = line.strip()
                if stripped_line and not stripped_line.startswith('#'):
                    patterns.append(stripped_line)
    return patterns

# --- BẮT ĐẦU THAY ĐỔI 1: Nâng cấp hàm is_path_ignored thành is_path_matched ---
def is_path_matched(path: Path, patterns: set, start_dir: Path) -> bool:
    """Kiểm tra xem một đường dẫn có khớp với pattern nào không (cho cả tên và đường dẫn)."""
    if not patterns:
        return False
    
    relative_path_str = path.relative_to(start_dir).as_posix()
    for pattern in patterns:
        if fnmatch.fnmatch(path.name, pattern) or fnmatch.fnmatch(relative_path_str, pattern):
            return True
    return False
# --- KẾT THÚC THAY ĐỔI 1 ---

def parse_comma_list(value: str | None) -> set:
    if not value: return set()
    return {item for item in value.split(',') if item != ''}

def run_update(files_to_scan: list):
    # ... (Hàm này không thay đổi)
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
            with file_path.open('r', encoding='utf-8') as f: lines = f.readlines()
            is_empty_or_whitespace = all(not line.strip() for line in lines)
            if is_empty_or_whitespace:
                print(f"  -> Skipping empty file: {relative_path.as_posix()}")
                continue
            original_lines = list(lines)
            correct_path_comment = f"{comment_prefix} Path: {relative_path.as_posix()}\n"
            line1_is_shebang = lines[0].startswith('#!')
            line1_is_path = lines[0].startswith(f"{comment_prefix} Path:")
            line2_is_path = False
            if len(lines) > 1 and lines[1].startswith(f"{comment_prefix} Path:"): line2_is_path = True
            if line1_is_shebang:
                if line2_is_path:
                    if lines[1] != correct_path_comment: lines[1] = correct_path_comment
                else: lines.insert(1, correct_path_comment)
            elif line1_is_path:
                if len(lines) > 1 and lines[1].startswith('#!'):
                    lines[0], lines[1] = lines[1], lines[0]
                    if lines[1] != correct_path_comment: lines[1] = correct_path_comment
                else:
                    if lines[0] != correct_path_comment: lines[0] = correct_path_comment
            else: lines.insert(0, correct_path_comment)
            if lines != original_lines:
                print(f"  -> Fixing header for: {relative_path.as_posix()}")
                with file_path.open('w', encoding='utf-8') as f: f.writelines(lines)
                processed_count += 1
        except Exception as e: print(f"  -> Error processing file {relative_path.as_posix()}: {e}")
    print("-" * 20)
    if processed_count > 0: print(f"✅ Done! Processed {processed_count} files.")
    else: print("✅ All files already conform to the convention. No changes needed.")

def main():
    parser = argparse.ArgumentParser(description="Automatically update or add '# Path:' comments to source files.")
    parser.add_argument("target_directory", nargs='?', default=None, help="Directory to scan (default: entire project, respecting .gitignore).")
    parser.add_argument("-e", "--extensions", default="py,js", help="File extensions to scan (default: 'py,js').")
    
    # --- BẮT ĐẦU THAY ĐỔI 2: Thêm tham số -I, --ignore ---
    parser.add_argument("-I", "--ignore", type=str, help="Comma-separated list of patterns to ignore.")
    args = parser.parse_args()
    
    use_gitignore = args.target_directory is None
    scan_path = PROJECT_ROOT / (args.target_directory or ".")
    
    submodule_paths = get_submodule_paths(PROJECT_ROOT)
    # Danh sách ignore cứng mặc định
    extensions_to_scan = [ext.strip() for ext in args.extensions.split(',')]
    
    # Hợp nhất các nguồn ignore
    gitignore_patterns = set()
    if use_gitignore:
        gitignore_patterns = set(parse_gitignore(PROJECT_ROOT))
        print("ℹ️ Default mode: Respecting .gitignore rules.")
    else:
        print(f"ℹ️ Specific path mode: Not using .gitignore for '{args.target_directory}'.")

    cli_ignore_patterns = parse_comma_list(args.ignore)
    # Danh sách ignore tổng hợp
    final_ignore_patterns = DEFAULT_IGNORE.union(gitignore_patterns).union(cli_ignore_patterns)
    # --- KẾT THÚC THAY ĐỔI 2 ---

    print(f"🔎 Scanning for *.{', *.'.join(extensions_to_scan)} in: {scan_path}")
    if final_ignore_patterns:
        print(f"🚫 Ignoring patterns: {', '.join(sorted(list(final_ignore_patterns)))}")

    all_files = []
    for ext in extensions_to_scan: all_files.extend(scan_path.rglob(f"*.{ext}"))
    
    files_to_process = []
    for file_path in all_files:
        if file_path.samefile(PROJECT_ROOT / __file__): continue
            
        abs_file_path = file_path.resolve()
        is_in_submodule = any(abs_file_path.is_relative_to(p) for p in submodule_paths)
        if is_in_submodule: continue
            
        # --- BẮT ĐẦU THAY ĐỔI 3: Dùng một lệnh kiểm tra ignore duy nhất ---
        if is_path_matched(file_path, final_ignore_patterns, PROJECT_ROOT):
            continue
        # --- KẾT THÚC THAY ĐỔI 3 ---
            
        files_to_process.append(file_path)
        
    run_update(files_to_process)

if __name__ == "__main__":
    main()