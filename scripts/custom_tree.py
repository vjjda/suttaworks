#!/usr/bin/env python3
import argparse
import configparser
from pathlib import Path

# Xác định thư mục gốc của dự án
PROJECT_ROOT = Path.cwd()
DEFAULT_IGNORE = {
    "__pycache__", ".venv", "venv", "node_modules", 
    "dist", "build", ".git"
}

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
        except configparser.Error:
            pass 
    return submodule_paths

def generate_tree(directory: Path, prefix: str = "", level: int = 0, max_level: int | None = None, 
                  ignore_list: set = None, submodules: set = None, prune_list: set = None,
                  dirs_only_list: set = None, is_in_dirs_only_zone: bool = False, counters: dict = None):
    """Hàm đệ quy để tạo và in ra cấu trúc cây thư mục."""
    if max_level is not None and level >= max_level:
        return

    try:
        contents = [path for path in directory.iterdir() if not path.name.startswith('.')]
    except FileNotFoundError:
        return

    dirs = sorted([d for d in contents if d.is_dir() and d.name not in ignore_list], key=lambda p: p.name.lower())
    
    files = []
    if not is_in_dirs_only_zone:
        files = sorted([f for f in contents if f.is_file() and f.name not in ignore_list], key=lambda p: p.name.lower())
    
    items_to_print = dirs + files
    pointers = ["├── "] * (len(items_to_print) - 1) + ["└── "]

    for pointer, path in zip(pointers, items_to_print):
        if path.is_dir(): counters['dirs'] += 1
        else: counters['files'] += 1
            
        is_submodule = path.is_dir() and path.name in submodules
        is_pruned = path.is_dir() and path.name in prune_list
        is_dirs_only_entry = path.is_dir() and path.name in dirs_only_list and not is_in_dirs_only_zone

        line = f"{prefix}{pointer}{path.name}{'/' if path.is_dir() else ''}"
        if is_submodule: line += " [submodule]"
        elif is_pruned: line += " [...]"
        elif is_dirs_only_entry: line += " [dirs only]"
        print(line)

        if path.is_dir() and not is_submodule and not is_pruned:
            extension = "│   " if pointer == "├── " else "    "
            next_is_in_dirs_only_zone = is_in_dirs_only_zone or is_dirs_only_entry
            generate_tree(path, prefix + extension, level + 1, max_level, ignore_list, submodules, prune_list, dirs_only_list, next_is_in_dirs_only_zone, counters)

def main():
    """Hàm chính để xử lý tham số và chạy script."""
    parser = argparse.ArgumentParser(description="Hiển thị cây thư mục, với các tùy chọn ignore, prune, và dirs-only.")
    parser.add_argument("start_path", nargs='?', default=".", help="Thư mục bắt đầu (mặc định: thư mục hiện tại).")
    parser.add_argument("-L", "--level", type=int, default=None, help="Giới hạn độ sâu hiển thị (mặc định: không giới hạn).")
    parser.add_argument("-I", "--ignore", type=str, default="", help="Danh sách thư mục/file cần bỏ qua, cách nhau bởi dấu phẩy.")
    parser.add_argument("-P", "--prune", type=str, default="", help="Danh sách thư mục cần 'cắt tỉa' (hiển thị nhưng không duyệt vào trong).")
    parser.add_argument("-d", "--dirs-only", nargs='?', const='_ALL_', default=None, type=str, help="Chỉ hiển thị thư mục. Có thể dùng độc lập (áp dụng toàn cục) hoặc theo sau bởi danh sách thư mục (e.g., 'src,build').")
    
    # --- THÊM MỚI ---
    parser.add_argument("-s", "--show-submodules", action='store_true', help="Hiển thị nội dung của các thư mục submodule (mặc định sẽ bị ẩn).")
    
    args = parser.parse_args()

    submodule_names = set()
    if not args.show_submodules:
        submodule_names = {p.name for p in get_submodule_paths(PROJECT_ROOT)}
        
    custom_ignore = set(args.ignore.split(',')) if args.ignore else set()
    
    # Sử dụng hằng số DEFAULT_IGNORE mới ở đây
    final_ignore_list = DEFAULT_IGNORE.union(custom_ignore)
    
    prune_list = set(args.prune.split(',')) if args.prune else set()
    
    global_dirs_only = args.dirs_only == '_ALL_'
    dirs_only_list = set()
    if args.dirs_only is not None and not global_dirs_only:
        dirs_only_list = set(args.dirs_only.split(','))

    start_dir = Path(args.start_path)
    
    level_info = "(Displaying full tree)" if args.level is None else f"(Displaying up to level {args.level})"
    mode_info = ", directories only" if global_dirs_only else ""
    print(f"{start_dir.resolve().name}/ {level_info}{mode_info}")
    
    counters = {'dirs': 0, 'files': 0}
    generate_tree(start_dir, max_level=args.level, ignore_list=final_ignore_list, 
                  submodules=submodule_names, prune_list=prune_list,
                  dirs_only_list=dirs_only_list, is_in_dirs_only_zone=global_dirs_only, counters=counters)

    files_info = "0 files (hidden)" if global_dirs_only and counters['files'] == 0 else f"{counters['files']} files"
    print(f"\n{counters['dirs']} directories, {files_info}")

if __name__ == "__main__":
    main()