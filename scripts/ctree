#!/usr/bin/env python3
import argparse
import configparser
import fnmatch
from pathlib import Path

# --- CẤU HÌNH MẶC ĐỊNH ---
PROJECT_ROOT = Path.cwd()
DEFAULT_IGNORE = {
    "__pycache__", ".venv", "venv", "node_modules", 
    ".git"
}
DEFAULT_PRUNE = {"dist", "build"}
DEFAULT_DIRS_ONLY = set()

# --- BẮT ĐẦU THAY ĐỔI 1: Nội dung template file cấu hình ---
CONFIG_TEMPLATE = """
; Configuration file for the custom_tree script.
; Uncomment lines you wish to use by removing the ';' symbol.
; Patterns support shell-like wildcards (e.g., *, ?, **).

[tree]

; --- DISPLAY ---

; level: Limit the depth of the directory tree.
; Example: level = 3
; level = 3

; show-submodules: Display the contents of submodule directories.
; Defaults to false. Set to true to enable.
; show-submodules = false


; --- FILTERING RULES ---

; ignore: Completely hide files/directories matching a pattern.
; Multiple patterns can be listed, separated by commas.
;
; Example (single names):   ignore = .DS_Store, thumbs.db
; Example (wildcards):      ignore = *.tmp, *.log
; Example (path patterns):  ignore = docs/drafts, src/**/temp
;
; ignore = 

; prune: Display a directory but do not traverse into it (shows '[...]').
; Useful for directories with many auto-generated files.
;
; Example (single names):        prune = dist, build
; Example (path with wildcard):  prune = */suttaplex/update
;
; prune = 

; dirs-only: Only display subdirectories inside directories matching a pattern.
; The entry directory will be marked with '[dirs only]'.
;
; dirs-only = assets, static
;
"""

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

def is_path_matched(path: Path, patterns: set, start_dir: Path) -> bool:
    # ... (Hàm này không thay đổi)
    if not patterns: return False
    relative_path_str = path.relative_to(start_dir).as_posix()
    for pattern in patterns:
        if fnmatch.fnmatch(path.name, pattern) or fnmatch.fnmatch(relative_path_str, pattern):
            return True
    return False

def parse_comma_list(value: str | None) -> set:
    # ... (Hàm này không thay đổi)
    if not value: return set()
    return {item for item in value.split(',') if item != ''}

def generate_tree(directory: Path, start_dir: Path, prefix: str = "", level: int = 0, max_level: int | None = None, 
                  ignore_list: set = None, submodules: set = None, prune_list: set = None,
                  dirs_only_list: set = None, is_in_dirs_only_zone: bool = False, counters: dict = None):
    # ... (Hàm này không thay đổi)
    if max_level is not None and level >= max_level: return
    try: contents = [path for path in directory.iterdir() if not path.name.startswith('.')]
    except (FileNotFoundError, NotADirectoryError): return
    dirs = sorted([d for d in contents if d.is_dir() and not is_path_matched(d, ignore_list, start_dir)], key=lambda p: p.name.lower())
    files = []
    if not is_in_dirs_only_zone:
        files = sorted([f for f in contents if f.is_file() and not is_path_matched(f, ignore_list, start_dir)], key=lambda p: p.name.lower())
    items_to_print = dirs + files
    pointers = ["├── "] * (len(items_to_print) - 1) + ["└── "]
    for pointer, path in zip(pointers, items_to_print):
        if path.is_dir(): counters['dirs'] += 1
        else: counters['files'] += 1
        is_submodule = path.is_dir() and path.name in submodules
        is_pruned = path.is_dir() and is_path_matched(path, prune_list, start_dir)
        is_dirs_only_entry = path.is_dir() and is_path_matched(path, dirs_only_list, start_dir) and not is_in_dirs_only_zone
        line = f"{prefix}{pointer}{path.name}{'/' if path.is_dir() else ''}"
        if is_submodule: line += " [submodule]"
        elif is_pruned: line += " [...]"
        elif is_dirs_only_entry: line += " [dirs only]"
        print(line)
        if path.is_dir() and not is_submodule and not is_pruned:
            extension = "│   " if pointer == "├── " else "    "
            next_is_in_dirs_only_zone = is_in_dirs_only_zone or is_dirs_only_entry
            generate_tree(path, start_dir, prefix + extension, level + 1, max_level, ignore_list, submodules, prune_list, dirs_only_list, next_is_in_dirs_only_zone, counters)

def main():
    """Main function to handle arguments and run the script."""
    parser = argparse.ArgumentParser(description="A smart directory tree generator with support for a .treeconfig file.")
    # ... (phần parser.add_argument không đổi)
    parser.add_argument("start_path", nargs='?', default=".", help="Starting path (file or directory).")
    parser.add_argument("-L", "--level", type=int, help="Limit the display depth.")
    parser.add_argument("-I", "--ignore", type=str, help="Comma-separated list of patterns to ignore.")
    parser.add_argument("-P", "--prune", type=str, help="Comma-separated list of patterns to prune.")
    parser.add_argument("-d", "--dirs-only", nargs='?', const='_ALL_', default=None, type=str, help="Show directories only.")
    parser.add_argument("-s", "--show-submodules", action='store_true', default=None, help="Show the contents of submodules.")
    parser.add_argument("--init", action='store_true', help="Create a sample .treeconfig file and exit.")
    args = parser.parse_args()

    if args.init:
        # ... (phần logic --init không đổi)
        config_filename = ".treeconfig"
        if Path(config_filename).exists():
            overwrite = input(f"'{config_filename}' already exists. Overwrite? (y/n): ").lower()
            if overwrite != 'y':
                print("Operation cancelled.")
                return
        with open(config_filename, 'w', encoding='utf-8') as f:
            f.write(CONFIG_TEMPLATE)
        print(f"✅ Successfully created '{config_filename}'.")
        return

    initial_path = Path(args.start_path).resolve()
    if not initial_path.exists():
        print(f"Error: Path does not exist: '{args.start_path}'")
        return
    start_dir = initial_path.parent if initial_path.is_file() else initial_path

    config = configparser.ConfigParser()
    config_file_path = start_dir / ".treeconfig"
    config_from_file = {}
    if config_file_path.exists():
        try:
            config.read(config_file_path)
            config_from_file = config['tree']
        except Exception as e:
            print(f"Warning: Could not read .treeconfig file: {e}")

    # Sử dụng đối tượng `config` chính để đọc, với fallback an toàn
    level = args.level if args.level is not None else config.getint('tree', 'level', fallback=None)
    show_submodules = args.show_submodules if args.show_submodules is not None else config.getboolean('tree', 'show-submodules', fallback=False)

    ignore_cli = parse_comma_list(args.ignore)
    ignore_file = parse_comma_list(config.get('tree', 'ignore', fallback=None))
    final_ignore_list = DEFAULT_IGNORE.union(ignore_file).union(ignore_cli)

    prune_cli = parse_comma_list(args.prune)
    prune_file = parse_comma_list(config.get('tree', 'prune', fallback=None))
    final_prune_list = DEFAULT_PRUNE.union(prune_file).union(prune_cli)

    dirs_only_cli = args.dirs_only
    dirs_only_file = config.get('tree', 'dirs-only', fallback=None)
    final_dirs_only = dirs_only_cli if dirs_only_cli is not None else dirs_only_file
    
    global_dirs_only = final_dirs_only == '_ALL_'
    dirs_only_list_custom = set()
    if final_dirs_only is not None and not global_dirs_only:
        dirs_only_list_custom = parse_comma_list(final_dirs_only)
    final_dirs_only_list = DEFAULT_DIRS_ONLY.union(dirs_only_list_custom)
    
    submodule_names = set()
    if not show_submodules:
        submodule_names = {p.name for p in get_submodule_paths(start_dir)}

    # --- BẮT ĐẦU THAY ĐỔI: Logic kiểm tra "Full view" ---
    is_truly_full_view = not final_ignore_list and not final_prune_list and not final_dirs_only_list and not global_dirs_only and not submodule_names
    filter_info = "Full view" if is_truly_full_view else "Filtered view"
    
    level_info = "full depth" if level is None else f"depth limit: {level}"
    mode_info = ", directories only" if global_dirs_only else ""
    
    print(f"{start_dir.name}/ [{filter_info}, {level_info}{mode_info}]")
    # --- KẾT THÚC THAY ĐỔI ---
    
    counters = {'dirs': 0, 'files': 0}
    generate_tree(start_dir, start_dir, max_level=level, ignore_list=final_ignore_list, 
                  submodules=submodule_names, prune_list=final_prune_list,
                  dirs_only_list=final_dirs_only_list, 
                  is_in_dirs_only_zone=global_dirs_only, counters=counters)

    files_info = "0 files (hidden)" if global_dirs_only and counters['files'] == 0 else f"{counters['files']} files"
    print(f"\n{counters['dirs']} directories, {files_info}")

if __name__ == "__main__":
    main()