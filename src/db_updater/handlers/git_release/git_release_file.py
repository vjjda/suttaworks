# Path: src/db_updater/handlers/git_release/git_release_file.py
import logging
import os
import tarfile
import zipfile
from pathlib import Path
from typing import Dict, Optional

import requests
from tqdm import tqdm

log = logging.getLogger(__name__)

__all__ = ["download_file", "decompress_archive"]


def download_file(url: str, dest_path: Path, headers: Optional[Dict[str, str]]):
    log.info(f"Đang tải về: {url} -> {dest_path.name}")
    try:
        with requests.get(url, stream=True, headers=headers) as r:
            r.raise_for_status()
            total_size = int(r.headers.get("content-length", 0))
            with (
                open(dest_path, "wb") as f,
                tqdm(
                    total=total_size, unit="iB", unit_scale=True, desc=dest_path.name
                ) as bar,
            ):
                for chunk in r.iter_content(chunk_size=8192):
                    size = f.write(chunk)
                    bar.update(size)
    except requests.exceptions.RequestException as e:
        log.error(f"Tải file thất bại: {e}")
        raise


def decompress_archive(
    archive_path: Path,
    original_asset_name: str,
    extract_dir: Path,
    force_extract: bool,
    auto_extract: bool,
):
    file_name_on_disk = archive_path.name
    should_delete_archive = True

    log.info(f"Đang xử lý file: {file_name_on_disk}")

    if force_extract:
        log.info(f"Ép buộc giải nén (zip): {file_name_on_disk} -> {extract_dir}")
        try:
            with zipfile.ZipFile(archive_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)
        except zipfile.BadZipFile:
            log.error(
                f"Lỗi: Đã ép buộc giải nén nhưng '{file_name_on_disk}' (tên gốc: {original_asset_name}) không phải file zip hợp lệ."
            )
            should_delete_archive = False
        except Exception as e:
            log.error(f"Lỗi khi ép buộc giải nén zip: {e}")
            should_delete_archive = False

    elif auto_extract:
        if original_asset_name.endswith(".zip"):
            log.info(f"Tự động giải nén (zip): {file_name_on_disk} -> {extract_dir}")
            with zipfile.ZipFile(archive_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)

        elif original_asset_name.endswith((".tar.gz", ".tgz")):
            log.info(f"Tự động giải nén (tar.gz): {file_name_on_disk} -> {extract_dir}")
            with tarfile.open(archive_path, "r:gz") as tar:
                tar.extractall(path=extract_dir)

        elif original_asset_name.endswith(".tar.bz2"):
            log.info(
                f"Tự động giải nén (tar.bz2): {file_name_on_disk} -> {extract_dir}"
            )
            with tarfile.open(archive_path, "r:bz2") as tar:
                tar.extractall(path=extract_dir)

        else:
            log.info(
                f"Giữ nguyên file (auto-extract, không khớp loại): {file_name_on_disk}"
            )
            should_delete_archive = False

    else:
        log.info(f"Giữ nguyên file (không giải nén): {file_name_on_disk}")
        should_delete_archive = False

    if should_delete_archive:
        os.remove(archive_path)
        log.info(f"Giải nén hoàn tất và đã xóa file nén: {file_name_on_disk}")
