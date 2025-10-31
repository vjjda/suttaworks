# Path: src/db_builder/config_loader.py

import yaml
import logging
from pathlib import Path


logger = logging.getLogger(__name__)


def load_config(config_path: Path) -> dict:
    try:
        logger.info(f"Đang đọc file cấu hình từ: {config_path}")
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        if "suttacentral-sqlite" not in config:
            raise ValueError(
                "Thiếu khóa chính 'suttacentral-sqlite' trong file config."
            )

        db_config = config["suttacentral-sqlite"]
        required_keys = ["path", "name", "tree"]
        for key in required_keys:
            if key not in db_config:
                raise ValueError(f"Thiếu khóa '{key}' bên trong 'suttacentral-sqlite'.")

        logger.info("✅ Đọc và xác thực file cấu hình thành công.")
        return db_config

    except FileNotFoundError:
        logger.error(f"Lỗi: Không tìm thấy file cấu hình tại '{config_path}'.")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Lỗi: File cấu hình YAML không hợp lệ: {e}")
        raise
    except ValueError as e:
        logger.error(f"Lỗi: Cấu hình không hợp lệ. {e}")
        raise