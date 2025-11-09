# Path: src/db_updater/__main__.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import argcomplete

from src.config import constants
from src.config.logging_config import setup_logging
from src.db_updater.db_updater_arg_parser import CliArgsHandler
from src.db_updater.db_updater_config_parser import load_config
from src.db_updater.handlers.api_handler import ApiHandler
from src.db_updater.handlers.gdrive_handler import GDriveHandler
from src.db_updater.handlers.git_handler import GitHandler

from src.db_updater.handlers.git_release import GitReleaseHandler

HANDLER_DISPATCHER = {
    "git-submodule": GitHandler,
    "api": ApiHandler,
    "google-drive": GDriveHandler,
    "git-release": GitReleaseHandler,
}


def main():

    config_path = constants.CONFIG_PATH / "updater_config.yaml"
    config = load_config(config_path)

    arg_handler = CliArgsHandler(config, log=logging.getLogger(__name__))
    argcomplete.autocomplete(arg_handler.parser)
    args = arg_handler.parse_args()

    setup_logging("db_updater.log")
    log = logging.getLogger(__name__)
    log.info(f"Đang đọc cấu hình từ: {config_path}")

    if not config:
        log.error("Không thể tải cấu hình. Dừng chương trình.")
        return

    processed_args = arg_handler.validate_args(args)
    if not processed_args:
        return

    log.info(f"Các module sẽ được xử lý: {', '.join(processed_args.modules_to_run)}")

    handler_instances = []
    for module_name in processed_args.modules_to_run:
        module_config = config[module_name]
        destination_dir = constants.RAW_DATA_PATH / module_name
        module_type = list(module_config.keys())[0]
        handler_config = module_config[module_type]
        handler_class = HANDLER_DISPATCHER.get(module_type)

        if not handler_class:
            log.warning(f"Không tìm thấy handler cho loại module '{module_type}'.")
            continue
        handler_instances.append(handler_class(handler_config, destination_dir))

    has_download_errors = False
    if processed_args.run_update:
        log.info(
            f"--- Bắt đầu Giai đoạn 1: Tải về ({len(handler_instances)} module song song) ---"
        )
        with ThreadPoolExecutor(max_workers=len(handler_instances) or 1) as executor:
            futures = {executor.submit(h.execute): h for h in handler_instances}
            for future in as_completed(futures):
                handler = futures[future]
                try:
                    future.result()
                    log.info(f"✅ Tải về hoàn tất: {handler.__class__.__name__}")
                except Exception:
                    log.critical(
                        f"❌ Lỗi nghiêm trọng khi TẢI VỀ {handler.__class__.__name__}.",
                        exc_info=True,
                    )
                    has_download_errors = True
    else:
        log.info("Bỏ qua Giai đoạn 1: Tải về.")

    if processed_args.run_post_process:
        if has_download_errors:
            log.error("❌ Do có lỗi ở Giai đoạn 1, sẽ BỎ QUA Giai đoạn 2: Hậu xử lý.")
        else:
            log.info(
                f"--- Bắt đầu Giai đoạn 2: Hậu xử lý ({len(handler_instances)} module song song) ---"
            )
            with ThreadPoolExecutor(
                max_workers=len(handler_instances) or 1
            ) as executor:
                futures = {
                    executor.submit(h.run_post_tasks, processed_args.tasks_to_run): h
                    for h in handler_instances
                }
                for future in as_completed(futures):
                    handler = futures[future]
                    try:
                        future.result()
                        log.info(f"✅ Hậu xử lý hoàn tất: {handler.__class__.__name__}")
                    except Exception:
                        log.critical(
                            f"❌ Lỗi nghiêm trọng khi HẬU XỬ LÝ {handler.__class__.__name__}.",
                            exc_info=True,
                        )
    else:
        log.info("Bỏ qua Giai đoạn 2: Hậu xử lý.")

    log.info("Hoàn tất!")


if __name__ == "__main__":
    main()
