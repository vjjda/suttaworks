# Path: src/db_updater/__main__.py
import argparse
import logging

from src.config.logging_config import setup_logging
from src.config import constants
from src.db_updater.db_updater_config_parser import load_config
from src.db_updater.db_updater_arg_parser import CliArgsHandler
from src.db_updater.handlers.git_release_handler import GitReleaseHandler
from src.db_updater.handlers.git_handler import GitHandler
from src.db_updater.handlers.api_handler import ApiHandler
from src.db_updater.handlers.gdrive_handler import GDriveHandler

HANDLER_DISPATCHER = {
    "git-submodule": GitHandler,
    "api": ApiHandler,
    "google-drive": GDriveHandler,
    "git-release": GitReleaseHandler,
}


def main():
    # Perform a preliminary parse to check for the help flag (-h, --help)
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument('-h', '--help', action='store_true', default=False)
    pre_args, _ = pre_parser.parse_known_args()

    config_path = constants.CONFIG_PATH / "updater_config.yaml"
    config = load_config(config_path)

    # Setup parser with config for full help message
    # We create a dummy logger here just for the arg handler, as real logging is set up later
    arg_handler = CliArgsHandler(config, log=logging.getLogger(__name__))

    if pre_args.help:
        arg_handler.parser.print_help()
        return

    # If not called for help, set up proper logging
    setup_logging("db_updater.log")
    log = logging.getLogger(__name__)
    log.info(f"Đang đọc cấu hình từ: {config_path}")

    if not config:
        log.error("Không thể tải cấu hình. Dừng chương trình.")
        return

    # Re-initialize arg_handler with the real logger
    arg_handler = CliArgsHandler(config, log)
    processed_args = arg_handler.parse()

    if not processed_args:
        return

    log.info(f"Các module sẽ được xử lý: {', '.join(processed_args.modules_to_run)}")
    for module_name in processed_args.modules_to_run:
        log.info(f"--- Bắt đầu xử lý module: '{module_name}' ---")
        module_config = config[module_name]
        destination_dir = constants.RAW_DATA_PATH / module_name

        module_type = list(module_config.keys())[0]
        handler_config = module_config[module_type]

        handler_class = HANDLER_DISPATCHER.get(module_type)

        if not handler_class:
            log.warning(f"Không tìm thấy handler cho loại module '{module_type}'.")
            continue

        try:
            handler_instance = handler_class(handler_config, destination_dir)
            handler_instance.process(
                run_update=processed_args.run_update,
                run_post_process=processed_args.run_post_process,
                tasks_to_run=processed_args.tasks_to_run,
            )
        except Exception:
            log.critical(f"Lỗi nghiêm trọng khi xử lý module '{module_name}'.", exc_info=True)

    log.info("Hoàn tất!")


if __name__ == "__main__":
    main()