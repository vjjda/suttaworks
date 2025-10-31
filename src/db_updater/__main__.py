# Path: src/db_updater/__main__.py
import argparse
import logging
import argcomplete

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
    # Load config first, as it's needed for parser setup
    config_path = constants.CONFIG_PATH / "updater_config.yaml"
    config = load_config(config_path)

    # Setup parser
    # A dummy logger is used here, as full logging is set up only after we know
    # we are not just doing a help or autocomplete call.
    arg_handler = CliArgsHandler(config, log=logging.getLogger(__name__))

    # Activate argcomplete. This will handle completion requests and exit.
    argcomplete.autocomplete(arg_handler.parser)

    # Now, parse the arguments. This will also handle -h/--help and exit.
    args = arg_handler.parse_args()

    # If we've reached this point, we are running the main logic.
    # Now we can set up proper logging.
    setup_logging("db_updater.log")
    log = logging.getLogger(__name__)
    log.info(f"Đang đọc cấu hình từ: {config_path}")

    if not config:
        log.error("Không thể tải cấu hình. Dừng chương trình.")
        return

    # Validate the parsed arguments
    processed_args = arg_handler.validate_args(args)

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