# Path: src/db_builder/db_builder_arg_parser.py
import argparse


class BuilderArgsParser:
    def __init__(self):
        self.parser = self._setup_parser()

    def _setup_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Công cụ xây dựng database SuttaCentral."
        )
        parser.add_argument(
            "--overwrite",
            action="store_true",
            help="Xóa file database hiện có trước khi xây dựng lại.",
        )
        return parser

    def parse(self) -> argparse.Namespace:
        return self.parser.parse_args()
