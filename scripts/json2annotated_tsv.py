# Path: scripts/json2annotated_tsv.py
from pathlib import Path
import json


ROOT_PALI_DIR = Path("data/raw/git/suttacentral-data/sc_bilara_data/root/pli/ms/sutta")
ROOT_EN_DIR = Path(
    "data/raw/git/suttacentral-data/sc_bilara_data/translation/en/sujato/sutta"
)
OUTPUT_DIR = Path("data/processed/sutta_tsv")

TAB_COUNT = 3


def convert_pair_to_tsv(pali_path: Path, en_path: Path, output_path: Path):
    with pali_path.open("r", encoding="utf-8") as f:
        pali_data = json.load(f)
    with en_path.open("r", encoding="utf-8") as f:
        en_data = json.load(f)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    tab_sep = "\t" * TAB_COUNT
    lines_written = 0

    with output_path.open("w", encoding="utf-8") as out:
        for key, pali_value in pali_data.items():
            if ":" not in key:
                continue
            prefix, affix = key.split(":", 1)

            out.write(f"pli\t{prefix}\t{affix}{tab_sep}{pali_value}\n")
            lines_written += 1

            if key in en_data:
                en_value = en_data[key]
                out.write(f"en\t{prefix}\t{affix}{tab_sep}{en_value}\n")
                lines_written += 1

    print(
        f"✅ {pali_path.stem} → {output_path.relative_to(OUTPUT_DIR.parent)} ({lines_written} dòng)"
    )


def main():
    processed_files = 0

    for pali_path in ROOT_PALI_DIR.rglob("*.json"):
        prefix = pali_path.stem.split("_")[0]

        relative_subpath = pali_path.parent.relative_to(ROOT_PALI_DIR)
        en_path = (
            ROOT_EN_DIR / relative_subpath / f"{prefix}_translation-en-sujato.json"
        )

        if en_path.exists():
            output_subdir = OUTPUT_DIR / relative_subpath
            output_filename = f"{prefix}_annotated.tsv"
            output_path = output_subdir / output_filename

            convert_pair_to_tsv(pali_path, en_path, output_path)
            processed_files += 1

    print(f"\n🎯 Hoàn tất: Đã xử lý {processed_files} file song ngữ Pali–Anh.")


if __name__ == "__main__":
    main()
