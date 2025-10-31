# Path: src/db_updater/post_tasks/parallels/parallels_processor.py
from collections import defaultdict
from itertools import combinations

from . import parallels_utils

__all__ = ["build_initial_map"]


def build_initial_map(data: list) -> defaultdict:
    sutta_map = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for group in data:
        relation_type = list(group.keys())[0]
        id_list = group[relation_type]

        full_list = [i for i in id_list if not i.startswith("~")]
        resembling_list = [i for i in id_list if i.startswith("~")]

        if relation_type == "parallels":
            for source, target in combinations(full_list, 2):
                base_s = parallels_utils.parse_sutta_id(source)
                base_t = parallels_utils.parse_sutta_id(target)
                sutta_map[base_s]["parallels"][source].append(target)
                sutta_map[base_t]["parallels"][target].append(source)
            if full_list and resembling_list:
                for source in full_list:
                    base_s = parallels_utils.parse_sutta_id(source)
                    for target in resembling_list:
                        cleaned_t = target.lstrip("~")
                        base_t = parallels_utils.parse_sutta_id(cleaned_t)
                        sutta_map[base_s]["resembles"][source].append(cleaned_t)
                        sutta_map[base_t]["resembles"][cleaned_t].append(source)
        elif relation_type in ["mentions", "retells"]:
            for source, target in combinations(full_list, 2):
                base_s = parallels_utils.parse_sutta_id(source)
                base_t = parallels_utils.parse_sutta_id(target)
                sutta_map[base_s][relation_type][source].append(target)
                sutta_map[base_t][relation_type][target].append(source)
            if full_list and resembling_list:
                for source in full_list:
                    base_s = parallels_utils.parse_sutta_id(source)
                    for target in resembling_list:
                        cleaned_t = target.lstrip("~")
                        base_t = parallels_utils.parse_sutta_id(cleaned_t)
                        sutta_map[base_s][relation_type][source].append(cleaned_t)
                        sutta_map[base_t][relation_type][cleaned_t].append(source)
    return sutta_map
