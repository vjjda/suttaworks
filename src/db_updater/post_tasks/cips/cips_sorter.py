# Path: src/db_updater/post_tasks/cips/cips_sorter.py
from typing import Dict
from natsort import natsorted, natsort_keygen

__all__ = ["sort_topic_index", "sort_sutta_index"]


def sort_topic_index(topic_index: Dict) -> Dict:
    sorted_index = {}
    for topic in natsorted(topic_index.keys()):
        original_topic_data = topic_index[topic]
        new_topic_data = {}

        for key in natsorted(original_topic_data.keys()):
            if key == "also_see":

                new_topic_data[key] = natsorted(
                    list(dict.fromkeys(original_topic_data[key]))
                )
            elif key == "contexts":
                original_contexts = original_topic_data[key]
                sorted_contexts = {}
                for context_name in natsorted(original_contexts.keys()):
                    sutta_data = original_contexts[context_name]
                    sorted_sutta_data = {}
                    for sutta_uid in natsorted(sutta_data.keys()):

                        sorted_segments = natsorted(
                            list(dict.fromkeys(sutta_data[sutta_uid]))
                        )
                        sorted_sutta_data[sutta_uid] = sorted_segments
                    sorted_contexts[context_name] = sorted_sutta_data
                new_topic_data[key] = sorted_contexts
        sorted_index[topic] = new_topic_data
    return sorted_index


def sort_sutta_index(sutta_index: Dict) -> Dict:
    sorted_index = {}
    natural_key_gen = natsort_keygen()

    for uid in natsorted(sutta_index.keys()):
        uid_data = sutta_index[uid]

        def get_topic_sort_key(topic_name):
            contexts_data = uid_data[topic_name]

            all_segments = [
                seg for seg_list in contexts_data.values() for seg in seg_list
            ]

            if not all_segments:

                return (1, natural_key_gen(topic_name), ())

            first_segment = natsorted(all_segments)[0]

            return (0, natural_key_gen(first_segment), natural_key_gen(topic_name))

        sorted_topic_names = sorted(uid_data.keys(), key=get_topic_sort_key)

        sorted_topics_in_sutta = {}
        for topic in sorted_topic_names:
            topic_data = uid_data[topic]

            sorted_contexts_in_sutta = {
                ctx: topic_data[ctx] for ctx in natsorted(topic_data.keys())
            }

            for seg_list in sorted_contexts_in_sutta.values():
                seg_list.sort(key=natural_key_gen)
            sorted_topics_in_sutta[topic] = sorted_contexts_in_sutta

        sorted_index[uid] = sorted_topics_in_sutta

    return sorted_index
