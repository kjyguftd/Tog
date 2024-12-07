import itertools
import random
import xmlrpc.client
import typing as tp

from Wikidata.simple_wikidata_db.db_deploy.utils_wikidata_query import find_start_entity


class WikidataQueryClient:
    def __init__(self, url: str):
        self.url = url
        self.server = xmlrpc.client.ServerProxy(url)

    def label2qid(self, label: str) -> str:
        return self.server.label2qid(label)

    def label2pid(self, label: str) -> str:
        return self.server.label2pid(label)

    def pid2label(self, pid: str) -> str:
        return self.server.pid2label(pid)

    def qid2label(self, qid: str) -> str:
        return self.server.qid2label(qid)

    def get_all_relations_of_an_entity(
        self, entity_qid: str
    ) -> tp.Dict[str, tp.List]:
        return self.server.get_all_relations_of_an_entity(entity_qid)

    def get_tail_entities_given_head_and_relation(
        self, head_qid: str, relation_pid: str
    ) -> tp.Dict[str, tp.List]:
        return self.server.get_tail_entities_given_head_and_relation(
            head_qid, relation_pid
        )

    def get_tail_values_given_head_and_relation(
        self, head_qid: str, relation_pid: str
    ) -> tp.List[str]:
        return self.server.get_tail_values_given_head_and_relation(
            head_qid, relation_pid
        )

    def get_external_id_given_head_and_relation(
        self, head_qid: str, relation_pid: str
    ) -> tp.List[str]:
        return self.server.get_external_id_given_head_and_relation(
            head_qid, relation_pid
        )

    def mid2qid(self, mid: str) -> str:
        return self.server.mid2qid(mid)


import time
import typing as tp
from concurrent.futures import ThreadPoolExecutor


class MultiServerWikidataQueryClient:
    def __init__(self, urls: tp.List[str]):
        self.clients = [WikidataQueryClient(url) for url in urls]
        self.executor = ThreadPoolExecutor(max_workers=len(urls))
        # test connections
        start_time = time.perf_counter()
        self.test_connections()
        end_time = time.perf_counter()
        print(f"Connection testing took {end_time - start_time} seconds")

    def test_connections(self):
        def test_url(client):
            try:
                # Check if server provides the system.listMethods function.
                client.server.system.listMethods()
                return True
            except Exception as e:
                print(f"Failed to connect to {client.url}. Error: {str(e)}")
                return False

        start_time = time.perf_counter()
        futures = [
            self.executor.submit(test_url, client) for client in self.clients
        ]
        results = [f.result() for f in futures]
        end_time = time.perf_counter()
        # print(f"Testing connections took {end_time - start_time} seconds")
        # Remove clients that failed to connect
        self.clients = [
            client for client, result in zip(self.clients, results) if result
        ]
        if not self.clients:
            raise Exception("Failed to connect to all URLs")

    def query_all(self, method, *args):
        start_time = time.perf_counter()
        futures = [
            self.executor.submit(getattr(client, method), *args)
            for client in self.clients
        ]
        # Retrieve results and filter out 'Not Found!'
        is_dict_return = method in [
            "get_all_relations_of_an_entity",
            "get_tail_entities_given_head_and_relation",
        ]
        results = [f.result() for f in futures]
        end_time = time.perf_counter()
        # print(f"HTTP Queries took {end_time - start_time} seconds")

        start_time = time.perf_counter()
        real_results = set() if not is_dict_return else {"head": [], "tail": []}
        for res in results:
            if isinstance(res, str) and res == "Not Found!":
                continue
            elif isinstance(res, tp.List):
                if len(res) == 0:
                    continue
                if isinstance(res[0], tp.List):
                    res_flattened = itertools.chain(*res)
                    real_results.update(res_flattened)
                    continue
                real_results.update(res)
            elif is_dict_return:
                real_results["head"].extend(res["head"])
                real_results["tail"].extend(res["tail"])
            else:
                real_results.add(res)
        end_time = time.perf_counter()
        # print(f"Querying all took {end_time - start_time} seconds")

        return real_results if len(real_results) > 0 else "Not Found!"


if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--dataset", type=str,
    #                     default="cwq", help="choose the dataset.")
    # parser.add_argument("--max_length", type=int,
    #                     default=256, help="the max length of LLMs output.")
    # parser.add_argument("--temperature_exploration", type=float,
    #                     default=0.4, help="the temperature in exploration stage.")
    # parser.add_argument("--temperature_reasoning", type=float,
    #                     default=0, help="the temperature in reasoning stage.")
    # parser.add_argument("--width", type=int,
    #                     default=3, help="choose the search width of ToG.")
    # parser.add_argument("--depth", type=int,
    #                     default=3, help="choose the search depth of ToG.")
    # parser.add_argument("--remove_unnecessary_rel", type=bool,
    #                     default=True, help="whether removing unnecessary relations.")
    # # parser.add_argument("--LLM_type", type=str,
    # #                     default="gpt-3.5-turbo", help="base LLM model.")
    # # parser.add_argument("--opeani_api_keys", type=str,
    # #                     default="", help="if the LLM_type is gpt-3.5-turbo or gpt-4, you need add your own openai api keys.")
    # parser.add_argument("--num_retain_entity", type=int,
    #                     default=5, help="Number of entities retained during entities search.")
    # parser.add_argument("--prune_tools", type=str,
    #                     default="llm", help="prune tools for ToG, can be llm (same as LLM_type), bm25 or sentencebert.")
    # parser.add_argument("--addr_list", type=int,
    #                     default="server_urls.txt", help="The address of the Wikidata service.")
    # args = parser.parse_args()
    #
    # datas, question_string = prepare_dataset(args.dataset)
    #
    #
    # for data in tqdm(datas):
    #     question = data[question_string]
    #     topic_entity = data['topic_entity']
    #     cluster_chain_of_entities = []
    #     pre_relations = [],
    #     pre_heads= [-1] * len(topic_entity)
    #     flag_printed = False
    #     addr_list = 'ToG/ToG-E/server_urls.txt'
    #     with open(addr_list, "r") as f:
    #         server_addrs = f.readlines()
    #         server_addrs = [addr.strip() for addr in server_addrs]
    #     print(f"Server addresses: {server_addrs}")
    #     client =  MultiServerWikidataQueryClient(server_addrs)
    #
    #     for depth in range(1, args.depth + 1):
    #         current_entity_relations_list = []
    #         i = 0
    #         for entity in topic_entity:
    #             if entity != "[FINISH_ID]":
    #                 retrieve_relations_with_scores = relation_search_prune(entity, topic_entity[entity], pre_relations,
    #                                                                        pre_heads[i], question, args,
    #                                                                        client)  # best entity triplet, entitiy_id
    #                 current_entity_relations_list.extend(retrieve_relations_with_scores)
    #             i += 1
    #         total_candidates = []
    #         total_scores = []
    #         total_relations = []
    #         total_entities_id = []
    #         total_topic_entities = []
    #         total_head = []
    #
    #         for entity in current_entity_relations_list:
    #             value_flag = False
    #             if entity['head']:
    #                 entity_candidates_id, entity_candidates_name = entity_search(entity['entity'], entity['relation'], True)
    #             else:
    #                 entity_candidates_id, entity_candidates_name = entity_search(entity['entity'], entity['relation'],
    #                                                                          False)
    #
    #             if len(entity_candidates_id) == 0:  # values
    #                 value_flag = True
    #                 if len(entity_candidates_name) >= 20:
    #                     entity_candidates_name = random.sample(entity_candidates_name, 10)
    #                 entity_candidates_id = ["[FINISH_ID]"] * len(entity_candidates_name)
    #             else:  # ids
    #                 entity_candidates_id, entity_candidates_name = del_all_unknown_entity(entity_candidates_id,
    #                                                           entity_candidates_name)
    #                 if len(entity_candidates_id) >= 20:
    #                     indices = random.sample(range(len(entity_candidates_name)), 10)
    #                     entity_candidates_id = [entity_candidates_id[i] for i in indices]
    #                     entity_candidates_name = [entity_candidates_name[i] for i in indices]
    #
    #             if len(entity_candidates_id) == 0:
    #                 continue
    #
    #             scores, entity_candidates, entity_candidates_id = entity_score(question, entity_candidates_id,
    #                                                                        entity_candidates_name, entity['score'],
    #                                                                        entity['relation'], args)
    #             total_candidates, total_scores, total_relations, total_entities_id, total_topic_entities, total_head = update_history(
    #                 entity_candidates, entity, scores, entity_candidates_id, total_candidates, total_scores,
    #                 total_relations, total_entities_id, total_topic_entities, total_head, value_flag)
    #
    #             if len(total_candidates) == 0:
    #                 half_stop(question, cluster_chain_of_entities, args)
    #                 break
    #
    #             flag, chain_of_entities, entities_id, pre_relations, pre_heads = entity_prune(total_entities_id,
    #                                                                                           total_relations,
    #                                                                                           total_candidates,
    #                                                                                           total_topic_entities,
    #                                                                                           total_head, total_scores,
    #                                                                                           args, client)
    #             cluster_chain_of_entities.append(chain_of_entities)
    #             if flag:
    #                 stop, results = reasoning(question, cluster_chain_of_entities, args)
    #                 if stop:
    #                     print("ToG stoped at depth %d." % depth)
    #                     save_2_jsonl(question, results, cluster_chain_of_entities, file_name=args.dataset)
    #                     flag_printed = True
    #                 else:
    #                     print("depth %d still not find the answer." % depth)
    #                     topic_entity = {entity: client.query_all("qid2label", entity) for entity in entities_id}
    #                     continue
    #             else:
    #                 half_stop(question, cluster_chain_of_entities, args)
    #
    #     if not flag_printed:
    #         results = generate_without_explored_paths(question, args)
    #         save_2_jsonl(question, results, [], file_name=args.dataset)

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--addr_list",
        type=str,
        help="path to server address list",
        default="D:\PycharmProjects\DS\ToG\Wikidata\simple_wikidata_db\db_deploy\server_urls_new.txt"
    )
    args = parser.parse_args()


    # ToG TODO
    question = input("prompt: ")
    topic_entity = find_start_entity(question)
    # cluster_chain_of_entities = []
    # pre_relations = [],
    # pre_heads = [-1] * len(topic_entity)
    # flag_printed = False


    with open(args.addr_list, "r") as f:
        server_addrs = f.readlines()
        server_addrs = [addr.strip() for addr in server_addrs]
    print(f"Server addresses: {server_addrs}")
    client = MultiServerWikidataQueryClient(server_addrs)



    print(
        f'MSFT\'s ticker code is  {client.query_all("get_tail_values_given_head_and_relation", "Q49213", "P373", )}'
    )
    print(
        f'MSFT\'s ticker code is  {client.query_all("get_all_relations_of_an_entity", "Q2906140")}'
    )