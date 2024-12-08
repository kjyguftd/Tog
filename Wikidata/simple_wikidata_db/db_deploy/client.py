import itertools
import random
import xmlrpc.client
import typing as tp

# from ToG.wiki_func import relation_search_prune
from Wikidata.simple_wikidata_db.db_deploy.utils_wikidata_query import find_start_entity, relation_search_prune_wiki, \
    search_wikidata_entity_id, clean_question, search_wikidata_property


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

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--addr_list",
        type=str,
        help="path to server address list",
        default="D:\PycharmProjects\DS\ToG\Wikidata\simple_wikidata_db\db_deploy\server_urls_new.txt"
    )
    parser.add_argument("--remove_unnecessary_rel", type=bool,
                        default=True, help="whether removing unnecessary relations.")
    args = parser.parse_args()

    with open(args.addr_list, "r") as f:
        server_addrs = f.readlines()
        server_addrs = [addr.strip() for addr in server_addrs]
    print(f"Server addresses: {server_addrs}")
    client = MultiServerWikidataQueryClient(server_addrs)

    # ToG TODO
    # question = input("prompt: ")
    question = "What is the zipcode of the parent organization of blanton museum of art?"
    start_entity = find_start_entity(question).replace(' ', '_')
    topic_entity = {search_wikidata_entity_id(start_entity): start_entity}
    cleaned_tokens, entity_dict = clean_question(question)
    property_dict = {}
    for token in cleaned_tokens:
        value = search_wikidata_property(token)
        print(value)
        property_dict[token] = value
    for key, value in property_dict.items():
        print(f"{key}: {value}")

    print(search_wikidata_entity_id(start_entity), property_dict[entity_dict[start_entity]])
    entity = client.query_all('get_tail_entities_given_head_and_relation', search_wikidata_entity_id(start_entity), property_dict[entity_dict[start_entity]])
    ut = entity['head'] if entity['head'] else entity['tail']
    print(ut[0]['qid'])
    print(property_dict[entity_dict[entity_dict[start_entity]]])
    entity = client.query_all('get_tail_values_given_head_and_relation', ut[0]['qid'],
                              property_dict[entity_dict[entity_dict[start_entity]]])
    entity_1 = client.query_all('get_tail_entities_given_head_and_relation', ut[0]['qid'],
                              property_dict[entity_dict[entity_dict[start_entity]]])
    print(entity)