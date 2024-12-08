import re
from itertools import chain

import requests
import spacy

import nltk
from nltk import pos_tag, CoreNLPDependencyParser
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

WIKI_URL = 'https://www.wikidata.org/w/api.php'

nltk.download('punkt_tab')
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger_eng')
nlp = spacy.load('en_core_web_sm')

def search_wikidata_entity_id(label, language="en", timeout=5):
    url = WIKI_URL
    params = {
        'action': 'wbsearchentities',
        'format': 'json',
        'language': language,
        'type': 'item',
        'search': label
    }

    response = requests.get(url, params=params, timeout=timeout)
    if response.status_code == 200:
        data = response.json()
        if 'search' in data:
            for entity in data['search']:
                # print(f"Label: {entity['label']}, QID: {entity['id']}")
                return entity['id']
        else:
            print(None)
    else:
        print(None)


def search_wikidata_property(label, language="en"):
  url = WIKI_URL
  params = {
      'action': 'wbsearchentities',
      'format': 'json',
      'language': language,
      'type': 'property',
      'search': label
  }

  response = requests.get(url, params=params)
  data = response.json()

  if 'search' in data:
      for entity in data['search']:
          print(f"Property ID: {entity['id']}, Label: {entity['label']}")
          return entity['id']
  else:
      print("Not Found")


def extract_entities(question):
    doc = nlp(question)
    entities = [(entity.text, entity.label_) for entity in doc.ents]
    return entities

def find_start_entity(question):
    entities = extract_entities(question)
    start_entity = None
    if entities:
        start_entity = entities[0][0]
    return start_entity


def clean_question(question):

    phrase_replacements = {
        "parent organization": "parent_organization",
        "blanton museum of art": "blanton_museum_of_art"
    }

    for phrase, replacement in phrase_replacements.items():
        question = question.replace(phrase, replacement)

    question = re.sub(r'[^\w\s]', '', question)
    tokens = word_tokenize(question)
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word.lower() not in stop_words]
    entity_dict = {}

    pos_tags = pos_tag(tokens)
    parser = CoreNLPDependencyParser(url='http://192.168.1.68:9000/')
    parse, = parser.raw_parse(question)
    # print("Dependency Parse:")
    for governor,_ , dependent in parse.triples():
        if governor[0] in filtered_tokens and dependent[0] in filtered_tokens:
            entity_dict[dependent[0]] = governor[0]
            print(dependent[0], governor[0])

    # cleaned_tokens = [word.replace('_', ' ') for word in filtered_tokens]
    return filtered_tokens, entity_dict


def check_end_word_wiki(s):
    words = [" ID", " code", " number", "instance of", "website", "URL", "inception", "image", " rate", " count"]
    return any(s.endswith(word) for word in words)


def abandon_rels_wiki(relation):
    useless_relation_list = ["category's main topic", "topic\'s main category", "stack exchange site", 'main subject', 'country of citizenship', "commons category", "commons gallery", "country of origin", "country", "nationality"]
    if check_end_word_wiki(relation) or 'wikidata' in relation.lower() or 'wikimedia' in relation.lower() or relation.lower() in useless_relation_list:
        return True
    return False


def relation_search_prune_wiki(entity_id, entity_name, pre_relations, pre_head, question, args, wiki_client):
    relations = []
    cleaned_tokens = clean_question(question)
    for token in cleaned_tokens:
        property_id = search_wikidata_property(token)
        relations.append(property_id)
        # relations = wiki_client.query_all("get_all_relations_of_an_entity", entity_id)

    # head_relations = relations['head']
    # tail_relations = relations['tail']
    #
    # if args.remove_unnecessary_rel:
    #     head_relations = [relation for relation in head_relations if not abandon_rels_wiki(relation)]
    #     tail_relations = [relation for relation in tail_relations if not abandon_rels_wiki(relation)]
    #
    # if len(pre_relations) != 0 and pre_head != -1:
    #     tail_relations = [rel for rel in pre_relations if pre_head and rel not in tail_relations]
    #     head_relations = [rel for rel in pre_relations if not pre_head and rel not in head_relations]
    #
    # head_relations = list(set(head_relations))
    # tail_relations = list(set(tail_relations))
    # total_relations = head_relations + tail_relations
    # total_relations.sort()  # make sure the order in prompt is always equal

    # prompt = construct_relation_prune_prompt(question, entity_name, total_relations, args)

    # result = run_llm(prompt, args.temperature_exploration, args.max_length, args.opeani_api_keys, args.LLM_type)
    # flag, retrieve_relations_with_scores = clean_relations(result, entity_id, head_relations)

    # if flag:
    #     return retrieve_relations_with_scores
    # else:
    #     return []  # format error or too small max_length


if __name__ == '__main__':
    question = "What is the zipcode of the parent organization of blanton museum of art?"
    cleaned_tokens, chain_dict = clean_question(question)
    for token in cleaned_tokens:
        search_wikidata_property(token)
    print(cleaned_tokens)
