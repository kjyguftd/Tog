import requests
import spacy


WIKI_URL = 'https://www.wikidata.org/w/api.php'

nlp = spacy.load('en_core_web_sm')

def search_wikidata_entity(label, language="en"):
    url = WIKI_URL
    params = {
        'action': 'wbsearchentities',
        'format': 'json',
        'language': language,
        'type': 'item',
        'search': label
    }
    response = requests.get(url, params=params)
    data = response.json()

    if 'search' in data:
        for entity in data['search']:
            print(f"Label: {entity['label']}, QID: {entity['id']}")
            return entity['id']
    else:
        print("No entities found.")


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
