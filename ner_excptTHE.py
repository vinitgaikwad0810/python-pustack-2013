import ner

with open('U4.txt', 'r') as f:
    sample = f.read()

tagger = ner.SocketNER(host='localhost', port=8080)
tagged_result=tagger.get_entities(sample)
print tagged_result
