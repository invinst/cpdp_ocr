#!/usr/bin/python3

import gensim
import json
import tempfile
import psycopg2

import pandas as pd

from os import listdir
from doccano_api_client import DoccanoClient

def pg_conn(db_name='cpdp', db_host='localhost', db_user='cpdp', db_pass='cpdp'):
    vals = dict(db_name=db_name, db_host=db_host, db_user=db_user, db_pass=db_pass)
    conn_str = "dbname={db_name} host={db_host} user={db_user} password={db_pass}".format(**vals)
    conn = psycopg2.connect(conn_str)

    return conn

conn = pg_conn()
curs = conn.cursor()

sqlstr = """
    SELECT distinct(sd.text)
    FROM cr_summary_data sd, cr_pdfs p, cr_foia_batch fb
    WHERE sd.pdf_id = p.id
    AND sd.text IS NOT NULL 
    AND LENGTH(sd.text) > 0
    AND p.batch_id = fb.id
    AND (
      (section_name = 'Accused Members' AND column_name = 'Initial / Intake Allegation') 
       OR (section_name = 'Review Incident' AND 'col_name' = 'Remarks') 
       OR (section_name = 'Incident Finding / Overall Case Finding' and column_name = 'Finding') 
       OR (section_name = 'Current Allegations' and column_name = 'Allegation'))
    """

def read_corpus(lines, tokens_only=False):
    for text_id, line in enumerate(lines):
        print(text_id, line)
        tokens = gensim.utils.simple_preprocess(line, deacc=True)
        if tokens_only:
            yield tokens
        else:
            yield gensim.models.doc2vec.TaggedDocument(tokens, [text_id])

df = pd.read_sql(sqlstr, con=conn)
lines = df['text']

train_corpus = list(read_corpus(lines))
test_corpus = list(read_corpus(lines, tokens_only=True))

test_corpus = []
model = gensim.models.doc2vec.Doc2Vec(vector_size=50, min_count=2, epochs=40)
model.build_vocab(train_corpus)
model.train(train_corpus, total_examples=model.corpus_count, epochs=model.epochs)

ranks = []
second_ranks = []
for doc_id in range(len(train_corpus)):
    inferred_vector = model.infer_vector(train_corpus[doc_id].words)
    sims = model.docvecs.most_similar([inferred_vector], topn=len(model.docvecs))
    rank = [docid for docid, sim in sims].index(doc_id)
    ranks.append(rank)

    second_ranks.append(sims[1])
