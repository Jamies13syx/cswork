import os
import re
import csv
import sys
from nltk.stem import *
from decimal import Decimal
from decimal import getcontext
import math
import time

if __name__ == '__main__':
    args = list(sys.argv)
    index_path = args[1]
    query_path = args[2]
    retrieval_model = args[3]
    index_type = args[4]
    result_path = args[5]
    result_path = result_path + 'result_' + str(index_type) + '_' + str(retrieval_model) + '.txt'

    stop_word_path = '.' + os.altsep + 'stops.txt'
    stop_words_handler = open(stop_word_path)
    stop_words = stop_words_handler.read().splitlines()
    query_tf = dict()

    document_number = 0
    sum_length = 0
    document_length_collection = dict()
    temp_path = os.listdir('.' + os.altsep + 'temp_' + str(index_type))
    for item in temp_path:
        document_number = document_number + 1
        document_path = os.path.join('.' + os.altsep + 'temp_' + str(index_type), item)
        f = open(document_path)
        document_length = len(f.readlines())
        document_length_collection[item.replace('.csv', '')] = document_length  # each document record its length
        sum_length = sum_length + document_length
    average_length = sum_length / document_number  # average length calculation

    def sort_similarity(similarity, query_retrieved_document_number, model_name):
        result_file = open(result_path, 'a')

        query_document_map = dict()
        key_list = list(query_retrieved_document_number)
        value_list = list(query_retrieved_document_number.values())
        query_document_map[key_list[0]] = value_list[0]
        for i in range(1, len(query_retrieved_document_number)):
            query_document_map[key_list[i]] = value_list[i] - value_list[i - 1]  # calculation each query number retrieved how many documents

        sub_similarity = dict()
        count = 1
        for key in similarity.keys():
            limit = query_document_map.get(key[0])
            if count <= limit:
                sub_similarity[key] = similarity.get(key)
                count = count + 1
            if count > limit:
                sorted_similarity = sorted(sub_similarity.items(), key=lambda x: x[1], reverse=True)  # sort sub dictionary's records
                rank = 1
                for data in sorted_similarity:
                    write_line = str(data[0][0]) + ' ' + '0' + ' ' + data[0][1] + ' ' + str(rank) + ' ' + str(data[1]) + ' ' + model_name
                    rank = rank + 1
                    result_file.write(write_line + '\n')
                    if rank > 100:
                        break
                sub_similarity.clear()
                count = 1

    def total_each_token_tf():
        tokens = dict()
        index = open(index_path, 'r')
        index_reader = csv.reader(index)
        for record in index_reader:
            token = record[0]
            for i in range(3, len(record)):
                temp_tuple = tuple(eval(record[i]))
                tf = temp_tuple[1]
                tokens[token] = tokens.get(token, 0) + tf
        return tokens

    def query_compact():
        if index_type == 'single':
            compact_query = open('./compact_query.csv', 'w', newline='')
            query_file = open(query_path, 'r')
            csv_writer = csv.writer(compact_query)
            query_number = int()
            for line in query_file:
                if line.startswith('<num>'):
                    query_number = re.findall(r'[0-9]+', line)[0]
                elif line.startswith('<title>'):
                    query = line[15:].strip()
                    query = query.replace('(', '').replace(')', '')  # normalization
                    query_token_list = re.split(r'[/\-\s]', query)   # split into tokens
                    for element in query_token_list:
                        element = element.lower()
                        if element in stop_words:
                            continue
                        query_tf[(query_number, element)] = query_tf.get((query_number, element), 0) + 1  # query weight compute
                        csv_writer.writerow((query_number, element))
        elif index_type == 'stem':
            compact_query = open('./compact_query.csv', 'w', newline='')
            query_file = open(query_path, 'r')
            csv_writer = csv.writer(compact_query)
            query_number = int()
            stemmer = PorterStemmer()
            for line in query_file:
                if line.startswith('<num>'):
                    query_number = re.findall(r'[0-9]+', line)[0]
                elif line.startswith('<title>'):
                    query = line[15:].strip()
                    query = query.replace('(', '').replace(')', '')
                    query_token_list = re.split(r'[/\-\s]', query)
                    for element in query_token_list:
                        element = element.lower()
                        if element in stop_words:
                            continue
                        element = stemmer.stem(element)
                        query_tf[(query_number, element)] = query_tf.get((query_number, element), 0) + 1  # query weight compute
                        csv_writer.writerow((query_number, element))

    def query_index_matcher():
        temp_output = open('./temp_output.csv', 'a', newline='')
        temp_output_writer = csv.writer(temp_output)
        compact_query = open('./compact_query.csv', 'r')
        query_reader = csv.reader(compact_query)
        for data in query_reader:
            query_number = data[0]
            query_token = data[1]
            index = open(index_path, 'r')
            index_reader = csv.reader(index)
            for record in index_reader:
                if record[0] == query_token:
                    output = [query_number] + record   # find posting list
                    temp_output_writer.writerow(output)
                    output.clear()

    def cosine():
        context = getcontext()
        context.prec = 15
        query_output = open('./temp_output.csv', 'r')
        query_reader = csv.reader(query_output)
        similarity = dict()
        query_retrieved_document_number = dict()

        var_1 = dict()
        var_2 = dict()
        var_3 = dict()
        tf_pow_2 = dict()

        for item in temp_path:
            document_path = os.path.join('.' + os.altsep + 'temp_' + str(index_type), item)
            f = open(document_path)
            reader = csv.reader(f)
            for record in reader:
                temp_record = tuple(eval(record[1]))
                tf_pow = Decimal(math.pow(temp_record[1], 2))
                tf_pow_2[item.replace('.csv', '')] = tf_pow_2.get(item.replace('.csv', ''), 0) + tf_pow

        for data in query_reader:
            query_number = data[0]
            query_token = data[1]
            df = Decimal(data[2])
            idf = Decimal(math.log(document_number / df, 10))
            for i in range(3, len(data)):
                temp_tuple = tuple(eval(data[i]))
                document_ID = temp_tuple[0]
                tf = Decimal(temp_tuple[1])
                query_weight = Decimal(query_tf.get((query_number, query_token)) * idf)
                document_weight = Decimal(tf * idf)
                var_1[(query_number, document_ID)] = var_1.get((query_number, document_ID), Decimal(0)) + Decimal(document_weight * query_weight)
                var_2[(query_number, document_ID)] = var_2.get((query_number, document_ID), Decimal(0)) + Decimal(math.pow(query_weight, 2))
                var_3[(query_number, document_ID)] = Decimal(math.pow(idf, 2)) * tf_pow_2.get(document_ID)
            query_retrieved_document_number[query_number] = len(var_1)
        for key in var_1.keys():
            similarity[key] = Decimal(var_1.get(key)) / Decimal(math.sqrt(var_2.get(key) * var_3.get(key)))

        sort_similarity(similarity, query_retrieved_document_number, 'TF_IDF')

    def BM25():
        query_output = open('./temp_output.csv', 'r')
        query_reader = csv.reader(query_output)
        similarity = dict()
        query_retrieved_document_number = dict()

        k1 = 1.2
        k2 = 500
        b = 0.75
        for data in query_reader:
            query_number = data[0]
            query_token = data[1]
            df = int(data[2])
            idf = math.log((document_number - df + 0.5) / (df + 0.5), 10)
            for i in range(3, len(data)):
                temp_tuple = tuple(eval(data[i]))
                document_ID = temp_tuple[0]
                tf = temp_tuple[1]
                document_weight = ((k1 + 1) * tf) / (tf + k1 * (1 - b + b * document_length_collection.get(document_ID) / average_length))
                query_weight = (k2 + 1) * query_tf.get((query_number, query_token)) / (k2 + query_tf.get((query_number, query_token)))
                similarity[(query_number, document_ID)] = similarity.get((query_number, document_ID), 0) + idf * document_weight * query_weight
            query_retrieved_document_number[query_number] = len(similarity)
        sort_similarity(similarity, query_retrieved_document_number, 'BM25')

    def language_model():
        query_output = open('./temp_output.csv', 'r')
        query_reader = csv.reader(query_output)
        similarity = dict()
        query_retrieved_document_number = dict()

        coefficient = 0.5
        tokens = total_each_token_tf()
        for data in query_reader:
            query_number = data[0]
            query_token = data[1]
            for i in range(3, len(data)):
                temp_tuple = tuple(eval(data[i]))
                document_ID = temp_tuple[0]
                tf = temp_tuple[1]
                probability = (1 - coefficient) * tf / document_length_collection.get(document_ID) + coefficient * tokens.get(query_token, 0) / sum_length
                similarity[(query_number, document_ID)] = similarity.get((query_number, document_ID), 0) - math.log(probability, 10)
            query_retrieved_document_number[query_number] = len(similarity)
        sort_similarity(similarity, query_retrieved_document_number, 'LanguageModel')

    start_time = time.time()
    query_compact()
    query_index_matcher()
    if retrieval_model == 'BM25':
        BM25()
        os.remove('./temp_output.csv')
    elif retrieval_model == 'LM':
        language_model()
        os.remove('./temp_output.csv')
    elif retrieval_model == 'VSM':
        cosine()
        os.remove('./temp_output.csv')
    end_time = time.time()
    running_time = end_time - start_time
    print("total running time: %s s" % running_time)
    os.remove('./compact_query.csv')


