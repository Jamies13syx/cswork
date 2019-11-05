import math
import os
import re
import csv
import sys
import time

if __name__ == '__main__':
    args = list(sys.argv)
    phrase_index_path = args[1]
    position_index_path = args[2]
    index_path = args[3]
    query_path = args[4]
    result_path = args[5]
    result_path = result_path + 'result_dynamic.txt'

    query_tf = dict()

    document_number = 0
    sum_length = 0
    document_length_collection = dict()
    temp_path = os.listdir('.' + os.altsep + 'temp_position')
    for item in temp_path:
        document_number = document_number + 1
        document_path = os.path.join('.' + os.altsep + 'temp_position', item)
        f = open(document_path)
        document_length = len(f.readlines())
        document_length_collection[item.replace('.csv', '')] = document_length
        sum_length = sum_length + document_length
    average_length = sum_length / document_number

    def sort_similarity(similarity, query_retrieved_document_number, model_name):

        result_file = open(result_path, 'a')

        query_document_map = dict()
        key_list = list(query_retrieved_document_number)
        value_list = list(query_retrieved_document_number.values())
        query_document_map[key_list[0]] = value_list[0]
        for i in range(1, len(query_retrieved_document_number)):
            query_document_map[key_list[i]] = value_list[i] - value_list[i - 1]

        sub_similarity = dict()
        count = 1
        for key in similarity.keys():
            limit = query_document_map.get(key[0])
            if count <= limit:
                sub_similarity[key] = similarity.get(key)
                count = count + 1
            if count > limit:
                sorted_similarity = sorted(sub_similarity.items(), key=lambda x: x[1], reverse=True)
                rank = 1
                for data in sorted_similarity:
                    write_line = str(data[0][0]) + ' ' + '0' + ' ' + data[0][1] + ' ' + str(rank) + ' ' + str(data[1]) + ' ' + model_name
                    rank = rank + 1
                    result_file.write(write_line + '\n')
                    if rank > 100:
                        break
                sub_similarity.clear()
                count = 1

    def phrase_query_compact():
        compact_phrase_query = open('./compact_phrase_query.csv', 'w', newline='')
        query_file = open(query_path, 'r')
        csv_writer = csv.writer(compact_phrase_query)
        query_number = int()
        for line in query_file:
            if line.startswith('<num>'):
                query_number = re.findall(r'[0-9]+', line)[0]
            elif line.startswith('<title>'):
                query = line[15:].strip()
                query = query.replace('(', '').replace(')', '')
                query_token_list = re.split(r'[/\-\s]', query)
                if len(query_token_list) == 2:
                    for i in range(len(query_token_list) - 1):
                        phrase = query_token_list[i].lower() + ' ' + query_token_list[i + 1].lower()
                        query_tf[(query_number, phrase)] = query_tf.get((query_number, phrase), 0) + 1  # query weight compute
                        csv_writer.writerow((query_number, phrase))
                elif len(query_token_list) > 2:
                    for i in range(len(query_token_list) - 1):
                        phrase = query_token_list[i].lower() + ' ' + query_token_list[i + 1].lower()
                        query_tf[(query_number, phrase)] = query_tf.get((query_number, phrase), 0) + 1  # query weight compute
                        csv_writer.writerow((query_number, phrase))  # 2 term phrase
                    for i in range(len(query_token_list) - 2):
                        phrase = query_token_list[i].lower() + ' ' + query_token_list[i + 1].lower() + ' ' + query_token_list[i + 2].lower()
                        query_tf[(query_number, phrase)] = query_tf.get((query_number, phrase), 0) + 1  # query weight compute
                        csv_writer.writerow((query_number, phrase))  # 3 term phrase

    def phrase_query_index_matcher():
        temp_output = open('./temp_output.csv', 'a', newline='')
        temp_output_writer = csv.writer(temp_output)
        compact_phrase_query = open('./compact_phrase_query.csv', 'r')
        query_reader = csv.reader(compact_phrase_query)

        def position_record_finder(token):
            position_index = open(position_index_path, 'r')
            position_index_reader = csv.reader(position_index)
            for item in position_index_reader:
                if item[0] == token:
                    return item
            return

        for data in query_reader:
            query_number = data[0]
            output = [query_number]
            query_token = data[1]
            query_token_list = re.split(r'\s', query_token)
            phrase_index = open(phrase_index_path, 'r')
            phrase_index_reader = csv.reader(phrase_index)
            for record in phrase_index_reader:
                if record[0] == query_token:
                    output = output + record
            if len(output) == 1:  # don't find match in phrase index
                container = []
                for token in query_token_list:
                    container.append(position_record_finder(token))
                if len(container) >= 2 and None not in container:
                    doc_retrieved = dict()
                    for i in range(2, len(container[0])):
                        term_tuple = tuple(eval(container[0][i]))
                        document_ID = term_tuple[0]
                        position_list = term_tuple[2]
                        for j in range(len(position_list)):
                            position_list[j] = position_list[j] + 1
                        for k in range(2, len(container[1])):
                            next_term_tuple = tuple(eval(container[1][k]))
                            if next_term_tuple[0] == document_ID:
                                for position in next_term_tuple[2]:
                                    if position in position_list:
                                        doc_retrieved[document_ID] = doc_retrieved.get(document_ID, 0) + 1
                    df = len(doc_retrieved)
                    if df != 0:
                        finding = [query_token, df]
                        for key in doc_retrieved.keys():
                            finding = finding + [(key, doc_retrieved.get(key))]
                        output = output + finding

            if len(output) > 1:
                temp_output_writer.writerow(output)
                output.clear()

    def query_compact():
        compact_query = open('./compact_query.csv', 'w', newline='')
        query_file = open(query_path, 'r')
        csv_writer = csv.writer(compact_query)
        query_number = int()
        for line in query_file:
            if line.startswith('<num>'):
                query_number = re.findall(r'[0-9]+', line)[0]
            elif line.startswith('<title>'):
                query = line[15:].strip()
                query = query.replace('(', '').replace(')', '')
                query_token_list = re.split(r'[/\-\s]', query)
                for element in query_token_list:
                    element = element.lower()
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
                    output = [query_number] + record
                    temp_output_writer.writerow(output)
                    output.clear()

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


    phrase_query_compact()
    phrase_query_index_matcher()
    query_compact()
    query_index_matcher()  # pre-processing work for query
    start_time = time.time()
    BM25()
    end_time = time.time()
    running_time = end_time - start_time
    print("Implementing model time: %s s" % running_time)
    os.remove('./temp_output.csv')
    os.remove('./compact_phrase_query.csv')
    os.remove('./compact_query.csv')









