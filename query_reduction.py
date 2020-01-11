import math
import sys
import os
import csv
import re
import time

if __name__ == '__main__':
    args = list(sys.argv)
    index_path = args[1]
    query_path = args[2]
    percentage = int(args[3])  # percentage set to reduce
    result_after_reduction_path = args[4]
    result_after_reduction_path = result_after_reduction_path + 'result_after_reduction_' + str(percentage) + '.txt'

    stop_word_path = '.' + os.altsep + 'stops.txt'
    stop_words_handler = open(stop_word_path)
    stop_words = stop_words_handler.read().splitlines()  # stop words loading
    stop_words_handler.close()

    query_tf = dict()

    document_number = 0
    sum_length = 0
    document_length_collection = dict()
    temp_path = os.listdir('.' + os.altsep + 'temp_single')
    for item in temp_path:
        document_number = document_number + 1
        document_path = os.path.join('.' + os.altsep + 'temp_single', item)
        f = open(document_path)
        document_length = len(f.readlines())
        document_length_collection[item.replace('.csv', '')] = document_length  # each document record its length
        sum_length = sum_length + document_length
        f.close()
    average_length = sum_length / document_number  # average length calculation

    def query_compact():
        compact_query = open('./compact_query.csv', 'w', newline='')
        query_file = open(query_path, 'r')
        csv_writer = csv.writer(compact_query)
        query_number = int()
        flag = False
        for line in query_file:
            if line.startswith('<num>'):
                query_number = re.findall(r'[0-9]+', line)[0]
            if line.startswith('<narr>'):
                flag = True
                continue
            if line.startswith('</top>'):
                flag = False
                continue
            if flag:
                query = line.strip()
                query_token_list = re.split(r'[\-_#@&$*^`()[\]<>/,.;:!?§×\'\"\s\n\t\r]', query)   # split into tokens
                for element in query_token_list:
                    if element:
                        element = element.lower()
                        if element in stop_words:
                            continue  # ignore stop words
                        query_tf[(query_number, element)] = query_tf.get((query_number, element), 0) + 1  # query weight compute
        for key_tuple in query_tf.keys():
            csv_writer.writerow(key_tuple)
        compact_query.close()
        query_file.close()

    def query_index_matcher():

        def write_to_temp_output(query_number, query_token_list):

            index = open(index_path, 'r')
            index_reader = csv.reader(index)
            for record in index_reader:
                temp_output = open('./temp_output.csv', 'a', newline='')
                temp_output_writer = csv.writer(temp_output)
                if record[0] in query_token_list:
                    output = [query_number] + record   # find posting list
                    temp_output_writer.writerow(output)
                    output.clear()
                else:
                    continue  # get temp_output
                temp_output.close()
            index.close()

        compact_query = open('./compact_query.csv', 'r')
        query_reader = csv.reader(compact_query)
        query_token_set = dict()
        for data in query_reader:
            query_number = data[0]
            query_token = data[1]
            query_token_set[query_number] = query_token_set.get(query_number, []) + [query_token]
            # format query_number: [token1, token2, token3]
        for query_number in query_token_set.keys():
            query_token_total_length = len(query_token_set.get(query_number))
            query_token_expected_length = int(query_token_total_length * percentage / 100)
            # print((query_token_total_length, query_token_expected_length))
            index = open(index_path, 'r')
            index_reader = csv.reader(index)
            selection_list = []
            query_token_list = []
            for record in index_reader:
                if query_token_set.get(query_number) is not None:
                    if record[0] in query_token_set.get(query_number):  # pre-calculate all tokens in respective query number set
                        df = int(record[1])
                        idf = math.log(document_number / df, 10)
                        selection_list = selection_list + [(record[0], idf)]  # format: [(token, idf)]
                    else:
                        continue
            index.close()

            def by_score(t):
                return -t[1]
            selection_list = sorted(selection_list, key=by_score)
            selection_list = selection_list[:query_token_expected_length]  # do reduction
            for token_tuple in selection_list:
                query_token_list = query_token_list + [token_tuple[0]]  # format: [token1, token2, token3] in sorted order
            write_to_temp_output(query_number, query_token_list)
        compact_query.close()

    def sort_similarity(similarity, query_retrieved_document_number, model_name):

        result_after_reduction_file = open(result_after_reduction_path, 'a')
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
                    result_after_reduction_file.write(write_line + '\n')
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

    query_compact()
    query_index_matcher()
    start_time = time.time()
    language_model()
    end_time = time.time()
    running_time = end_time - start_time
    print("Implementing model time: %s s" % running_time)
    os.remove('./temp_output.csv')
    os.remove('./compact_query.csv')

