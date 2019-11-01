import gc
import os
import re
import csv
from nltk.stem import *
import sys
import time
import shutil


if __name__ == '__main__':

    def compact_document():
        flag = False
        collection = open(collection_path, 'w')
        collection.truncate()

        sample = os.listdir(args[1])  # args definition

        file_path_list = []
        for item in sample:
            file_path = os.path.join(args[1], item)
            file_path_list.append(file_path)

        for file in file_path_list:
            f = open(file)
            for line in f:
                line = line.strip()
                if line.startswith('<DOCNO>'):
                    collection.write(line)
                    continue
                if line.startswith('</DOC>'):
                    collection.write(line)
                    continue
                if line.startswith('<TEXT>'):
                    flag = True
                    continue
                if line.startswith('</TEXT>'):
                    flag = False
                    continue
                if flag:
                    if len(line) == 0:
                        continue
                    if re.match(r'\s', line) is not None:
                        continue
                    if line.startswith('<!--'):
                        continue
                    if re.search('&blank;', line) is not None:
                        line = re.sub('&blank;', '&', line)
                    if re.search('&hyph;', line) is not None:
                        line = re.sub('&hyph;', '-', line)
                    if re.search('&sect;', line) is not None:
                        line = re.sub('&sect;', '§', line)
                    if re.search('&times;', line) is not None:
                        line = re.sub('&times;', '×', line)
                    if re.search(',', line) is not None:
                        line = re.sub(',', ' ', line)  # remove comma in 1,000
                    if re.search(r'0\.0*', line) is not None:
                        line = re.sub(r' \.0*', '', line)  # remove .00
                    collection.write(line.lower())

    def merger_writer(path, write_list):
        with open(path, 'a', newline='') as index:
            csv_writer = csv.writer(index)
            csv_writer.writerow(write_list)

    def index_merger(path):

        temp = os.listdir('.' + os.altsep + 'temp')
        temp_path_list = []
        for item in temp:
            file_path = os.path.join('.' + os.altsep + 'temp', item)
            temp_path_list.append(file_path)
        merge_start_time = time.time()

        if len(args) < 5:
            temp_index = open('.' + os.altsep + 'temp.csv', 'a+', newline='')
            csv_writer = csv.writer(temp_index)
            for num in range(len(temp_path_list)):
                with open(temp_path_list[num]) as f1:
                    reader = csv.reader(f1)
                    for data in reader:
                        csv_writer.writerow(data)

            term_info = dict()
            document_frequency_counter = dict()
            index = dict()
            with open('.' + os.altsep + 'temp.csv', 'r', newline='') as csv_file:
                csv_reader = csv.reader(csv_file)
                for data in csv_reader:
                    term_tuple = data[1]  # (document_ID, term_frequency)
                    term_info.setdefault(data[0], []).append(term_tuple)
                    # term_info format: key: [(document_ID, term_frequency)]
                    document_frequency_counter[data[0]] = len(term_info.get(data[0]))
                    # count all triplets in list as document frequency
                for key in term_info.keys():
                    index[key] = [document_frequency_counter.get(key), term_info.get(key)]
                    # index format: key: [document_frequency, [(document_ID, term_frequency), (document_ID, term_frequency)]]
                for key in sorted(index.keys()):
                    write_list = [key, index.get(key)[0]]
                    for i in range(0, index.get(key)[0]):  # from document_frequency get the exact number of tuples
                        write_list.append(index.get(key)[1][i])
                        # format: [token, document_frequency, (document_ID, term_frequency), (document_ID, term_frequency)]
                    merger_writer(path, write_list)
                merge_end_time = time.time()
                merge_running_time = 1000 * (merge_end_time - merge_start_time)
                print("Merge running time: %s ms" % merge_running_time)
                gc.collect()
        else:
            limit = args[4]
            count = 0

            def two_way_merge(f1, f2, limit, count, temp_path_list):

                output = open('.' + os.altsep + 'temp' + os.altsep + 'output_%i.csv' % count, 'w')
                output_path = output.name
                temp_path_list.append(output_path)
                buffer = []

                def getKey(line):
                    key = line.split(',', 1)[0]
                    return key

                def getValue(line):
                    value = line.split(',', 1)[1].replace('\n', '').replace('\r', '')
                    return value

                def getBuffer(line, limit, path):
                    if len(buffer) == limit:
                        out = open(path, 'a')
                        out.writelines(sorted(buffer))
                        buffer.clear()
                        buffer.append(line)
                    else:
                        buffer.append(line)
                    return buffer

                reader_1 = open(f1, 'r')
                reader_2 = open(f2, 'r')

                line_1 = reader_1.readline()
                line_2 = reader_2.readline()

                while line_1 and line_2:
                    if getKey(line_1) < getKey(line_2):
                        buffer = getBuffer(line_1, limit, output_path)
                        line_1 = reader_1.readline()
                    elif getKey(line_1) > getKey(line_2):
                        buffer = getBuffer(line_2, limit, output_path)
                        line_2 = reader_2.readline()
                    else:
                        new_line = line_1.replace('\n', '').replace('\r', '') + ',' + getValue(line_2)
                        buffer = getBuffer(new_line, limit, output_path)
                        line_1 = reader_1.readline()
                        line_2 = reader_2.readline()

                if not line_1:
                    while line_2:
                        buffer = getBuffer(line_2, limit, output_path)
                        line_2 = reader_2.readline()

                if not line_2:
                    while line_1:
                        buffer = getBuffer(line_1, limit, output_path)
                        line_1 = reader_1.readline()

                out = open(output_path, 'a')
                out.writelines(sorted(buffer))

            while len(temp_path_list) >= 3:
                count = count + 1
                two_way_merge(temp_path_list[0], temp_path_list[1], limit, count, temp_path_list)
                os.remove(temp_path_list[0])
                os.remove(temp_path_list[1])
                del temp_path_list[0]
                del temp_path_list[0]

            two_way_merge(temp_path_list[0], temp_path_list[1], limit, count + 1, temp_path_list)
            os.remove(temp_path_list[0])
            os.remove(temp_path_list[1])
            del temp_path_list[0]
            del temp_path_list[0]

            merge_end_time = time.time()
            merge_running_time = 1000 * (merge_end_time - merge_start_time)
            print("Merge running time: %s ms" % merge_running_time)

    # def gettermID():
    #     f = open(collection_path)
    #     termID = dict()
    #     count = 0
    #     for line in f:
    #         if line.startswith('<DOCNO>'):
    #             continue
    #         if line.startswith('</DOC>'):
    #             continue
    #         for item in re.split(r'[@&*^`()[\-\]<>/,.;:!?§×\'\"\s]', line):
    #             if termID.get(item) is None:
    #                 count = count + 1
    #                 termID[item] = count
    #     return termID

    def position_index_tokenizer(f):
        document_ID = ''
        position = 0
        bag = []
        temp_start_time = time.time()
        for line in f:
            if line.startswith('<DOCNO>'):
                find_list = re.findall(r'\w*-\w*-\w*', line)
                document_ID = find_list[0]
                continue
            if line.startswith('</DOC>'):
                position = 0
                position_index_builder(bag)
                bag.clear()
                continue
            for element in re.split(r'[#@&$*^`()[\-\]<>/,.;:!?§×\'\"\s\n]', line):  # reduced to normal case
                if len(element) == 0:
                    continue
                position = position + 1
                bag.append((element, document_ID, position))  # add triples
        temp_end_time = time.time()
        temp_running_time = 1000 * (temp_end_time - temp_start_time)
        print("temp running time: %s ms" % temp_running_time)


    def position_index_builder(bag):
        if len(bag) != 0:
            term_frequency_counter = dict()
            position_counter = dict()
            document_ID = bag[0][1]
            for term in bag:
                term_frequency_counter[term[0]] = term_frequency_counter.get(term[0], 0) + 1
                position_counter.setdefault(term[0], []).append(term[2])
            position_temp_writer(document_ID, term_frequency_counter, position_counter)
            term_frequency_counter.clear()
            position_counter.clear()
            # write batch to temp


    def position_temp_writer(document_ID, term_frequency_counter, position_counter):
        with open('.' + os.altsep + 'temp' + os.altsep + '%s.csv' % document_ID, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            for key in term_frequency_counter.keys():
                write_list = [key, (document_ID, term_frequency_counter.get(key), position_counter.get(key))]
                # format:[termID, (token, document_ID, term_frequency, position list)]
                csv_writer.writerow(write_list)


    def stem_index_tokenizer(f):
        stemmer = PorterStemmer()
        document_ID = ''
        bag = []
        temp_start_time = time.time()
        for line in f:
            if line.startswith('<DOCNO>'):
                find_list = re.findall(r'\w*-\w*-\w*', line)
                document_ID = find_list[0]
                continue
            if line.startswith('</DOC>'):
                stem_index_builder(bag)
                bag.clear()
                continue
            for element in re.split(r'[#@&$*^`()[\-\]<>/,.;:!?§×\'\"\s\n]', line):  # reduced to normal case
                if len(element) == 0:
                    continue
                if element in stop_words:
                    continue
                bag.append((stemmer.stem(element), document_ID))  # add tuples
        temp_end_time = time.time()
        temp_running_time = 1000 * (temp_end_time - temp_start_time)
        print("temp running time: %s ms" % temp_running_time)

    def phrase_index_tokenizer(f):
        document_ID = ''
        bag = []
        temp_start_time = time.time()
        for line in f:
            if line.startswith('<DOCNO>'):
                find_list = re.findall(r'\w*-\w*-\w*', line)
                document_ID = find_list[0]
                continue
            if line.startswith('</DOC>'):
                phrase_index_builder(bag)
                bag.clear()
                continue
            if not line:
                continue
            for element in re.split(r'[#@&$*^`()[\-\]<>/,.;:!?§×\'\"\s\n]', line):  # reduced to normal case
                if len(element) == 0:
                    continue
                bag.append((element, document_ID))  # add tuples
        temp_end_time = time.time()
        temp_running_time = 1000 * (temp_end_time - temp_start_time)
        print("temp running time: %s ms" % temp_running_time)

    def phrase_index_builder(bag):
        if len(bag) >= 3:
            phrase_bag = []
            term_frequency_counter = dict()
            document_ID = bag[0][1]
            for i in range(len(bag) - 1):
                if bag[i][0] in stop_words:
                    continue
                elif bag[i + 1][0] in stop_words:
                    continue
                elif bag[i][0].isdigit():
                    continue
                elif bag[i + 1][0].isdigit():
                    continue
                else:
                    phrase = bag[i][0] + ' ' + bag[i + 1][0]
                    phrase_bag.append((phrase, document_ID))  # 2 word phrase

            for i in range(len(bag) - 2):
                if bag[i][0] in stop_words:
                    continue
                elif bag[i + 1][0] in stop_words:
                    continue
                elif bag[i + 2][0] in stop_words:
                    continue
                elif bag[i][0].isdigit():
                    continue
                elif bag[i + 1][0].isdigit():
                    continue
                elif bag[i + 2][0].isdigit():
                    continue
                else:
                    phrase = bag[i][0] + ' ' + bag[i + 1][0] + ' ' + bag[i + 2][0]
                    phrase_bag.append((phrase, bag[i][1]))  # 3 word phrase
            for term in phrase_bag:
                term_frequency_counter[term[0]] = term_frequency_counter.get(term[0], 0) + 1
            stem_temp_writer(document_ID, term_frequency_counter)
            term_frequency_counter.clear()
            # write batch to temp


    def phrase_index_merger():

        temp = os.listdir('.' + os.altsep + 'temp')
        temp_path_list = []
        for item in temp:
            file_path = os.path.join('.' + os.altsep + 'temp', item)
            temp_path_list.append(file_path)
        merge_start_time = time.time()
        if len(args) < 5:

            temp_index = open('.' + os.altsep + 'temp.csv', 'a', newline='')
            csv_writer = csv.writer(temp_index)
            for num in range(len(temp_path_list)):
                with open(temp_path_list[num]) as f1:
                    reader = csv.reader(f1)
                    for data in reader:
                        csv_writer.writerow(data)

            term_info = dict()
            document_frequency_counter = dict()
            index = dict()
            with open('.' + os.altsep + 'temp.csv', 'r', newline='') as csv_file:
                csv_reader = csv.reader(csv_file)
                for data in csv_reader:
                    term_tuple = data[1]  # (document_ID, term_frequency)
                    term_info.setdefault(data[0], []).append(term_tuple)
                    # term_info format: key: [(document_ID, term_frequency)]
                    document_frequency_counter[data[0]] = len(term_info.get(data[0]))
                    # count all triplets in list as document frequency
                for key in term_info.keys():
                    if document_frequency_counter.get(key) >= 10:
                        index[key] = [document_frequency_counter.get(key), term_info.get(key)]
                    # index format: key: [document_frequency, [(document_ID, term_frequency), (document_ID, term_frequency)]]
                for key in sorted(index.keys()):
                    write_list = [key, index.get(key)[0]]
                    for i in range(0, index.get(key)[0]):  # from document_frequency get the exact number of tuples
                        write_list.append(index.get(key)[1][i])
                        # format: [token, document_frequency, (document_ID, term_frequency), (document_ID, term_frequency)]
                    merger_writer(phrase_index_path, write_list)
                merge_end_time = time.time()
                merge_running_time = 1000 * (merge_end_time - merge_start_time)
                print("Merge running time: %s ms" % merge_running_time)
                gc.collect()
        else:
            limit = args[4]
            count = 0

            def two_way_merge(f1, f2, limit, count, temp_path_list):

                output = open('.' + os.altsep + 'temp' + os.altsep + 'output_%i.csv' % count, 'w')
                output_path = output.name
                temp_path_list.append(output_path)
                buffer = []

                def getKey(line):
                    key = line.split(',', 1)[0]
                    return key

                def getValue(line):
                    value = line.split(',', 1)[1].replace('\n', '').replace('\r', '')
                    return value

                def getBuffer(line, limit, path):
                    if len(buffer) == limit:
                        out = open(path, 'a')
                        out.writelines(buffer)
                        buffer.clear()
                        buffer.append(line)
                    else:
                        buffer.append(line)
                    return buffer

                reader_1 = open(f1, 'r')
                reader_2 = open(f2, 'r')

                line_1 = reader_1.readline()
                line_2 = reader_2.readline()

                while len(line_1) != 0 and len(line_2) != 0:
                    if getKey(line_1) < getKey(line_2):
                        buffer = getBuffer(line_1, limit, output_path)
                        line_1 = reader_1.readline()
                    elif getKey(line_1) > getKey(line_2):
                        buffer = getBuffer(line_2, limit, output_path)
                        line_2 = reader_2.readline()
                    else:
                        new_line = line_1.replace('\n', '').replace('\r', '') + ',' + getValue(line_2)
                        buffer = getBuffer(new_line, limit, output_path)
                        line_1 = reader_1.readline()
                        line_2 = reader_2.readline()

                if len(line_1) == 0:
                    while len(line_2) != 0:
                        buffer = getBuffer(line_2, limit, output_path)
                        line_2 = reader_2.readline()

                if len(line_2) == 0:
                    while len(line_1) != 0:
                        buffer = getBuffer(line_1, limit, output_path)
                        line_1 = reader_1.readline()

                out = open(output_path, 'a')
                out.writelines(buffer)

            while len(temp_path_list) >= 3:
                count = count + 1
                two_way_merge(temp_path_list[0], temp_path_list[1], limit, count, temp_path_list)
                os.remove(temp_path_list[0])
                os.remove(temp_path_list[1])
                del temp_path_list[0]
                del temp_path_list[0]

            two_way_merge(temp_path_list[0], temp_path_list[1], limit, count + 1, temp_path_list)
            os.remove(temp_path_list[0])
            os.remove(temp_path_list[1])
            del temp_path_list[0]
            del temp_path_list[0]
            merge_end_time = time.time()
            merge_running_time = 1000 * (merge_end_time - merge_start_time)
            print("Merge running time: %s ms" % merge_running_time)


    def stem_index_builder(bag):
        if len(bag) != 0:
            term_frequency_counter = dict()
            document_ID = bag[0][1]
            for term in bag:
                term_frequency_counter[term[0]] = term_frequency_counter.get(term[0], 0) + 1
            stem_temp_writer(document_ID, term_frequency_counter)
            term_frequency_counter.clear()
            # write batch to temp


    def stem_temp_writer(document_ID, term_frequency_counter):
        with open('.' + os.altsep + 'temp' + os.altsep + '%s.csv' % document_ID, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            for key in term_frequency_counter.keys():
                write_list = [key, (document_ID, term_frequency_counter.get(key))]
                # format:[termID, token, document_ID, term_frequency]
                csv_writer.writerow(write_list)

    def single_index_tokenizer(f):
        document_ID = ''
        bag = []
        temp_start_time = time.time()
        for line in f:
            if line.startswith('<DOCNO>'):
                find_list = re.findall(r'\w*-\w*-\w*', line)
                document_ID = find_list[0]
                continue
            if line.startswith('</DOC>'):
                stem_index_builder(bag)
                bag.clear()
                gc.collect()
                continue
            if not line:
                continue
            normalization(line, bag, document_ID)
        temp_end_time = time.time()
        temp_running_time = 1000 * (temp_end_time - temp_start_time)
        print("temp running time: %s ms" % temp_running_time)

    def normalization(line, bag, document_ID):

        email = re.findall(r'[a-z0-9.\-+_]+@[a-z0-9.\-+_]+\.[a-z]+', line)
        if email:
            for item in email:
                bag.append((item, document_ID))
            line = re.sub(r'[a-z0-9.\-+_]+@[a-z0-9.\-+_]+\.[a-z]+', '', line)  # handle email
            gc.collect()

        monetary = re.findall(r'\$[0-9]*', line)
        if monetary:
            for item in monetary:
                bag.append((item, document_ID))
            line = re.sub(r'\$[0-9]*', '', line)  # handle monetary
            gc.collect()

        dates = re.findall(r'\d{2}[-/]\d{2}[-/]\d{4}', line) + re.findall(r'\w*\s\d{1,2},\s\d{4}', line)
        if dates:
            for item in dates:
                bag.append((item, document_ID))
            line = re.sub(r'\d{2}[-/]\d{2}[-/]\d{4}', '', line)
            line = re.sub(r'\w*\s\d{1,2},\s\d{4}', '', line)  # handle date
            gc.collect()

        hyph_collection = re.findall(r'[A-Z0-9]+-[A-Z0-9]+', line)
        if hyph_collection:
            for token in hyph_collection:
                token = re.sub('-', '', token)
                bag.append((token, document_ID))  # store F-16 into F16

        letters = re.findall(r'[A-Z0-9]+-', line)
        if letters:
            for item in letters:
                if len(item) >= 4:
                    bag.append((item[0:len(item) - 1], document_ID))  # store word/number length >= 3

        numbers = re.findall(r'-[A-Z0-9]+', line)
        if numbers:
            for item in numbers:
                if len(item) >= 4:
                    bag.append((item[1:], document_ID))  # store word/number length >= 3
            line = re.sub(r'[A-Z0-9]+-[A-Z0-9]+', '', line)  # handle F-16/ I-20/1 hour/prefix
            gc.collect()

        dot_collection = re.findall(r'[A-Z]+.[A-Z]+.*', line) + re.findall(r'[A-Z]+.[A-Z]+.[A-Z]+', line)
        #  handle Ph.D/ PH.D./U.S.A
        if dot_collection:
            for item in dot_collection:
                bag.append((re.sub('.', '', item), document_ID))
            line = re.sub(r'[A-Z]+.[A-Z]+.*', '', line)
            line = re.sub(r'[A-Z]+.[A-Z]+.[A-Z]+', '', line)
            gc.collect()

        for element in re.split(r'[#@&$*^`()[\-\]<>/,.;:!?§×\'\"\s\n]', line):  # reduced to normal case
            if len(element) == 0:
                continue
            if element in stop_words:
                continue
            bag.append((element.strip(), document_ID))  # add tuples

    args = list(sys.argv)
    stop_word_path = '.' + os.altsep + 'stops.txt'
    stop_words_handler = open(stop_word_path)
    stop_words = stop_words_handler.read().splitlines()

    collection_path = '.' + os.altsep + 'documents.txt'
    start_time = time.time()
    try:
        compact_document()

    except FileNotFoundError:
        print("invalid sample path")
        exit(1)
    # termID = gettermID()
    if not os.path.exists('.' + os.altsep + 'temp'):
        os.makedirs('.' + os.altsep + 'temp')
    if not os.path.exists(args[3]):
        os.makedirs(args[3])

    if len(args) < 5:
        if args[2] == 'stem':
            stem_index_path = os.path.join(args[3], 'stem_index.csv')
            open(stem_index_path, 'w').close()
            stem_index_tokenizer(open(collection_path))
            index_merger(stem_index_path)
            open('.' + os.altsep + 'temp.csv', 'w').truncate()
            gc.collect()
        elif args[2] == 'position':
            position_index_path = os.path.join(args[3], 'single_term_positional_index.csv')
            open(position_index_path, 'w').close()
            position_index_tokenizer(open(collection_path))
            index_merger(position_index_path)
            open('.' + os.altsep + 'temp.csv', 'w').truncate()
            gc.collect()
        elif args[2] == 'phrase':
            phrase_index_path = os.path.join(args[3], 'phrase_index.csv')
            open(phrase_index_path, 'w').close()
            phrase_index_tokenizer(open(collection_path))
            phrase_index_merger()
            open('.' + os.altsep + 'temp.csv', 'w').truncate()
            gc.collect()
        elif args[2] == 'single':
            single_index_path = os.path.join(args[3], 'single_term_index.csv')
            open(single_index_path, 'w').close()
            single_index_tokenizer(open(collection_path))
            index_merger(single_index_path)
            open('.' + os.altsep + 'temp.csv', 'w').truncate()
            gc.collect()
        else:
            print('invalid index type')
            exit(0)
    else:
        if args[2] == 'stem':
            stem_index_path = os.path.join(args[3], 'stem_index.csv')
            stem_index_tokenizer(open(collection_path))
            index_merger(stem_index_path)
            temp = os.listdir('.' + os.altsep + 'temp')
            temp_path_list = []
            for item in temp:
                file_path = os.path.join('.' + os.altsep + 'temp', item)
                temp_path_list.append(file_path)
            old_name = temp_path_list[0]
            new_name = os.path.join('.' + os.altsep + 'temp', 'stem_index.csv')
            os.rename(old_name, new_name)
            shutil.move(new_name, stem_index_path)
            open('.' + os.altsep + 'temp.csv', 'w').truncate()
            gc.collect()

        elif args[2] == 'position':
            position_index_path = os.path.join(args[3], 'single_term_positional_index.csv')
            position_index_tokenizer(open(collection_path))
            index_merger(position_index_path)
            temp = os.listdir('.' + os.altsep + 'temp')
            temp_path_list = []
            for item in temp:
                file_path = os.path.join('.' + os.altsep + 'temp', item)
                temp_path_list.append(file_path)
            old_name = temp_path_list[0]
            new_name = os.path.join('.' + os.altsep + 'temp', 'single_term_positional_index.csv')
            os.rename(old_name, new_name)
            shutil.move(new_name, position_index_path)
            open('.' + os.altsep + 'temp.csv', 'w').truncate()
            gc.collect()
        elif args[2] == 'phrase':
            phrase_index_path = os.path.join(args[3], 'phrase_index.csv')
            phrase_index_tokenizer(open(collection_path))
            phrase_index_merger()
            temp = os.listdir('.' + os.altsep + 'temp')
            temp_path_list = []
            for item in temp:
                file_path = os.path.join('.' + os.altsep + 'temp', item)
                temp_path_list.append(file_path)
            old_name = temp_path_list[0]
            new_name = os.path.join('.' + os.altsep + 'temp', 'phrase_index.csv')
            os.rename(old_name, new_name)
            shutil.move(new_name, phrase_index_path)
            open('.' + os.altsep + 'temp.csv', 'w').truncate()
            gc.collect()
        elif args[2] == 'single':
            single_index_path = os.path.join(args[3], 'single_term_index.csv')
            single_index_tokenizer(open(collection_path))
            index_merger(single_index_path)
            temp = os.listdir('.' + os.altsep + 'temp')
            temp_path_list = []
            for item in temp:
                file_path = os.path.join('.' + os.altsep + 'temp', item)
                temp_path_list.append(file_path)
            old_name = temp_path_list[0]
            new_name = os.path.join('.' + os.altsep + 'temp', 'single_term_index.csv')
            os.rename(old_name, new_name)
            shutil.move(new_name, single_index_path)
            open('.' + os.altsep + 'temp.csv', 'w').truncate()
            gc.collect()
        else:
            print('invalid index type')
            exit(0)
    end_time = time.time()
    running_time = 1000 * (end_time - start_time)
    print("total running time: %s ms" % running_time)
