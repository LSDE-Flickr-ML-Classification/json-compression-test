import os
import numpy as np
import time

from compressor_thread import CompressorThread

INPUT_FOLDER = '/media/corneliu/UbuntuFiles/projects/yfcc100m-awesome-keyword-search/public/data/'
EXTENSION = '.json'
EXCLUDE_LIST = ['inverted_list.json', 'label_cloud.json', 'tag_list.json']
OUTPUT_COMPRESSED = INPUT_FOLDER + '/compressed/'
NR_THREADS = 16


def get_partitions_of_files_for_input_folder(input_folder, nr_partitions, file_extension):
    files_to_be_compressed = []

    for item in os.listdir(input_folder):
        if item.endswith(file_extension) and item not in EXCLUDE_LIST:
            files_to_be_compressed.append(os.path.join(input_folder, item))

    return np.array_split(files_to_be_compressed, nr_partitions)


def create_output_file_if_not_exists(destination_location):
    if not os.path.exists(destination_location):
        print("Output directory does not exist! Creating one...")
        os.mkdir(destination_location)


def compress_folder_items_using_method(method, input_json_folder, output_folder, nr_partitions, file_extension):
    print("%s -> %s Using %s" % (input_json_folder, output_folder, method))
    time_started = time.time()

    create_output_file_if_not_exists(output_folder)
    partitions = get_partitions_of_files_for_input_folder(input_json_folder, nr_partitions, file_extension)

    nr_items = 0
    for partition in partitions:
        nr_items += len(partition)

    threads = []

    for partition in partitions:
        new_thread = CompressorThread(partition, OUTPUT_COMPRESSED, method, file_extension)
        threads.append(new_thread)
        new_thread.start()

    for thread in threads:
        thread.join()

    duration = time.time() - time_started
    items_per_sec = nr_items / duration

    print("Finished the compression using the %s method" % method)

    if CompressorThread.INPUT_FOLDER_SIZE_COUNTER == 0:
        percentage_decrease_all = 0
    else:
        percentage_decrease_all = 100 - ((CompressorThread.OUTPUT_FOLDER_SIZE_COUNTER /
                                          CompressorThread.INPUT_FOLDER_SIZE_COUNTER) * 100)

    compression_time_per_item = CompressorThread.TOTAL_COMPRESSION_DURATION / nr_items if nr_items != 0 else 0
    compressed_items_per_second = nr_items / CompressorThread.TOTAL_COMPRESSION_DURATION \
        if CompressorThread.TOTAL_COMPRESSION_DURATION != 0 else 0

    print("Stats for method: %s" % method)
    print("Initial folder size: %d" % CompressorThread.INPUT_FOLDER_SIZE_COUNTER)
    print("Output folder size: %d" % CompressorThread.OUTPUT_FOLDER_SIZE_COUNTER)
    print("Compression ratio: %.2f%%" % percentage_decrease_all)
    print("Duration: %.2fs" % duration)
    print("Number of items: %d" % nr_items)
    print("Items per second: %.2fitems/s" % items_per_sec)
    print("Total compression time: %.2fs" % CompressorThread.TOTAL_COMPRESSION_DURATION)
    print("Compression time per item: %.2fs" % compression_time_per_item)
    print("Nr items compressed per second: %.2fitems/s" % compressed_items_per_second)


def reset_variables():
    CompressorThread.TOTAL_COMPRESSION_DURATION = 0
    CompressorThread.OUTPUT_FOLDER_SIZE_COUNTER = 0
    CompressorThread.INPUT_FOLDER_SIZE_COUNTER = 0


if __name__ == '__main__':
    # compress_folder_items_using_method("ubjson", INPUT_FOLDER, OUTPUT_COMPRESSED, NR_THREADS, EXTENSION)
    reset_variables()
    # compress_folder_items_using_method("bson", INPUT_FOLDER, OUTPUT_COMPRESSED, NR_THREADS, EXTENSION)
    compress_folder_items_using_method("msgpack", INPUT_FOLDER, OUTPUT_COMPRESSED, NR_THREADS, EXTENSION)