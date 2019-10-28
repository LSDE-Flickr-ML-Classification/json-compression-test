import msgpack
import os
import json
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import Row

INPUT_FOLDER = "/home/corneliu/Downloads/original-output-large"
OUTPUT_FOLDER = "/home/corneliu/Downloads/output-large-compressed"
EXTENSION = ".json"
EXCLUDE_LIST = ['inverted_list.json', 'label_cloud.json', 'tag_list.json']


def get_json_as_object(json_file_location):
    json_file = open(json_file_location)
    json_raw = json_file.read()
    json_file.close()
    return json.loads(json_raw)


def write_bytes_to_file(bytes_content, output_file_location):
    bin_file = open(output_file_location, "wb")
    bin_file.write(bytes_content)
    bin_file.close()


def compress_and_store(file_location):
    json_obj = get_json_as_object(file_location) if EXTENSION == ".json" else None
    if json_obj is None:
        return
    msgpack_bytes = msgpack.packb(json_obj, use_bin_type=True)
    basename = os.path.splitext(os.path.basename(file_location))[0]
    full_output_file_path = OUTPUT_FOLDER + "/" + basename
    print("%s -> %s" % (file_location, full_output_file_path))
    write_bytes_to_file(msgpack_bytes, full_output_file_path)


spark = SparkSession.builder.master("local").appName("Word Count").getOrCreate()
sc = spark.sparkContext

all_file_paths = [os.path.join(INPUT_FOLDER, item) for item in os.listdir(INPUT_FOLDER) if item.endswith(EXTENSION) and item not in EXCLUDE_LIST]


def copy_metadata_to_folder(metadata_filename_list, location):
    for item in metadata_filename_list:
        full_path = INPUT_FOLDER + "/" + item
        print(full_path + "->" + location)
        shutil.copy(full_path, location)


copy_metadata_to_folder(EXCLUDE_LIST, OUTPUT_FOLDER)
sc.parallelize(all_file_paths).foreach(compress_and_store)
