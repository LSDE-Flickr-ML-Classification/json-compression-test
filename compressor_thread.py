import threading
import simpleubjson
import json
import os
import time
import bson
import msgpack

class CompressorThread(threading.Thread):
    INPUT_FOLDER_SIZE_COUNTER = 0
    OUTPUT_FOLDER_SIZE_COUNTER = 0
    TOTAL_COMPRESSION_DURATION = 0

    def __init__(self, items, output_folder, compression_type):
        threading.Thread.__init__(self)
        self.items = items
        self.output_folder = output_folder
        self.compression_type = compression_type

    def get_json_as_object(self, json_file_location):
        json_file = open(json_file_location)
        json_raw = json_file.read()
        json_file.close()
        return json.loads(json_raw)

    def compress_file_ub_json(self, json_file_location):
        parsed_json = self.get_json_as_object(json_file_location)
        compr_started_time = time.time()
        ubjson_bytes = simpleubjson.encode(parsed_json)
        duration_compression = time.time() - compr_started_time
        CompressorThread.TOTAL_COMPRESSION_DURATION += duration_compression
        return ubjson_bytes

    def compress_file_bson(self, json_file_location):
        parsed_json = self.get_json_as_object(json_file_location)
        compr_started_time = time.time()
        bson_bytes = bson.encode_array(parsed_json, [])
        duration_compression = time.time() - compr_started_time
        CompressorThread.TOTAL_COMPRESSION_DURATION += duration_compression
        return bson_bytes

    def compress_file_msgpack(self, json_file_location):
        parsed_json = self.get_json_as_object(json_file_location)
        compr_started = time.time()
        msgpack_bytes = msgpack.packb(parsed_json, use_bin_type=True)
        duration_compression = time.time() - compr_started
        CompressorThread.TOTAL_COMPRESSION_DURATION += duration_compression
        return msgpack_bytes

    def write_bytes_to_location(self, output_file_location, bytes_output):
        bin_json = open(output_file_location, "wb")
        bin_json.write(bytes_output)
        bin_json.close()

    def update_stats(self, initial_file_location, compressed_file_location, file_name):
        initial_size = os.path.getsize(initial_file_location)
        compressed_size = os.path.getsize(compressed_file_location)
        CompressorThread.INPUT_FOLDER_SIZE_COUNTER += initial_size
        CompressorThread.OUTPUT_FOLDER_SIZE_COUNTER += compressed_size

    def compress_and_store_file(self, input_location, output_location, file_name):
        bytes_compressed = self.call_compression_function(input_location)
        self.write_bytes_to_location(output_location, bytes_compressed)
        self.update_stats(input_location, output_location, file_name)

    def call_compression_function(self, input_location):
        if self.compression_type == 'ubjson':
            return self.compress_file_ub_json(input_location)
        elif self.compression_type == 'bson':
            return self.compress_file_bson(input_location)
        elif self.compression_type == 'msgpack':
            return self.compress_file_msgpack(input_location)

    def run(self):
        for item in self.items:
            basename = os.path.basename(item)
            output_file = self.output_folder + basename
            self.compress_and_store_file(item, output_file, basename)