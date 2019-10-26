import os
import msgpack
import time

ITEMS_LOCATION = '/home/corneliu/Downloads/output-large/compressed'

COUNT_ITEMS = 0
TOTAL_TIME = 0

def decompress_item(file_location):
    global COUNT_ITEMS
    global TOTAL_TIME
    with open(file_location, "rb") as bin_file:
        msg_bytes = bin_file.read()

        time_started = time.time()
        image_array = msgpack.unpackb(msg_bytes, use_list=True, raw=False)
        duration_unpacking = time.time() - time_started

        COUNT_ITEMS += 1
        TOTAL_TIME += duration_unpacking


if __name__ == '__main__':
    for item in os.listdir(ITEMS_LOCATION):
        if item.endswith('.json'):
            complete_file_location = os.path.join(ITEMS_LOCATION, item)
            decompress_item(complete_file_location)

    print("Total amount of items: %d" % COUNT_ITEMS)
    print("Time per 1 unpacking: %.2f" % (TOTAL_TIME / COUNT_ITEMS))
    print("Items unpacked per second: %.2f" % (COUNT_ITEMS / TOTAL_TIME))
