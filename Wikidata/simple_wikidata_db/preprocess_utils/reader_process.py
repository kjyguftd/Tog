import json
import logging
from datetime import datetime
from multiprocessing import Queue, Value
from pathlib import Path
import gzip
from tqdm import tqdm

logging.basicConfig(filename='logfile.log', level=logging.DEBUG)



def count_lines(input_file: Path, max_lines_to_read: int):
    cnt = 0
    # with gzip.open(input_file, 'rb') as f:
    #     for _ in tqdm(f):
    #         cnt += 1
    #         if max_lines_to_read > 0 and cnt >= max_lines_to_read:
    #             break
    # return cnt
    with open(input_file, 'r') as f:
        for _ in tqdm(f):
            cnt += 1
            if 0 < max_lines_to_read <= cnt:
                break
    return cnt

def read_data(input_file: Path, num_lines_read: Value, max_lines_to_read: int, work_queue: Queue):
    """
    Reads the data from the input file and pushes it to the output queue.
    :param input_file: Path to the input file.
    :param num_lines_read: Value to store the number of lines in the input file.
    :param max_lines_to_read: Maximum number of lines to read from the input file (for testing).
    :param work_queue: Queue to push the data to.
    """
    with open(input_file, "r") as f:
        num_lines = 0
        for ln in f:
            if isinstance(ln, bytes):
                if ln == b"[\n" or ln == b"]\n":
                    continue
                line_str = ln.decode('utf-8')
                if line_str.endswith(",\n"):  # all but the last element
                    obj = line_str[:-2]
                else:
                    obj = line_str
            else:
                if ln == b"[\n" or ln == b"]\n":
                    continue
                # line_str = ln.decode('utf-8')
                if ln.endswith(",\n"):  # all but the last element
                    obj = ln[:-2]
                else:
                    obj = ln
            num_lines += 1
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            logging.debug(f"Putting object into work_queue: {obj}, Time: {current_time}")
            work_queue.put(obj)
            logging.debug(f"Current size of work_queue: {work_queue.qsize()}, Time: {current_time}")

            if 0 < max_lines_to_read <= num_lines:
                break
    num_lines_read.value = num_lines
    return