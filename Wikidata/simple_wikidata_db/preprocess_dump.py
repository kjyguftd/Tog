import argparse
import multiprocessing
from multiprocessing import Queue, Process
from pathlib import Path
import time
import json

from Wikidata.simple_wikidata_db.preprocess_utils.reader_process import read_data
from Wikidata.simple_wikidata_db.preprocess_utils.worker_process import process_data
from Wikidata.simple_wikidata_db.preprocess_utils.writer_process import Writer, write_data


def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type=str, default='D:\PycharmProjects\DS\ToG\data\\blanton_simple.json', help='path to wikidata json file')
    parser.add_argument('--out_dir', type=str, default='data/', help='path to output directory')
    parser.add_argument('--language_id', type=str, default='en', help='language identifier')
    parser.add_argument('--processes', type=int, default=1, help="number of concurrent processes to spin off.")
    parser.add_argument('--batch_size', type=int, default=4)
    parser.add_argument('--num_lines_read', type=int, default=-1, help='Terminate after num_lines_read lines are read. Useful for debugging.')
    parser.add_argument('--num_lines_in_dump', type=int, default=-1, help='Number of lines in dump. If -1, we will count the number of lines.')
    return parser

# def read_data(input_file, num_lines_read, max_lines_to_read):
#     print("Reading data...")
#     try:
#         with open(input_file, 'r', encoding='utf-8') as f:
#             data = json.load(f)  # 读取整个 JSON 对象
#             print("Data loaded from file")
#             num_lines_read[0] = 1
#             print(f"Processed object: {data}")
#         return [data]  # 返回包含 JSON 对象的列表
#     except json.JSONDecodeError as e:
#         print(f"JSONDecodeError: {e}")
#     except Exception as e:
#         print(f"Unexpected error: {e}")
#     finally:
#         print("Exiting read_data function")
#     return []

# def process_data(language_id, data):
#     print("Starting process_data...")
#     processed_data = []
#     for json_obj in data:
#         try:
#             print(f"Processing data: {json_obj}")
#             # 模拟数据处理
#             processed_data.append(json_obj)  # 假设 process_json 函数返回相同的数据
#             print("Data processed")
#         except Exception as e:
#             print(f"Error processing data: {e} - Data: {json_obj}")
#     print("Exiting process_data function")
#     return processed_data

def main():
    start = time.time()
    args = get_arg_parser().parse_args()
    print(f"ARGS: {args}")

    maxsize = 10 * args.processes
    output_queue = Queue(maxsize=maxsize)
    work_queue = Queue(maxsize=maxsize)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True, parents=True)

    input_file = Path(args.input_file)
    assert input_file.exists(), f"Input file {input_file} does not exist"
    max_lines_to_read = args.num_lines_read

    # max_lines_to_read = args.num_lines_read
    # num_lines_read = [0]
    #
    # print("Starting read process...")
    # start = time.time()
    # data = read_data(input_file, num_lines_read, max_lines_to_read)
    # print(f"Done! Read {num_lines_read[0]} objects")
    #
    # if len(data) > 0:
    #     print(f"Data in memory: {len(data)} items")
    #
    #     # 处理数据
    #     processed_data = process_data(args.language_id, data)
    #
    #     # 写入数据
    #     write_data(out_dir, args.batch_size, processed_data)
    #
    # print(f"Finished processing in {time.time() - start}s")

    # Processes for reading/processing/writing
    num_lines_read = multiprocessing.Value("i", 0)
    read_process = Process(
        target=read_data,
        args=(input_file, num_lines_read, max_lines_to_read, work_queue)
    )

    read_process.start()

    write_process = Process(
        target=write_data,
        args=(out_dir, args.batch_size, output_queue)
    )
    write_process.start()

    work_processes = []
    for _ in range(max(1, args.processes - 2)):
        work_process = Process(
            target=process_data,
            args=(args.language_id, work_queue, output_queue)
        )
        work_process.daemon = True
        work_process.start()
        work_processes.append(work_process)

    read_process.join()
    print(f"Done! Read {num_lines_read.value} lines")
    # Cause all worker process to quit
    for work_process in work_processes:
        work_queue.put(None)
    # Now join the work processes
    for work_process in work_processes:
        work_process.join()
    output_queue.put(None)
    write_process.join()

    print(f"Finished processing {num_lines_read.value} in {time.time() - start}s")


if __name__ == "__main__":
    start = time.time()
    main()

