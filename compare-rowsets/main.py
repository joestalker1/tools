import sys
import logging
from collections import defaultdict
from util.csv_report import CsvReport, S3CsvReport

DESCRIPTION = '''
Compare two csv files taken from either S3 bucket or the local filesystem. Bucket path should be prefixed with s3:<bucket>/<path>/<resource>.
File is in CSV-format that can be either gzipped,zipped. You can set up key_index starts from 0 as column indices to unite them to one key to compare lines.
'''
S3_ENDPOINT = 'http://localhost:9000'
S3_ACCESS_KEY_ID = 'minioadmin'
S3_SECRET_ACCESS_KEY = 'minioadmin'
LOCAL_DIR = '/tmp'


# S3_ENDPOINT = ''
# S3_ACCESS_KEY_ID = 'AKIAQPAXNKJ2Q6THS23X'
# S3_SECRET_ACCESS_KEY = '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'


def setup_logging():
    global logger
    logger = logging.getLogger('compare-rowsets')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def make_key(arr, key_index):
    if not key_index:
        return '.'.join(arr)
    new_arr = [arr[i] for i in key_index]
    return '.'.join(new_arr)



def split_key(key):
    parts = key.split('.')
    return [part for part in parts]


def compare_resources(path1, path2, key_index = None):
    report1 = S3CsvReport(path1, LOCAL_DIR, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_ENDPOINT) if path1.startswith(
        's3:') else CsvReport(path1, LOCAL_DIR)
    report2 = S3CsvReport(path2, LOCAL_DIR, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_ENDPOINT) if path2.startswith(
        's3:') else CsvReport(path2, LOCAL_DIR)
    # put report1 lines to map
    report1.open()
    report2.open()
    path1_total = 0
    line_to_count = defaultdict(int)
    for line in report1.read():
        line_to_count[make_key(line, key_index)] += 1
        path1_total += 1
    # compare report1 lines to report2
    error = False
    diff_lines2 = 0
    path2_total = 0
    for line in report2.read():
        key = make_key(line, key_index)
        if key not in line_to_count or line_to_count[key] == 0:
            print('Error: {} is not found in {}'.format(split_key(key), path1))
            error = True
            diff_lines2 += 1
        else:
            line_to_count[key] -= 1
        path2_total += 1
    # check if we use all lines from report1
    diff_lines1 = 0
    for key, count in line_to_count.items():
        if count > 0:
            print('Error: {} is not found in {}'.format(split_key(key), path2))
            diff_lines1 += count
            error = True
    print('\n')
    print('{} has {} different lines of {}'.format(path1, diff_lines1, path1_total))
    print('{} has {} different lines of {}'.format(path2, diff_lines2, path2_total))
    report1.close()
    report2.close()
    return not error


if __name__ == '__main__':
    setup_logging()
    print(DESCRIPTION)
    if len(sys.argv) < 3:
        print('Usage: main.py <path1> <pat2> or main.py <path1> <path2> <key_index>')
    else:
        path1 = sys.argv[1]
        path2 = sys.argv[2]
        key_index = None
        if len(sys.argv) > 3:
            key_index = [int(a) for a in sys.argv[3].split(',')]
        logger.info('Comparing {} {}'.format(path1, path2))
        if compare_resources(path1, path2, key_index):
            print('\nTwo files are the SAME')
        else:
            print('\nTwo files are DIFFERENT.')
