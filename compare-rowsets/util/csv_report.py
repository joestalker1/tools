import gzip
from zipfile import ZipFile
import os
from .s3_client import S3Client


def open_file(file_name, local_dir):
    if file_name.endswith('.gz'):
        return gzip.open(file_name, 'rt')
    if file_name.endswith('.zip'):
        with ZipFile(file_name, 'r') as zip:
            zip.extractall(local_dir)
        norm_file_name = file_name[0: file_name.find('.zip')]
        return open(norm_file_name, 'rt')
    return open(file_name, 'rt')


def read_lines(f_in):
    for line in f_in:
        parts = line.split(',')
        new_line = []
        for part in parts:
            unescaped = part.split('\n')
            new_line.append(unescaped[0])
        if len(new_line):
            yield new_line


# assume it's abstract class
class Report:
    def __init__(self, file_name, local_dir):
        self.file_name = file_name
        self.local_dir = local_dir

    def open(self):
        pass

    def read(self):
        pass

    def close(self):
        pass


class CsvReport(Report):
    def __init__(self, file_name, local_dir='/tmp'):
        super().__init__(file_name, local_dir)
        self.file_desc = None

    def open(self):
        self.file_desc = open_file(self.file_name, "rt")

    def read(self):
        return read_lines(self.file_desc)

    def close(self):
        self.file_desc.close()


class S3CsvReport(Report):
    def __init__(self, file_name, local_dir, s3_access_key_id, s3_secret_access_key, s3_endpoint=''):
        if file_name.startswith('s3:'):
            file_name = file_name[3:]
        super().__init__(file_name, local_dir)
        self.temp_file = ''
        self.file_desc = None
        self.s3_client = S3Client(s3_access_key_id, s3_secret_access_key, s3_endpoint)

    def open(self):
        parsed_path = self.parse_bucket_path()
        found_resource = self.s3_client.list(parsed_path[0], parsed_path[1], parsed_path[2])
        if not found_resource:
            raise Exception('Cannot find the {}'.format(self.file_name))

        self.temp_file = '{}/temp_1_{}'.format(self.local_dir, parsed_path[2])
        self.s3_client.save_to_local_file(parsed_path[0], found_resource[0], self.temp_file)
        self.file_desc = open_file(self.temp_file, 'rt')

    def parse_bucket_path(self):
        parts = self.file_name.split('/')
        bucket = parts[0]
        file_name = parts[-1]
        prefix = '/'.join(parts[1:-1])
        return [bucket, prefix, file_name]

    def read(self):
        return read_lines(self.file_desc)

    def close(self):
        self.file_desc.close()
        os.remove(self.temp_file)
