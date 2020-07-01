import boto3
import logging

boto3.set_stream_logger('boto3.resources', logging.INFO)


class S3Client:
    def __init__(self, key_id, secret_token, endpoint=''):
        session = boto3.session.Session()
        if endpoint:
            self.s3_client = session.client(service_name='s3',
                                            aws_access_key_id=key_id,
                                            aws_secret_access_key=secret_token,
                                            endpoint_url=endpoint)
        else:
            self.s3_client = session.client(service_name='s3',
                                            aws_access_key_id=key_id,
                                            aws_secret_access_key=secret_token)

    def save_to_local_file(self, bucket, key, file_name):
        with open(file_name, 'wb') as f:
            self.s3_client.download_fileobj(bucket, key, f)

    def _get_matching_s3_keys(self, bucket, prefix='', suffix=''):
        """
        Generate the keys in an S3 bucket.

        :param bucket: Name of the S3 bucket.
        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).
        """
        kwargs = {'Bucket': bucket}

        # If the prefix is a single string (not a tuple of strings), we can
        # do the filtering directly in the S3 API.
        if isinstance(prefix, str):
            kwargs['Prefix'] = prefix

        while True:

            # The S3 API response is a large blob of metadata.
            # 'Contents' contains information about the listed objects.
            resp = self.s3_client.list_objects_v2(**kwargs)
            for obj in resp['Contents']:
                key = obj['Key']
                if key.startswith(prefix) and key.endswith(suffix):
                    yield key

            # The S3 API is paginated, returning up to 1000 keys at a time.
            # Pass the continuation token into the next response, until we
            # reach the final page (when this field is missing).
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def list(self, bucket, prefix, suffix):
        file_list = []

        for key in self._get_matching_s3_keys(bucket, prefix, suffix):
            file_list.append(key)

        return file_list
