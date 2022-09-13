#!/usr/bin/env python
# coding: utf-8
from io import StringIO, BytesIO
from logging import Logger
from typing import Optional

from google.cloud import storage

from config import cfg
from logs import get_logger

_LOGGER: Optional[Logger] = None


def log() -> Logger:
    global _LOGGER
    if not _LOGGER:
        _LOGGER = get_logger(__name__)
    return _LOGGER


log().debug('Initializing GCS module.')

storage_client = storage.Client()

# Name of the default project GCS bucket
DEFAULT_BUCKET = cfg.GCS_DEFAULT_BUCKET


def upload_file(dest_file_name: str = None, data: object = None, content_type: str = 'text/plain',
                bucket_name: str = DEFAULT_BUCKET,
                file_encoding: str = 'utf-16', encode: bool = True, public_bucket: bool = False, no_cache: bool = False,
                metadata: dict = None) -> str:
    """Uploads supplied data to the bucket with a specific destination file name.

    :param dest_file_name: name of the file in the bucket.
    :param data: the data to store in the file (bytes or str or StringIO or BytesIO)
    :param content_type: type of the content being uploaded.
    :param bucket_name: name of the bucket into which the content with the specified name  being uploaded.
    :param file_encoding: encoding to use
    :param encode: encode the stringIO again or does it come in the right format?
    :param public_bucket: set to True if uploading to a public bucket. Always provide a bucket name in this case!
    The bucket needs to have global (bucket-wide) permissions.
    :param no_cache: set to True if you want to disable caching of the uploaded file
    :param metadata: GCS metadata to be stored with the file
    :return: URL of the stored file
    """
    log().info('Uploading file "%s" of type "%s" to the GCS bucket "%s".', dest_file_name, content_type, bucket_name)
    unpacked_data = data
    if (public_bucket is True) & (bucket_name == DEFAULT_BUCKET):
        raise Exception(f"You cannot upload to a public bucket without providing a bucket name because the default "
                        f"bucket {DEFAULT_BUCKET} is private!")
    if isinstance(data, StringIO) or isinstance(data, BytesIO):
        log().info('Unpacking data from StringIO.')
        data.seek(0)
        unpacked_data = data.read()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(dest_file_name)
    if isinstance(unpacked_data, str) and encode is True:
        log().info('Encoding string data as ' + file_encoding + ' bytes.')
        unpacked_data = unpacked_data.encode(file_encoding)
    blob.upload_from_string(unpacked_data, content_type)
    if no_cache is not None:
        blob.cache_control = "no-cache, max-age=0"
        blob.patch()
        log().info(f"Set the blob to not use caching.")
    if metadata is not None:
        # allows to set metadata on the blob
        blob.metadata = metadata
        blob.patch()
        log().info(f"Set the following metadata on the file: {metadata}")
    if public_bucket is True:
        log().info('File "%s" uploaded successfully and is publicly accessible at "%s", size: "%s".',
                   blob.name, blob.public_url, blob.size)
        return blob.public_url
    else:  # private
        log().info('File "%s" uploaded successfully and privately to "%s", size: "%s"',
                   blob.name, blob.self_link, blob.size)
        return blob.self_link
