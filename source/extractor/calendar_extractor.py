from queue import Empty
from queue import Full
import time
import boto3
import json


class CalendarExtractor:
    """
    Load raw data from S3 (json_lines)
    Extract and transform raw data to CalendarEvent type
    Store CalendarEvent objects to DynamoDB
    """
    def __init__(self, from_s3_queue, to_db_queue, max_threads=2, sleeping_time=10):
        """

        :param from_s3_queue: Concurrent queue containing source path of raw data in S3
        :param to_db_queue: Concurrent queue containing object that is ready to be stored into DynamoDB
        :param max_threads: number of a load_and_transform threads
        :param sleeping_time: sleeping time of a thread when __from_s3_queue is empty
        """
        self.__from_s3_queue = from_s3_queue
        self.__to_db_queue = to_db_queue
        self.__max_threads = max_threads
        self.__sleeping_time = sleeping_time

    def load_and_transform(self):
        """
        Get an item from __from_s3_queue
        Load raw object
        Transform it to CalendarEvent object
        Push CalendarEvent into __to_db_queue
        Repeat until the __from_s3_queue is not empty
        :return:
        """
        while True:
            try:
                s3_obj = self.__from_s3_queue.get()
                raw_items = self.load_raw(s3_obj)
                for raw_item in raw_items:
                    event_item = self.transform_event(raw_item)
                    try:
                        self.__to_db_queue.put(event_item)
                    except Full:
                        pass
            except Empty:
                time.sleep(self.__sleeping_time)

    def load_raw(self, s3_obj):
        """

        :param s3_obj: Description of file stored in S3
            Bucket: "bucket_name"
            Key: "full path"
        :return: list of objects parsed from file content
        """
        client = boto3.client("s3")
        bucket = s3_obj["bucket"]
        key = s3_obj["key"]

        result = client.get_object(Bucket=bucket, Key=key)

        # Read the object (not compressed):
        text = result["Body"].read().decode()
        return json.loads(text)


    def transform_event(self, raw_object):
        """
        Transform an raw object to an CalendarEvent object
        :param raw_object: raw object to be transform
        :return: CalendarEvent object
        """
        event_object = {}
        event_object[""]