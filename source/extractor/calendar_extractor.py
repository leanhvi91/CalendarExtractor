from queue import Empty
from queue import Full
import time
import boto3
from businesses.calendar_events import CalendarEvent
from dynamo_db_models.calendar_event_model import CalendarEventModel


class CalendarExtractor:
    """
    Load raw data from S3 (json_lines)
    Extract and transform raw data to CalendarEvent type
    Store CalendarEvent objects to DynamoDB
    """
    def __init__(self, from_s3_queue, to_db_queue, max_threads=2, sleeping_time=10, queue_timeout=0.5,batch_size=100,
                 calendar_events_table="CalendarEvents"):
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
        self.__queue_timeout = queue_timeout
        self.__batch_size = batch_size
        self.__calendar_events_table = calendar_events_table

    def process_from_s3_queue(self, trials_max=10):
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
                event_items = self.load_and_transform(s3_obj)
                for event_item in event_items:
                    trials_count = 0
                    while trials_count < trials_max:
                        try:
                            self.__to_db_queue.put(item=event_item, timeout=self.__queue_timeout)
                        except Full:
                            trials_count += 1
                            time.sleep(self.__sleeping_time)
                            pass
            except Empty:
                time.sleep(self.__sleeping_time)

    @staticmethod
    def load_and_transform(s3_obj):
        """
        Load text file stored in S3
        TODO: Design partially loading for big json lines file
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
        raw_content = result["Body"].read().decode()
        lines = raw_content.split("\n")

        # Extracted calendar event objects
        event_items = []
        for line in lines:
            event_item = CalendarEvent.load_text(line) #.get_body()
            if event_item:
                event_items.append(event_item)

        return event_items

    def process_to_db_queue(self):
        """
        Get item from
        :return:
        """
        buffer = []
        # while True:
        #     try:
        #         event_item = self.__to_db_queue.get(timeout=self.__queue_timeout)
        #         buffer.append(event_item)
        #         if len(buffer) >= self.__batch_size:
        #             batch_put_items(items=buffer, table=self.__calendar_events_table)
        #     except Empty:
        #         if len(buffer) > 0:
        #             batch_put_items(items=buffer, table=self.__calendar_events_table)
        #         time.sleep(self.__sleeping_time)





