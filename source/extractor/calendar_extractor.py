from queue import Empty
from queue import Full
import time
import boto3
from businesses.calendar_events import CalendarEvent
from dynamo_db_models.calendar_event_model import CalendarEventModel
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

class CalendarExtractor:
    """
    Load raw data from S3 (json_lines)
    Extract and transform raw data to CalendarEvent type
    Store CalendarEvent objects to DynamoDB
    """
    def __init__(self, from_s3_queue, to_db_queue, max_threads=20, sleeping_time=1, queue_timeout=0.5,batch_size=100,
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
        self.__command = {
            "stop": False
        }

    def start_work(self):
        """

        :return:
        """
        with ThreadPoolExecutor(max_workers=self.__max_threads) as executor:
            # executor.submit(self.process_from_s3_queue, self.__from_s3_queue, self.__to_db_queue)
            executor.submit(self.command, self.__command)
            for i in range(10):
                executor.submit(self.process_from_s3_queue, self.__from_s3_queue, self.__to_db_queue, self.__command)
            executor.submit(self.process_to_db_queue, self.__to_db_queue, self.__command)

    @staticmethod
    def command(cmd):
        """

        :param cmd:
        :return:
        """
        while not cmd["stop"]:
            txt = input("Press 1 to stop: \n")
            if txt == "1":
                cmd["stop"] = True
                print("STOP COMMAND IS SUBMITTED")


    @staticmethod
    def process_from_s3_queue(from_s3_queue, to_db_queue, command, trials_max=10, sleeping_time=1, time_out=0.1):
        """
        Get an item from __from_s3_queue
        Load raw object
        Transform it to CalendarEvent object
        Push CalendarEvent into __to_db_queue
        Repeat until the __from_s3_queue is not empty
        :return:
        """
        while not command["stop"]:
            print("Press 1 to stop: \n")
            try:
                # from_s3_queue = Queue(100)
                s3_obj = from_s3_queue.get(timeout=0.1)
                print("Thread 1 : %s" % (s3_obj))
                event_items = CalendarExtractor.load_and_transform(s3_obj)
                # print(event_items)
                for event_item in event_items:
                    try:
                        to_db_queue.put(item=event_item, timeout=0.1)
                    except Full:
                        time.sleep(sleeping_time)
                        pass

            except Empty:
                time.sleep(sleeping_time)

    @staticmethod
    def process_to_db_queue(to_db_queue, command, batch_size=100, sleeping_time=1, time_out=0.1):
        """
        Get item from
        :return:
        """
        def bath_write(w_items):
            with CalendarEventModel.batch_write() as batch:
                for w_item in w_items:
                    item = CalendarEventModel(w_item)
                    batch.save(item)
        items = []
        while not command["stop"]:
            print("Press 1 to stop: \n")
            try:
                event_item = to_db_queue.get(timeout=0.1)
                print("Thread 2 :%s" % event_item)
                items.append(event_item)
                if len(items) >= batch_size:
                    print("DYNAMO BATCH START")
                    bath_write(items)
                    items = []
                    print("DYNAMO OK")
            except Empty:
                print("QUEUE EMPTY")
                if len(items) > 0:
                    print("DYNAMO BATCH START")
                    bath_write(items)
                    items = []
                    print("DYNAMO OK")
                time.sleep(sleeping_time)

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
        bucket = s3_obj["Bucket"]
        key = s3_obj["Key"]

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







