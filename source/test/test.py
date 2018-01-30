import boto3
from queue import Queue
from extractor.calendar_extractor import CalendarExtractor
import json
from dynamo_db_models.calendar_event_model import CalendarEventModel


def test_S3():

    BUCKET = "hrs-data"
    KEY = "raw/calendar/055hd8mols3uuq5heglvpcpe4u_20181010T113000Z.json"

    client = boto3.client("s3")

    result = client.get_object(Bucket=BUCKET, Key=KEY)

    # Read the object (not compressed):
    text = result["Body"].read().decode()

    print(text)


def bath_write(w_items):
    with CalendarEventModel.batch_write() as batch:
        for w_item in w_items:
            item = CalendarEventModel(w_item)
            batch.save(item)

if __name__ == "__main__":

    from_s3_queue = Queue(100)
    to_db_queue = Queue(100)

    client = boto3.client("s3")

    bucket = "hrs-data"

    objs = client.list_objects(Bucket=bucket, Prefix="raw/calendar/055hd8mols", MaxKeys=100)
    keys = [obj["Key"] for obj in objs["Contents"]]

    s3_objs = [{"Bucket": bucket, "Key": key} for key in keys]

    print(json.dumps(s3_objs, indent=2))

    for obj in s3_objs:
        from_s3_queue.put(obj)
    extractor = CalendarExtractor(from_s3_queue, to_db_queue)
    extractor.start_work()
