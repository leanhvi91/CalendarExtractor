import boto3
from queue import Queue
from extractor.calendar_extractor import CalendarExtractor
import json


def test_S3():
    BUCKET = "hrs-data"
    KEY = "raw/calendar/055hd8mols3uuq5heglvpcpe4u_20181010T113000Z.json"

    client = boto3.client("s3")

    result = client.get_object(Bucket=BUCKET, Key=KEY)

    # Read the object (not compressed):
    text = result["Body"].read().decode()

    print(text)

if __name__ == "__main__":

    from_s3_queue = Queue(100)
    to_db_queue = Queue(100)
    s3_obj = {
        "bucket": "hrs-data",
        "key": "raw/calendar/055hd8mols3uuq5heglvpcpe4u_20181010T113000Z.json"
    }

    extractor = CalendarExtractor(from_s3_queue, to_db_queue)
    event = extractor.load_raw(s3_obj)
    txt = json.dumps(event, indent=2)
    print(txt)