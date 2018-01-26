import json
import boto3
from utils.util import transfer


class Business:
    """

    """
    def __int__(self):
        """

        :return:
        """
        self.__type = None

    def load_s3(self, bucket, key):
        """
        Load raw object from s3 json_line file
        :param bucket:
        :param key:
        :return:
        """
        try:
            client = boto3.client("s3")
            result = client.get_object(Bucket=bucket, Key=key)
            text = result["Body"].read().decode()
            raw_obj = json.loads(text)
        except:
            raw_obj = {}
        return raw_obj


class CalendarEvent(Business):
    """
    Object of type CalendarEvent
    """
    def __int__(self):
        """

        :return:
        """
        self.__type = "CalendarEvent"

    def load_s3(self, bucket, key):
        """
        Load CalendarEvent object from S3
        :param bucket:
        :param key:
        :return:
        """
        raw_obj = super().load_s3()
        obj = {}
        if raw_obj:
            transfer(raw_obj, "id", obj, "EventId")
            transfer(raw_obj, "creator.email", obj, "UserId")
            transfer(raw_obj, "organizer.email", obj, "Organizer")
            transfer(raw_obj, "attendees.email", obj, "Attendees")
            transfer(raw_obj, "start.dateTime", obj, "StartTime")
            transfer(raw_obj, "start", obj, "EndTime")
            transfer(raw_obj, "summary", obj, "Summary")
            transfer(raw_obj, "description", obj, "Description")
            transfer(raw_obj, "location", obj, "Location")
            transfer(raw_obj, "created")

        return obj


class Activity(Business):
    """

    """
    def __int__(self):
        """

        :return:
        """
        self.__type = "Activity"
