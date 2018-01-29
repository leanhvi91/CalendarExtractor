import json
from utils.util import transfer, get_nested_value, get_time_stamp, extract_time_info


class Business:
    """

    """
    def __int__(self, text=None):
        """

        :return:
        """
        self.__type = None

    @staticmethod
    def load_text(text):
        """

        :param text:
        :return:
        """
        return {}


class CalendarEvent(Business):
    """
    Object of type CalendarEvent
    """
    def __int__(self):
        """

        :return:
        """
        self.__type = "CalendarEvent"

    @staticmethod
    def load_text(text):
        """
        Load CalendarEvent object from S3
        :param bucket:
        :param key:
        :return:
        """
        try:
            raw_obj = json.loads(text)
        except:
            raw_obj = {}
        obj = {}
        if raw_obj:
            transfer(raw_obj, "id", obj, "EventId")
            transfer(raw_obj, "creator.email", obj, "UserId")
            transfer(raw_obj, "organizer.email", obj, "Organizer")
            transfer(raw_obj, "attendees.email", obj, "Attendees")
            transfer(raw_obj, "end.dateTime", obj, "StartTime", get_time_stamp)
            transfer(raw_obj, "end.dateTime", obj, "EndTime", get_time_stamp)
            transfer(raw_obj, "created", obj, "CreatedTime", get_time_stamp)
            transfer(raw_obj, "updated", obj, "UpdatedTime", get_time_stamp)
            transfer(raw_obj, "start.timeZone", obj, "TimeZone")
            transfer(raw_obj, "summary", obj, "Summary")
            transfer(raw_obj, "description", obj, "Description")
            transfer(raw_obj, "location", obj, "Location")

            start_time = get_nested_value(raw_obj, "start.dateTime")
            if start_time:
                time_info = extract_time_info(start_time)
                obj["Year"] = time_info["year"]
                obj["Month"] = time_info["month"]
                obj["Day"] = time_info["day"]
                obj["WeekDay"] = time_info["weekday"]
                obj["Time"] = time_info["time"]

            if "UserId" in obj:
                obj["UserId"] = obj["UserId"].strip()
                activity_code = obj["UserId"]
                if "Summary" in obj:
                    obj["Summary"].strip()
                    activity_code = ("%s|%s" % (activity_code, obj["Summary"]))
                if "Description" in obj:
                    obj["Description"].strip()
                    activity_code = ("%s|%s" % (activity_code, obj["Description"]))
                obj["ActivityCode"] = activity_code

        return obj




class Activity(Business):
    """

    """
    def __int__(self):
        """

        :return:
        """
        self.__type = "Activity"
