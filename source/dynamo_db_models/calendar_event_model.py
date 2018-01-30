from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, UnicodeSetAttribute,
    UTCDateTimeAttribute, ListAttribute
)
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection


class UserIdStartTimeIndex(GlobalSecondaryIndex):
    """
    This class represents a global secondary index
    """

    class Meta:
        # Index_name is optional, but can be provided to override the default name
        index_name = "UserId-StartTime-index"
        read_capacity_units = 2
        write_capacity_units = 1
        # All attributes are projected
        projection = AllProjection()

    # Map with UserID and StartTime in object
    UserId = UnicodeAttribute(hash_key=True)
    StartTime = NumberAttribute(default=0, range_key=True)


class ActivityCodeStartTimeIndex(GlobalSecondaryIndex):
    """

    """

    class Meta:
        # index_name is optional, but can be provided to override the default name
        index_name = "ActivityCode-StartTime-index"
        read_capacity_units = 2
        write_capacity_units = 1
        # All attributes are projected
        projection = AllProjection()

    # Map with attributes ActivityCode and StartTime in object
    ActivityCode = UnicodeAttribute(hash_key=True)
    StartTime = NumberAttribute(default=0, range_key=True)


class UserIdMonthIndex(GlobalSecondaryIndex):
    """

    """
    class Meta:
        # index_name is optional, but can be provided to override the default name
        index_name = "UserId-Month-index"
        read_capacity_units = 2
        write_capacity_units = 1
        # All attributes are projected
        projection = AllProjection()

    # Map with attributes UserId and Month in object
    UserId = UnicodeAttribute(hash_key=True)
    Month = NumberAttribute(default=0, range_key=True)


class UserIdWeekDayIndex(GlobalSecondaryIndex):
    """

    """
    class Meta:
        # index_name is optional, but can be provided to override the default name
        index_name = "UserId-WeekDay-index"
        read_capacity_units = 2
        write_capacity_units = 1
        # All attributes are projected
        projection = AllProjection()

    # Map with attributes UserId and WeekDay in object
    UserId = UnicodeAttribute(hash_key=True)
    WeekDay = NumberAttribute(default=0, range_key=True)


class UserIdTimeIndex(GlobalSecondaryIndex):
    """

    """
    class Meta:
        # index_name is optional, but can be provided to override the default name
        index_name = "UserId-Time-index"
        read_capacity_units = 2
        write_capacity_units = 1
        # All attributes are projected
        projection = AllProjection()

    # Map with attributes UserId and Time in object
    UserId = UnicodeAttribute(hash_key=True)
    Time = NumberAttribute(default=0, range_key=True)


class CalendarEventModel(Model):
    """

    """
    class Meta:
        table_name = "CalendarEvents"
        region = "ap-northeast-1"

    # Primary key
    EventId = UnicodeAttribute(hash_key=True)

    # Attributes
    UserId = UnicodeAttribute()
    StartTime = NumberAttribute()
    EndTime = NumberAttribute()
    Organizer = UnicodeAttribute(null=True)
    Attendees = ListAttribute(null=True)
    Summary = UnicodeAttribute(null=True)
    Description = UnicodeAttribute(null=True)
    Location = UnicodeAttribute(null=True)
    ActivityCode = UnicodeAttribute(null=True)
    Year = NumberAttribute(null=True)
    Month = NumberAttribute(null=True)
    Day = NumberAttribute(null=True)
    WeekDay = NumberAttribute(null=True)
    # Time of day in seconds
    Time = NumberAttribute(null=True)
    CreatedTime = NumberAttribute(null=True)
    UpdatedTime = NumberAttribute(null=True)

    # Indexes
    user_id_start_time_index = UserIdStartTimeIndex()
    activity_code_start_time_index = ActivityCodeStartTimeIndex()
    user_id_month_index = UserIdMonthIndex()
    user_id_weekday_index = UserIdWeekDayIndex()
    user_id_time_index = UserIdTimeIndex()

    def __init__(self, obj):
        """

        :param obj: object holding value to be passed in model
        """
        super().__init__(obj["EventId"])

        # self.EventId = obj["EventId"]
        self.UserId = obj["UserId"]
        self.StartTime = obj["StartTime"]
        self.EndTime = obj["EndTime"]
        self.ActivityCode = obj["ActivityCode"]
        self.Year = obj["Year"]
        self.Month = obj["Month"]
        self.Day = obj["Day"]
        self.WeekDay = obj["WeekDay"]
        self.Time = obj["Time"]
        self.CreatedTime = obj["CreatedTime"]
        self.UpdatedTime = obj["UpdatedTime"]
