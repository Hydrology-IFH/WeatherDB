"""
Some utilities functions and classes that are used in the module.
"""
# libraries
import dateutil
import ftplib
import pathlib
import re

from pandas import Timestamp, NaT, Timedelta
import datetime
from .connections import CDC_HOST


# functions
def get_ftp_file_list(ftp_conn, ftp_folders):
    """Get a list of files in the folders with their modification dates.

    Parameters
    ----------
    ftp_conn : ftplib.FTP
        Ftp connection.
    ftp_folders : list of str or pathlike object
        The directories on the ftp server to look for files.

    Returns
    -------
    list of tuples of strs
        A list of Tuples. Every tuple stands for one file.
        The tuple consists of (filepath, modification date).
    """
    # check types
    if type(ftp_folders) == str:
        ftp_folders = [ftp_folders]
    for i, ftp_folder in enumerate(ftp_folders):
        if issubclass(type(ftp_folder), pathlib.Path):
            ftp_folders[i] = ftp_folder.as_posix()

    try:
        ftp_conn.voidcmd("NOOP")
    except ftplib.all_errors:
        ftp_conn.connect()

    # get files and modification dates
    files = []
    for ftp_folder in ftp_folders:
        lines = []
        ftp_conn.dir(ftp_folder, lines.append)
        for line in lines:
            parts = line.split(maxsplit=9)
            filepath = ftp_folder + parts[8]
            modtime = dateutil.parser.parse(parts[5] + " " + parts[6] + " " + parts[7])
            files.append((filepath, modtime))

    return files

def get_cdc_file_list(ftp_folders):
    with ftplib.FTP(CDC_HOST) as ftp_con:
        ftp_con.login()
        files = get_ftp_file_list(ftp_con, ftp_folders)
    return files

# classes
class TimestampPeriod(object):
    """A class to save a Timespan with a minimal and maximal Timestamp.
    """    
    _COMPARE = {
        "inner": {
            0: max,
            1: min},
        "outer": {
            0: min,
            1: max}}
    _REGEX_HAS_TIME = re.compile(
        r"((^\d{6}[ \-\.]+)"+ # 991231
        r"|(^\d{8}[ \-\.]*)|" + # 19991231
        r"(^\d{1,4}[ \-\.]\d{1,2}[ \-\.]\d{1,4}[ \-\.]+))" + # 1999-12-31
        r"+(\d+)") # has additional numbers -> time

    def __init__(self, start, end, tzinfo="UTC"):
        """Initiate a TimestampPeriod.

        Parameters
        ----------
        start : pd.Timestamp or similar
            The start of the Period.
        end : pd.Timestamp or similar
            The end of the Period.
        tzinfo : str or datetime.timezone object or None, optional
            The timezone to set to the timestamps.
            If the timestamps already have a timezone they will get converted.
            If None, then the timezone is not changed or set.
            The default is "UTC".
        """
        # check if input is a date or a timestamp
        if ((type(start) == datetime.date and type(end) == datetime.date) or
            (type(start) == str and not self._REGEX_HAS_TIME.match(start) and 
             type(end) == str and not self._REGEX_HAS_TIME.match(end))):
            self.is_date = True
        else:
            self.is_date = False

        # convert to correct timestamp format
        period = list([start, end])
        for i, tstp in enumerate(period):
            if type(tstp) != Timestamp:
                period[i] = Timestamp(tstp)

            # check timezone
            self.tzinfo=tzinfo
            if tzinfo is not None:
                if period[i].tzinfo is None:
                    period[i] = period[i].tz_localize(tzinfo)
                else:
                    period[i] = period[i].tz_convert(tzinfo)

        self.start = period[0]
        self.end = period[1]

    @staticmethod
    def _check_period(period):
        if type(period) != TimestampPeriod:
            period = TimestampPeriod(*period)
        return period

    def union(self, other, how="inner"):
        """Unite 2 TimestampPeriods to one.

        Compares the Periods and computes a new one.

        Parameters
        ----------
        other : TimestampPeriod
            The other TimestampPeriod with whome to compare.
        how : str, optional
            How to compare the 2 TimestampPeriods.
            Can be "inner" or "outer".
            "inner": the maximal Timespan for both is computed.
            "outer": The minimal Timespan for both is computed.
            The default is "inner".

        Returns
        -------
        TimestampPeriod
            A new TimespanPeriod object uniting both TimestampPeriods.
        """        
        other = self._check_period(other)

        # check for daily period in elements
        tdsother = [Timedelta(0), Timedelta(0)]
        tdsself = [Timedelta(0), Timedelta(0)]
        if self.is_date and not other.is_date and not other.is_empty():
            tdsself[1] = Timedelta(
                hours=other.end.hour, 
                minutes=other.end.minute, 
                seconds=other.end.second)
        elif not self.is_date and other.is_date and not self.is_empty():
            tdsother[1] = Timedelta(
                hours=self.end.hour, 
                minutes=self.end.minute, 
                seconds=self.end.second)

        # check if empty and inner
        if how=="inner" and (self.is_empty() or other.is_empty()):
            return TimestampPeriod(None, None)

        # get the united period
        period = [None, None]
        for i in range(2):
            comp_list = [val + td 
                for val, td in zip([self[i], other[i]], 
                                   [tdsself[i], tdsother[i]])
                if type(val) == Timestamp]
            if len(comp_list) > 0:
                period[i] = self._COMPARE[how][i](comp_list)

        # check if end < start
        if period[0]>=period[1]:
            period = (None, None)

        # return the period
        if self.is_date and other.is_date and all(period):
            # if both were data periods, then the result should also be a date period
            return TimestampPeriod(
                *[val.date() for val in period
                             if type(val) == Timestamp])
        else:
            return TimestampPeriod(*period)

    def get_period(self):
        return (self.start, self.end)

    def __getitem__(self, key):
        if key == 0 or key == "start":
            return self.start
        elif key == 1 or key == "end":
            return self.end

    def __setitem__(self, key, value):
        if value != Timestamp:
            value = Timestamp(value)

        if key == 0 or key == "start":
            self.start = value
        elif key == 1 or key == "end":
            self.end = value

    def __iter__(self):
        return self.get_period().__iter__()
    
    def __str__(self):
        msg = "TimestampPeriod: {0} - {1}"
        if self.is_date:
            return msg.format(*self.strftime(format="%Y-%m-%d"))
        else:
            return msg.format(*self.strftime(format="%Y-%m-%d %H:%M:%S"))

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        other = self._check_period(other)
        
        if self.start == other.start and self.end == other.end:
            return True
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def has_NaT(self):
        """Has the TimestampPeriod at least one NaT. 
        
        This means that the start or end is not given.
        Normally this should never happen, because it makes no sense.

        Returns
        -------
        bool
            True if the TimestampPeriod has at least on NaT.
            False if the TimestampPeriod has at least a start or a end.
        """    
        return any([tstp is NaT for tstp in self])

    def has_only_NaT(self):
        """Has the TimestampPeriod only NaT, meaning is empty. 
        
        This means that the start and end is not given.

        Returns
        -------
        bool
            True if the TimestampPeriod is empty.
            False if the TimestampPeriod has a start and an end.
        """    
        return all([tstp is NaT for tstp in self])
    
    def is_empty(self):
        """Is the TimestampPeriod empty. 
        
        This means that the start and end is not given.

        Returns
        -------
        bool
            True if the TimestampPeriod is empty.
            False if the TimestampPeriod has a start and an end.
        """        
        return self.has_only_NaT()

    def strftime(self, format="%Y-%m-%d %H:%M:%S"):
        """Convert the TimestampPeriod to a list of strings.

        Formates the Timestamp as a string.

        Parameters
        ----------
        format : str, optional
            The Timestamp-format to use. 
            The Default is "%Y-%m-%d %H:%M:%S"

        Returns
        -------
        list of 2 strings
            A list of the start and end of the TimestampPeriod as formated string.
        """        
        out = [tstp.strftime(format) if tstp is not NaT else None
                    for tstp in self.get_period()]
        return out

    def inside(self, other):
        """Is the TimestampPeriod inside another TimestampPeriod?

        Parameters
        ----------
        other : Timestampperiod or tuple of 2 Timestamp or Timestamp strings
            The other Timestamp to test against.
            Test if this TimestampPeriod is inside the other.

        Returns
        -------
        bool
            True if this TimestampPeriod is inside the other.
            Meaning that the start is higher or equal than the others starts
            and the end is smaller than the others end.
        """        
        other = self._check_period(other)
        if self.start >= other.start and self.end <= other.end:
            return True
        else:
            return False

    def contains(self, other):
        """Does this TimestampPeriod contain another TimestampPeriod?

        Parameters
        ----------
        other : Timestampperiod or tuple of 2 Timestamp or Timestamp strings
            The other Timestamp to test against.
            Test if this TimestampPeriod contains the other.

        Returns
        -------
        bool
            True if this TimestampPeriod contains the other.
            Meaning that the start is smaller or equal than the others starts
            and the end is higher than the others end.
        """
        other = self._check_period(other)
        return other.inside(self)

    def get_sql_format_dict(self, format="'%Y%m%d %H:%M'"):
        """Get the dictionary to use in sql queries.

        Parameters
        ----------
        format : str, optional
            The Timestamp-format to use. 
            The Default is "'%Y%m%d %H:%M'"

        Returns
        -------
        dict
            a dictionary with 2 keys (min_tstp, max_tstp) and the corresponding Timestamp as formated string.
        """        
        period_str = self.strftime(format=format)
        period_str = [str(el).replace("None", "NULL") for el in period_str]
        return dict(min_tstp=period_str[0], max_tstp=period_str[1])

    def get_interval(self):
        """Get the interval of the TimestampPeriod.

        Returns
        -------
        pd.Timedelta
            The interval of this TimestampPeriod.
            E.G. Timedelta(2 days 12:30:12)
        """        
        return self.end - self.start

    def get_middle(self):
        """Get the middle Timestamp of the TimestampPeriod.

        Returns
        -------
        Timestamp
            The middle Timestamp of this TimestampPeriod.
        """
        middle = self.start + self.get_interval() / 2
        if self.is_date:
            middle = Timestamp(middle.date())
        if self.tzinfo is not None:
            if middle.tzinfo is None:
                middle = middle.tz_localize(self.tzinfo)
        return middle
    
    def copy(self):
        """Copy this TimestampPeriod.

        Returns
        -------
        TimestampPeriod
            a new TimestampPeriod object that is equal to this one.
        """        
        new = TimestampPeriod(self.start, self.end)
        new.is_date = self.is_date
        return new

    def expand_to_timestamp(self):
        if self.is_date:
            return TimestampPeriod(
                start=self.start,
                end=self.end + Timedelta(
                    hours=23, minutes=59, seconds=59, milliseconds=999))
        else:
            return self


