
# libraries
import dateutil
import ftplib
import pathlib

from pandas import Timestamp, NaT, Timedelta
import datetime


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

# classes
class TimestampPeriod(object):
    """A class to save a Timespan with a minimal and maximal Timestamp.
    """    
    COMPARE = {
        "inner": {
            0: max,
            1: min},
        "outer": {
            0: min,
            1: max}}

    def __init__(self, start, end):
        """Initiate a TimestampPeriod.

        Parameters
        ----------
        start : pd.Timestamp or similar
            The start of the Period.
        end : pd.Timestamp or similar
            The end of the Period.
        """
        if type(start) == datetime.date and type(end) == datetime.date:
            self.is_date = True
        else:
            self.is_date = False

        period = list([start, end])
        for i, tstp in enumerate(period):
            if type(tstp) != Timestamp:
                period[i] = Timestamp(tstp)

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
                period[i] = self.COMPARE[how][i](comp_list)

        # return the period
        if self.is_date and other.is_date:
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
        return any([tstp is NaT for tstp in self])

    def has_only_NaT(self):
        return all([tstp is NaT for tstp in self])

    def strftime(self, format="%Y-%m-%d %H:%M:%S"):
        out = [tstp.strftime(format) if tstp is not NaT else None
                    for tstp in self.get_period()]
        return out

    def inside(self, other):
        other = self._check_period(other)
        if self.start >= other.start and self.end <= other.end:
            return True
        else:
            return False

    def contains(self, other):
        other = self._check_period(other)
        return other.inside(self)

    def is_empty(self):
        if self.start is NaT and self.end is NaT:
            return True
        else:
            return False

    def get_sql_format_dict(self, format="%Y%m%d %H:%M"):
        period_str = self.strftime(format=format)
        period_str = [str(el).replace("None", "NULL") for el in period_str]
        return dict(min_tstp=period_str[0], max_tstp=period_str[1])


