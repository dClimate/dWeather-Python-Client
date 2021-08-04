from dweather_client.ipfs_queries import IpfsDataset
import datetime

class DateRangeRetriever(IpfsDataset):
    """
    Instantiable class for pulling in date ranges from metadata generated after an as_of datetime
    """
    @property
    def dataset(self):
        return self._dataset

    def __init__(self, dataset, ipfs_timeout=None):
        super().__init__(ipfs_timeout=ipfs_timeout)
        self._dataset = dataset

    def get_data(self, as_of):
        """
        Returns a list of datetime ranges corresponding to all metadata generated after `as_of`
        """
        super().get_data()
        cur_metadata = self.get_metadata(self.head)
        time_generated = datetime.datetime.fromisoformat(cur_metadata["time generated"])

        ret = []
        while time_generated >= as_of:
            if "date range" in cur_metadata:
                str_range = cur_metadata["date range"]
            elif "date_range" in cur_metadata:
                str_range = cur_metadata["date_range"]
            else:
                raise KeyError("metadata has no date range key") 

            date_range = _convert_str_range(str_range)
            ret.append(date_range)

            try:
                prev_release = cur_metadata['previous hash']
            except KeyError:
                break
            if prev_release is not None:
                cur_metadata = self.get_metadata(prev_release)
                time_generated = datetime.datetime.fromisoformat(cur_metadata["time generated"])
            else:
                break
        return ret

def _convert_str_range(str_range):
    """
    Convert a list of ISO datetime strings `str_range` to datetime objects
    """
    return [datetime.datetime.fromisoformat(s) for s in str_range]

def _overlaps(slice, date_range):
    """
    Determine whether a datetime `slice` overlaps with a `date_range`. If `slice` has length 1,
    determines whether any of the date range falls after slice's only ewlement
    """
    if len(slice) == 1:
        return date_range[1] >= slice[0]
    else:
        return slice[0] <= date_range[0] <= slice[1] or slice[0] <= date_range[1] <= slice[1]

def has_changed(slices, metadata_ranges):
    """
    Determine whether a list of datetime slices overlaps with a list of datetime ranges
    """
    sorted_slices = sorted(slices, key=lambda x: x[0], reverse=True)
    for slice in sorted_slices:
        for date_range in reversed(metadata_ranges):
            if _overlaps(slice, date_range):
                return True
    return False
