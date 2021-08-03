from dweather_client.ipfs_queries import IpfsDataset
import datetime

class DateRangeRetriever(IpfsDataset):
    @property
    def dataset(self):
        return self._dataset

    def __init__(self, dataset, ipfs_timeout=None):
        super().__init__(ipfs_timeout=ipfs_timeout)
        self._dataset = dataset

    def get_data(self):
        """
        Returns a dict with keys of times generated and values of the time span for each metadata in the
        dataset's chain
        """
        super().get_data()
        hashes = self.traverse_ll(self.head)
        ret = {}
        for h in hashes:
            metadata = self.get_metadata(h)
            if "date range" in metadata:
                str_range = metadata["date range"]
            elif "date_range" in metadata:
                str_range = metadata["date_range"]
            else:
                raise KeyError("metadata has no date range key") 
            date_range = _convert_str_range(str_range)
            time_generated = datetime.datetime.fromisoformat(metadata["time generated"])
            ret[time_generated] = date_range      
        return ret

def _convert_str_range(str_range):
    return [datetime.datetime.fromisoformat(s) for s in str_range]

def _overlaps(slice, date_range):
    if len(slice) == 1:
        return date_range[1] >= slice[0]
    else:
        return slice[0] <= date_range[0] <= slice[1] or slice[0] <= date_range[1] <= slice[1]

def has_changed(slices, metadata_ranges, as_of):
    updates_since_last_run = [metadata_ranges[k] for k in metadata_ranges if k >= as_of]
    sorted_slices = sorted(slices, key=lambda x: x[0], reverse=True)
    for slice in sorted_slices:
        for date_range in updates_since_last_run:
            if _overlaps(slice, date_range):
                return True
    return False
