from dweather_client.client import GRIDDED_DATASETS
import pickle
import os

def constructor(self, as_of, ipfs_timeout):
    pass

def get_data(self, lat, lon):
    to_open = os.path.join(os.path.dirname(__file__), "etc", f"{self.dataset}_{lat}_{lon}.p")
    with open(to_open, "rb") as f:
        return pickle.load(f)

def get_patched_datasets():
    patched_datasets = {}
    for k in GRIDDED_DATASETS:
        old_class = GRIDDED_DATASETS[k]
        new_class = type(old_class.__name__, (object, ), {
            "dataset": k,
            "__init__": constructor,
            "get_data": get_data
        })
        patched_datasets[k] = new_class
    return patched_datasets
        