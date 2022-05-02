from dweather_client.client import GRIDDED_DATASETS
import pickle
import os

def constructor(self, as_of, ipfs_timeout):
    pass

def get_data(self, lat, lon):
    to_open = os.path.join(os.path.dirname(__file__), "etc", f"{self.dataset}_{lat}_{lon}.p")
    with open(to_open, "rb") as f:
        return pickle.load(f)

def dummy_enter(self):
    return self

def dummy_exit(self, exc_type, exc_val, exc_tb):
    if isinstance(exc_val, Exception):
        return False
    return True

def get_patched_datasets():
    patched_datasets = {}
    for k in GRIDDED_DATASETS:
        old_class = GRIDDED_DATASETS[k]
        new_class = type(old_class.__name__, (object, ), {
            "dataset": k,
            "__init__": constructor,
            "__enter__": dummy_enter,
            "__exit__": dummy_exit,
            "get_data": get_data
        })
        patched_datasets[k] = new_class
    return patched_datasets
        