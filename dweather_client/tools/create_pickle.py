import sys
import os
sys.path.insert(1, os.getcwd())

from dweather_client.client import GRIDDED_DATASETS
import pickle
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("dataset_name")
args = parser.parse_args()

def pickle_set(dataset_name):
    dataset_obj = GRIDDED_DATASETS[dataset_name](ipfs_timeout=20)
    data = dataset_obj.get_data(37, -83)
    with open(f"dweather_client/tests/etc/{dataset_name}_37_-83.p", "wb") as f:
        data = data[0], data[1].iloc[-5000:]
        pickle.dump(data, f)
    print(f'pickled {dataset_name} into dweather_client/tests/etc/{dataset_name}_37_-83.p')


pickle_set(args.dataset_name)