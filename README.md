# dWeather-Python-Client

## Quickstart

Install dweather_client:

    pip3 install dweather_client
    
Get valid dataset names and associated hashes:

    http_client.get_heads()
    
Get the metadata for a given dataset name:

    http_client.get_metadata('chirps_05-daily')
    
Get a rainfall dict for a gridded dataset:

    http_client.get_rainfall_dict(41.175, -75.125, 'chirps_05-daily')

Get a rainfall dataframe:

    df_loader.get_rainfall_df(41.125, -75.125, 'chirps_05-daily')
    
Get a station dataframe:

    df_loader.get_station_rainfall_df('USW00024285')
    df_loader.get_station_temperature_df('USW00024285')
    df_loader.get_station_snow_df('USW00024285')

See further examples in `tests`

## Development

    cd dweather-python-client
    python3 -m venv .
    bin/pip3 install -r requirements.txt
    bin/python3 -m pytest dweather_clieht/tests

Some dweather_client features require an ipfs daemon to work.

### Download Go-IPFS version 0.6.0
See Assets list at the bottom of this page: https://github.com/ipfs/go-ipfs/releases/tag/v0.6.0
Download the build appropriate for your machine, or just download the source tar if you're not sure.

### Install go-IPFS

Unzip the file that you downloaded.

    tar xvfz {filename}.tar.gz

Move the binary into your path. `sudo` may be required for this.

    mv go-ipfs/ipfs /usr/local/bin/ipfs

Initialize a ~/.ipfs directory. This is where your files and config will be saved.

    ipfs init

### Configure go-IPFS

Remove default peers for performance.

    ipfs bootstrap rm --all

Add the dWeather server as a peer.

    ipfs bootstrap add  "/ip4/198.211.104.50/tcp/4001/p2p/QmWsAFSDajELyneR7LkMsgfaRk2ib1y3SEU7nQuXSNPsQV"

### Make sure go-IPFS it works

Start the IPFS daemon. You will need to have the daemon running to use some functionality of the dWeather client.

    ipfs daemon

In a new window, confirm that you can pull content.

    ipfs cat QmVsy2HZCi39ePJRpNqXEJvHgRMqjcyu1FLqgiFkPTMknq/USW00014704.csv.gz

Confirm that the dWeather server is a peer.

    ipfs swarm peers

### Install the Python dWeather Client

Create an isolated Python installation and install the dependencies.

    git clone https://github.com/Arbol-Project/dWeather-Python-Client.git
    cd dWeather-Python-Client
    python3 -m venv .
    bin/pip3 install -r requirements.txt

Run the tests, if you want.

    bin/python3 -m pytest -s --log-cli-level=20 dweather_client/tests

## Local data

Certain dweather functions will try to save query results locally to disk for faster performance on subsequent loads. This can be overridden by passing `pin=False` in these function calls.

Load the ipfs UI to browse what files are stored locally. Paste the following into a web browser. http://127.0.0.1:5001/webui

Navigate to Files, then click "pins." Content can be unpinned via the UI.

If you just want to remove everything, delete ~/.ipfs and rerun the installation and configuration from `ipfs init`.

    rm -rf ~/.ipfs

## Further documentation

See `tests` directory for example usage. Documented examples of usage should appear in a docs repository or in product-dev-notebook.
