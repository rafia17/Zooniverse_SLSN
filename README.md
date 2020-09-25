=======
# Zooniverse_SLSN
Classify super luminous super novae based on their light curves

Setting up the environment:

1. Step 1: Install confluent kafka package in Anaconda using:
conda install -c conda-forge python-confluent-kafka

2. Step 2: Install plotly:
conda install -c plotly plotly=4.5.2

3. Step 3: install panstamps:
pip install panstamps

4. go to the directory where panstamps is installed and replace the file downloader.py with the version in repository

5. Install Panoptes client:
pip install panoptes-client

6. Set up environment variable LASAIR_CONFIG_PATH to the directory that points to your config.ini file
7. Make sure environment variables are set for PANOPTES_USERNAME and PANOPTES_PASSWORD

8. Config.ini settings:
RECORDS_LIMIT: 10 ## sets the max limit of subjects pulled from LASAIR to 10. To pull everything, set this to None.
GROUP_ID: Automatically pulls subjects from where last left off. Change to a new GROUP_ID, if you want to pull from the beginning of the kafka queue.

# caesar consumer

## Installation

```
conda env create --file environment.yml
```

You will also need to install miclaraia/caesar_external in the conda environment.

```
conda activate lasair
git clone https://github.com/miclaraia/caesar_external.git
cd caesar_external
pip install -e .
```

Next setup caesar_external following these steps relating to ```caesar config new```:
https://zooniverse.github.io/swap/setup.html#caesar-config-command-reference

I have shared the arguments to pass on slack.  At the moment you don't need to do any of the other steps on that webpage.

Once complete you should be able to run

```
python caesar_consumer.py
```
