# Lasair-Zooniverse Integration

This repository demonstrates integration of [lasair](https://lasair.roe.ac.uk/), a broker for astronomers studying transient and variable astrophysical sources and the [Zooniverse](https://www.zooniverse.org/) citizen science platform.  The code here was used to prepare daily data uploads based on new alerts from lasair for the [Superluminous Supernova citizen science project](https://www.zooniverse.org/projects/mrniaboc/superluminous-supernovae) hosted on Zooniverse.

## Installation

Clone this repo
```
$ git clone https://github.com/rafia17/Zooniverse_SLSN.git
```

install dependencies
```
$ pip install wget confluent-kafka matplotlib panoptes_client
```

install [panstamps](https://github.com/thespacedoctor/panstamps) which is required by the Superluminous Supernova project example
```
$ pip install panstamps
```

## Run example

To run the example do
```
$ python run.py
```

# In development

## Realtime processing of volunteer classifications with Caesar

### Installation

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
