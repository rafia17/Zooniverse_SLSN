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

Take a look at config.ini. which contains important parameters for this example.  The example script queries the [218SN-likethings](https://lasair.roe.ac.uk/myquery/44/) ```TOPIC``` from the lasair kafka stream at ```KAFKA_SERVER``` i.e. lasair.roe.ac.uk:9092.  The kafka stream returns all alerts added to that stream since the stream was last queried by the ```GROUP_ID```, 'Test' in this case.  For this example the number of alerts returned is limited (```RECORDS_LIMIT```) to 20.  The script downloads lightcurve data from ```URL```for each alert e.g. https://lasair.roe.ac.uk/object/ZTF20acufbmq.json and saves these to ```DATA_DIR```.  From the downloaded lightcurve data, the script produces a lightcurve plot and grabs a PanSTARRS-1 (PS1) image at the location of each alert (see example below) which are also saved to ```DATA_DIR```.  ***Note: If the kafka stream does not return any alerts it may be because the ```GROUP_ID``` in the config.ini has already queried all the latest alerts from the stream.  In this case try changing ```GROUP_ID``` to something else and try running the script again.***

The plots and PS1 image are required to produce *subjects* for upload to a specific Zooniverse workflow and project identified by ```WORKFLOW_ID``` and ```PROJECT_ID``` located at the Zooniverse endpoint (```ENDPOINT```).  ***Note: The example script will fail to upload to the Zooniverse project at this point unless you have the correct premissions for that project.***  To test the upload to Zooniverse [build a Zooniverse project of your own](https://help.zooniverse.org/getting-started/) and update the ```WORKFLOW_ID``` and ```PROJECT_ID``` in config.ini for your project.

**Example Subject data for ZTF20acufbmq**
<p float="left">
  <img src="https://github.com/rafia17/Zooniverse_SLSN/blob/master/ZTF20acufbmq_light_curve.jpeg" width="500"/>
  <img src="https://github.com/rafia17/Zooniverse_SLSN/blob/master/color__ra2.093337_dec33.089009_arcsec75_skycell2009.020.jpeg" width="200"/>
</p>

## In development

Realtime processing of volunteer classifications with Caesar

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

```
python caesar_consumer.py
```
