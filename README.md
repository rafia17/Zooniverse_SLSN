# Zooniverse_SLSN - zoo-to-lasair-example branch

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
