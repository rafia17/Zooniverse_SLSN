#!/usr/bin/env python
"""Get Superluminous Supernova candidate lightcurves from the Lasair broker and upload to Zooniverse, along with Pan-STARRS stamps.

Usage:
  %s [--configfile=<configfile>] [--test]
  %s (-h | --help)
  %s --version

Options:
  -h --help                   Show this screen.
  --version                   Show version.
  --configfile=<configfile>   Specify the config file [default: ./config.ini].
  --test                      Run in test mode, which appends a random UUID to the Kafka topic (always unique).

E.g.:

  %s
  %s --configfile=/tmp/config.ini
  %s --configfile=/tmp/config.ini --test
"""

import sys
import os
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
from gkutils.commonutils import Struct, cleanOptions
################
import sys
import logging
from configparser import ConfigParser
from lasair_zooniverse import lasair_zooniverse_class
from lasair_zooniverse import lasair_object
import time
import numpy as np
from lasair import LasairError, lasair_client as lasair
import uuid
################################   

def main():
  opts = docopt(__doc__, version='0.1')
  opts = cleanOptions(opts)
  options = Struct(**opts)

  # Read config file
  config = ConfigParser()
  config.read(options.configfile)
  log = logging.getLogger("main-logger")
  L = lasair(config.get('APP','LASAIR_TOKEN'), endpoint = 'https://lasair-ztf.lsst.ac.uk/api', timeout = 2.0)

  # Instantiate lasair-Zooniverse interface class
  lasair_zoo = lasair_zooniverse_class(config.get('APP', 'KAFKA_SERVER'),
                                       config.get('APP', 'ENDPOINT'))

  # Query lasair kafka stream for objects according to group id and the
  # topic (aka. Stream name at https://lasair.roe.ac.uk/streams/)
  groupId = config.get('APP','GROUP_ID')
  if options.test:
    # Add a random UUID to the group ID so the Kafka queue starts from scratch. (Should always be unique.)
    groupId = groupId + '_' + str(uuid.uuid4())
  objectIds = lasair_zoo.query_lasair_topic(groupId, config.get('APP','TOPIC'))

  # If a limit on the number of objects to process is set, truncate the object
  # list accordingly
  if (config.get('APP','RECORDS_LIMIT')) != 'None':
    max_limit = config.getint('APP','RECORDS_LIMIT')
    objectIds = objectIds[:max_limit]

  # Create an empty list to track the proto-subjects.
  proto_subjects = []

  # Iterate through the objects queried from lasair
  for object_id in objectIds:
    # Grab the lightcurve data for each subject from lasair
#    lasair_zoo.wget_objectdata(object_id,
#                               config.get('APP','URL'),
#                               config.get('APP','DATA_DIR'))
    lasair_zoo.get_objectdata_via_api(object_id, config.get('APP','DATA_DIR'), L)
  
    # Create a proto-subject for this objects.  A proto-subject gathers the
    # information required to construct a subject for the Zooniverse.  In this
    # case the lasair_zoo.produce_proto_subject produces a lightcurve plot,
    # from the data downloaded above from lasair.  This method also gets a
    # PanSTARRS-1 image at the transient location with panstamps
    # (https://github.com/thespacedoctor/panstamps).
    proto_subject = \
      lasair_zoo.produce_proto_subject(object_id, config.get('APP','DATA_DIR'))
  
    # If everything went well in the previous step add this to our list of
    # proto-subjects
    if (proto_subject != None):
      proto_subjects.append(proto_subject)

  # The lasair_zoo.create_subjects_and_link_to_project take the list of
  # proto-subjects and produce subjects for the Zooniverse project.  The method
  # also uploads the subjects to Zooniverse and links the to the project and
  # workflow corresponding to the project and workflow ids provided in the
  # config. In this example the subjects are linked to the Superluminous \
  # Supernova project.
  lasair_zoo.create_subjects_and_link_to_project(proto_subjects,
                                                 config.get('APP','PROJECT_ID'),
                                                 config.get('APP','WORKFLOW_ID'),
                                                 None)

if __name__ == '__main__':
  main()
