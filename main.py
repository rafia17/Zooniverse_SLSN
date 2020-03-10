################
import sys
import os
from configparser import ConfigParser
from lasair_zooniverse import lasair_zooniverse_class
from lasair_zooniverse import lasair_object
import time


################################                
base_dir = os.getenv('LASAIR_CONFIG_PATH')

config = ConfigParser()    
config.read('%s\\config.ini' % (base_dir))

while(1):
    lasair_zoo = lasair_zooniverse_class(config.get('APP', 'KAFKA_SERVER'), config.get('APP', 'ENDPOINT'))
    objectIds = lasair_zoo.query_lasair_topic(config.get('APP','GROUP_ID'), config.get('APP','TOPIC'))

    if (config.get('APP','RECORDS_LIMIT')) != 'None':
        max_limit = config.getint('APP','RECORDS_LIMIT')
        objectIds = objectIds[:max_limit]
    proto_subjects = []
    for object_id in objectIds:
        lasair_zoo.wget_object_data(object_id, config.get('APP','URL'))
        proto_subject = lasair_zoo.produce_proto_subject(object_id, config.get('APP','PLOT_DIR'))
        proto_subjects.append(proto_subject)

    lasair_zoo.create_subjects_and_link_to_project(proto_subjects,config.get('APP','PROJECT_ID'), config.get('APP','WORKFLOW_ID'), None)
    time.sleep(config.getint('APP','SLEEP_TIME')) #sleep for one day


