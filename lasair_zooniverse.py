import os
import time
import json
import wget
import lasair_consumer
from lasair_consumer import msgConsumer

from lasair_zooniverse_base import lasair_zooniverse_base_class
import logging

# 3rd party imports
import plotly.graph_objs as go
from panoptes_client import Panoptes, Project, SubjectSet, Subject, Workflow
from panstamps import __version__
from panstamps import cl_utils
from panstamps import utKit
from panstamps.downloader import downloader

class lasair_object:

    def __init__(self):
        'default constructor'

    def __init__(self, objectId, ra, dec, stamp, lightcurve_plot):
        self.objectId = objectId
        self.ramean = ra
        self.decmean = dec
        self.stamp = stamp
        self.lightcurve_plot = lightcurve_plot
    
    Detections = []


def get_objectId(msg):
    msgString= msg.decode("utf-8")
    step_0 = msgString.split(',')
    for pair in step_0:
        step_1 = pair.split(':')
        if step_1[0][2:9] =='objectI':
            objectname = step_1[1].replace('"', '')
            objectname = objectname.replace(' ', '')
    return objectname                


class  lasair_zooniverse_class(lasair_zooniverse_base_class):

    def __init__(self, kafka_server, ENDPOINT):
        self.kafka_server = kafka_server
        self.ENDPOINT = ENDPOINT

    def query_lasair_topic(self, group_id, topic):
        c = msgConsumer(self.kafka_server, group_id)
        c.subscribe(topic)
        print('Topics are ', c.topics())
        start = time.time()
        objectIds = []

        while 1:
            msg = c.poll()
            print(msg)
            if msg == None:
                break
            else:
                objectIds.append(get_objectId(msg))
        print('======= %.1f seconds =========' % ((time.time()-start)))
        print('poll done')
        return objectIds

    def wget_object_data(self, objectId, url):
        url = url % (objectId)
        wget.download(url, objectId + '.json')


    def produce_proto_subject(self, unique_id, plot_dir):
        # UMN TODO
        # for SLSNe project unique_id = object_id?
        # produce plots and gather metadata for each subject to be created
        lasair_zobject = self.parse_object_data(unique_id)
        light_curve, panstamps = self.build_plots(lasair_zobject, plot_dir)
        metadata = {'objectId': lasair_zobject.objectId, 'ramean': lasair_zobject.ramean, 'decmean': lasair_zobject.decmean }

        proto_subject = {}
        proto_subject['location_lc'] = light_curve
        proto_subject['location_ps'] = panstamps
        proto_subject['metadata'] = metadata
        
        return (proto_subject)

    def create_subjects_and_link_to_project(self, proto_subjects, project_id, workflow_id, subject_set_id):

        USERNAME = os.getenv('PANOPTES_USERNAME') 
        PASSWORD = os.getenv('PANOPTES_PASSWORD')  
        Panoptes.connect(username=USERNAME, password=PASSWORD, endpoint=self.ENDPOINT)

        project = Project.find(project_id)
        #workflow = Workflow.workflows.find(workflow_id)

        if subject_set_id == None:
            subject_set = SubjectSet()
            ts = time.gmtime()
            subject_set.display_name = time.strftime("%m-%d-%Y %H:%M:%S", ts) 
            subject_set.links.project = project
            subject_set.save()
        else:
            subject_set = SubjectSet().find(subject_set_id)
        subjects = []
        for proto_subject in proto_subjects:
            subject = Subject()
            subject.links.project = project
            subject.add_location(proto_subject['location_lc'])
            subject.add_location(proto_subject['location_ps'])
            subject.metadata.update(proto_subject['metadata'])
            subject.save()
            subjects.append(subject)

        subject_set.add(subjects)
        #workflow.add_subject_sets(subject_set)
        
        return True

    def  parse_object_data(self, objectId):
        f=open(objectId + '.json', "r")
        data = json.load(f)
        f.close()

        lo = lasair_object(objectId, 0,0,0,0)
   
        for key, value in data.items(): 
            if key == 'objectData':
                objectData = value
                for objData in objectData:
                    for dkey, dvalue in objectData.items():
                        if dkey == 'ramean':
                            lo.ramean = dvalue
                        elif dkey == 'decmean':
                            lo.decmean = dvalue   
            elif key == 'candidates':
                candidates = value           
                for candidate in candidates:
                    dml = ''
                    for ckey, cvalue in candidate.items():
                        if ckey == 'mjd':
                            mjd = cvalue
                        elif ckey == 'sigmapsf':
                            error = cvalue
                        elif ckey == 'fid':
                            if cvalue == 1:
                                fid = 'blue'
                                dmlcolor = 'lightskyblue'
                            if cvalue == 2:
                                fid = 'red'
                                dmlcolor = 'lightpink'
                        elif ckey == "magpsf":
                            mag = cvalue
                        elif ckey == "diffmaglim":
                            dml = cvalue
                    lo.Detections.append({'mjd':mjd, 'mag':mag, 'fid':fid, 'dml':dml, 'dmlcolor': dmlcolor, 'error':error})    
        return lo      

    def build_plots(self, lasair_object, plot_dir):    

        mjd = []
        mag = []
        fid = []
        error = []
        dml = []
        dmlcolor = []

        for i in range(len(lasair_object.Detections)):
            for ckey, cvalue in lasair_object.Detections[i].items():
                if ckey == 'mjd':
                    mjd.append(cvalue)
                elif ckey == 'error':
                    error.append(cvalue)
                elif ckey == 'fid':
                    fid.append(cvalue)
                elif ckey == "mag":
                    mag.append(cvalue)
                elif ckey == "dml":
                    if cvalue != '':
                        dml.append(cvalue)
                elif ckey == 'dmlcolor':
                    if cvalue != '':
                        dmlcolor.append(cvalue)        

        trace1 = go.Scatter(x=mjd, y=mag, marker={'color': fid,  'size': 15}, error_y= { 'type': 'data', 'array': error, 'visible': True }, mode="markers")
        trace2 = go.Scatter(x=mjd, y=dml, marker={'color': dmlcolor,  'size': 15}, mode="markers")
        data=go.Data([trace1, trace2])
        layout=go.Layout(title="Light curve for %s" % (lasair_object.objectId), xaxis={'title':'Mean Julian Date','tickformat': ',d'}, yaxis={'title':'Magnitude', 'autorange': 'reversed'})

        dirpath = os.path.join(plot_dir, time.strftime("%m-%d-%Y", time.gmtime()), lasair_object.objectId)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        figure=go.Figure(data=data,layout=layout)
        figure.write_image("%s\\light_curve.jpeg" % (dirpath), format='jpeg', width=800, height=600)
        
        #now get the panstamps image
        logger = logging.getLogger("Panstamps")
        fitsPaths, jpegPaths, colorPath = downloader(
                log=logger,
                settings=False,
                fits=False,
                jpeg=True,
                arcsecSize=500,
                filterSet='gri',
                color=True,
                singleFilters=True,
                ra=lasair_object.ramean,
                dec=lasair_object.decmean,
                imageType="stack",
                downloadDirectory=dirpath,
                mjdStart=False,
                mjdEnd=False,
                window=False
        ).get()

        light_curve = os.path.join(dirpath, "light_curve.jpeg")
        plots = dict({ 'light_curve': light_curve, 'panstamps': colorPath })
        return light_curve, colorPath[0]  
    
        
