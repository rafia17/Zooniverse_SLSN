import os
import time
import json
import wget
import lasair_consumer
from lasair_consumer import msgConsumer
from PIL import Image, ImageDraw

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
        self.Detections = []


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
                objectname = get_objectId(msg)
                objectIds.append(objectname)
        print('======= %.1f seconds =========' % ((time.time()-start)))
        print('poll done')
        return objectIds

    def wget_objectdata(self, objectId, url):
        url = url % (objectId)
        wget.download(url, objectId + '.json')


    def produce_proto_subject(self, unique_id, plot_dir):
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
        workflow = Workflow().find(workflow_id)

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
        workflow.add_subject_sets(subject_set)
        
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
                    mjd_dml = ''
                    dml = ''
                    dmlcolor = ''
                    mag = ''
                    flag = False
                    for ckey, cvalue in candidate.items():
                        if ckey == 'mjd':
                            mjd = cvalue
                        elif ckey == 'sigmapsf':
                            error = cvalue
                        elif ckey == 'fid':
                            if cvalue == 1:
                                fid = 'blue'
                            if cvalue == 2:
                                fid = 'red'
                        elif ckey == "magpsf":
                            if flag == False:
                                mag = cvalue
                        elif ckey == "diffmaglim":
                            dml = cvalue
                            mjd_dml = mjd
                            mjd = ''
                            error = ''
                            mag = ''
                            flag = True
                            if fid == 'blue':
                                dmlcolor = 'lightskyblue'
                            elif fid == 'red':
                                dmlcolor = 'lightpink'
                            fid = ''
                    lo.Detections.append({'mjd':mjd, 'mag':mag, 'fid':fid, 'mjd_dml':mjd_dml, 'dml':dml, 'dmlcolor': dmlcolor, 'error':error})  
        return lo      
    
    def gather_metadata(self, ramean, decmean, dirpath):
        logger = logging.getLogger("Panstamps")
        fitsPaths, jpegPaths, colorPath = downloader(
                log=logger,
                settings=False,
                fits=False,
                jpeg=True,
                arcsecSize=75,
                filterSet='gri',
                color=True,
                singleFilters=True,
                ra=ramean,
                dec=decmean,
                imageType="stack",
                downloadDirectory=dirpath,
                mjdStart=False,
                mjdEnd=False,
                window=False
        ).get()

        return colorPath    
    

    def build_plots(self, lasair_object, plot_dir):    

        mjd = []
        mag = []
        fid = []
        error = []
        mjd_dml = []
        dml = []
        dmlcolor = []

        for i in range(len(lasair_object.Detections)):
            for ckey, cvalue in lasair_object.Detections[i].items():
                if ckey == 'mjd':
                    if cvalue != '':
                        mjd.append(cvalue)
                elif ckey == 'error':
                    if cvalue != '':
                        error.append(cvalue)
                elif ckey == 'fid':
                    if cvalue != '':
                        fid.append(cvalue)
                elif ckey == "mag":
                    if cvalue != '':
                        mag.append(cvalue)
                elif ckey == "mjd_dml":
                    if cvalue != '':
                        mjd_dml.append(cvalue)
                elif ckey == "dml":
                    if cvalue != '':
                        dml.append(cvalue)
                elif ckey == 'dmlcolor':
                    if cvalue != '':
                        dmlcolor.append(cvalue)        

        
        trace1 = go.Scatter(x=mjd, y=mag, marker={'color': fid,  'size': 10}, error_y= { 'type': 'data', 'array': error, 'visible': True, 'thickness': 1}, mode="markers")
        trace2 = go.Scatter(x=mjd_dml, y=dml, marker={'color': dmlcolor,  'size': 8}, mode="markers")

        data=go.Data([trace1, trace2])
        layout=go.Layout(xaxis={'title':'Modified Julian Date','tickformat': 'd'}, yaxis={'title':'Difference Magnitude', 'autorange': 'reversed'}, showlegend= False)
        dirpath = os.path.join(plot_dir, time.strftime("%m-%d-%Y", time.gmtime()), lasair_object.objectId)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        figure=go.Figure(data=data,layout=layout)
        figure.write_image("%s\\light_curve.jpeg" % (dirpath), format='jpeg', width=800, height=600)
        #figure.write_html("first_figure.html",  auto_open=True)

        #now get the panstamps image
        colorPath = self.gather_metadata(lasair_object.ramean,lasair_object.decmean, dirpath)

        #put crosshairs on the panstamps image
        self.draw_crosshairs(lasair_object.ramean,lasair_object.decmean, colorPath)

        light_curve = os.path.join(dirpath, "light_curve.jpeg")
        plots = dict({ 'light_curve': light_curve, 'panstamps': colorPath })
        return light_curve, colorPath[0]  
    
    def draw_crosshairs(self, ramean, decmean, colorPath):
        im = Image.open(colorPath[0], mode='r')

        # DETERMINE THE SIZE OF THE IMAGE
        imWidth, imHeight = im.size

        # THE CROSS HAIRS SHOULD BE 1/6 THE LENGTH OF THE SMALLEST DIMENSON
        chLen = int(min(imWidth, imHeight) / 6)

        # THE GAP IN THE CENTRE SHOULD BE 1/60 OF THE LENGTH OF THE SMALLEST DIMENSON
        gapLen = int(min(imWidth, imHeight) / 60)

        
        # LINE WIDTH SHOULD BE EASILY VIEWABLE AT ALL SIZES - 0.2% OF THE WIDTH SEEMS GOOD
        # SEEMS FINE
        lineWidth = int(max(imWidth, imHeight) / 300)

        lines = []
        l = (imWidth / 2 - gapLen - chLen, imHeight /
            2, imWidth / 2 - gapLen, imHeight / 2)
        lines.append(l)
        l = (imWidth / 2 + gapLen, imHeight /
            2, imWidth / 2 + gapLen + chLen, imHeight / 2)
        lines.append(l)
        l = (imWidth / 2, imHeight /
            2 - gapLen - chLen, imWidth / 2, imHeight / 2 - gapLen)
        lines.append(l)
        l = (imWidth / 2, imHeight /
            2 + gapLen, imWidth / 2, imHeight / 2 + gapLen + chLen)
        lines.append(l)


        # GENERATE THE DRAW OBJECT AND DRAW THE CROSSHAIRS
        draw = ImageDraw.Draw(im)
        draw.line(l, fill=128, width=lineWidth)
        for l in lines:
            draw.line(l, fill=128, width=lineWidth)

        del draw

        im.save(colorPath[0], "JPEG")