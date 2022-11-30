import os
import time
import json
import wget
import lasair_consumer
import logging
import matplotlib

from lasair_consumer import msgConsumer
from PIL import Image, ImageDraw
from lasair_zooniverse_base import lasair_zooniverse_base_class

# 3rd party imports
import matplotlib.pyplot as plt
import numpy as np
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

    def __str__(self):
      print (self.objectId, self.ramean, self.decmean)


def get_objectId(msg):
    msgString = msg.decode("utf-8")
    msgDict = json.loads(msgString)
    objectname = msgDict['objectId']
    return objectname                


def handle_object(objectId, L):
    # from the objectId, we can get all the info that Lasair has
    objectInfo = L.objects([objectId])[0]
    if not objectInfo:
        return None

    #print(json.dumps(objectInfo['image_urls'], indent=2))
    return objectInfo


class lasair_zooniverse_class(lasair_zooniverse_base_class):

    def __init__(self, kafka_server, ENDPOINT):
        self.kafka_server = kafka_server
        self.ENDPOINT = ENDPOINT
        self.log = logging.getLogger("lasair-zooniverse-logger")


    def query_lasair_topic(self, group_id, topic):

        c = msgConsumer(self.kafka_server, group_id)
        c.subscribe(topic)
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

        #remove duplicates from the list
        objectIds = list(set(objectIds))

        return objectIds

    def wget_objectdata(self, objectId, url, data_dir):
        try:
            url = url % (objectId)
            dirpath = os.path.join(data_dir, time.strftime("%m-%d-%Y", time.gmtime()))
            if not os.path.exists(dirpath):
                os.makedirs(dirpath)
            print(dirpath)
            wget.download(url, os.path.join(dirpath, objectId + '.json'))
        except Exception:
            self.log.exception("Error in wget for object: " + objectId)

    # 2022-11-29 KWS Added API equivalent of wget_objectdata above
    def get_objectdata_via_api(self, objectId, data_dir, L):
        # Initially let's do this as before and write the JSON to disk.
        try:
            dirpath = os.path.join(data_dir, time.strftime("%m-%d-%Y", time.gmtime()))
            if not os.path.exists(dirpath):
                os.makedirs(dirpath)
            print(dirpath)
            obj = handle_object(objectId, L)
            with open(dirpath + '/' + objectId + '.json', 'w') as fp:
                json.dump(obj, fp)

        except Exception as e:
            self.log.exception("Error in API get for object: " + objectId)



    def produce_proto_subject(self, unique_id, data_dir):
        # produce plots and gather metadata for each subject to be created
        lasair_zobject = self.parse_object_data(unique_id, data_dir)
        if(lasair_zobject != None):
            try:
                light_curve, panstamps = self.build_plots(lasair_zobject, data_dir)
                metadata = {'objectId': lasair_zobject.objectId, 'ramean': lasair_zobject.ramean, 'decmean': lasair_zobject.decmean }

                proto_subject = {}
                proto_subject['location_lc'] = light_curve
                proto_subject['location_ps'] = panstamps
                proto_subject['metadata'] = metadata

                return (proto_subject)
            except Exception:
                self.log.exception("Error in produce_proto_subject for object: " + unique_id)
        return None

    def create_subjects_and_link_to_project(self, proto_subjects, project_id, workflow_id, subject_set_id):

        
        try:
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
        except Exception:
            self.log.exception("Error in create_subjects_and_link_to_project ")
        

    def parse_object_data(self, objectId, data_dir):
        try:
            dirpath = os.path.join(data_dir, time.strftime("%m-%d-%Y", time.gmtime()))
            f=open(os.path.join(dirpath, objectId + '.json'), "r")
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
                    print(len(candidates))
                    for candidate in candidates:
                        mjd = candidate['mjd']
                        fid = candidate['fid']
                        mag = candidate['magpsf']
                        try:
                          error = candidate['sigmapsf']
                          flag = True
                        except KeyError:
                          mag = candidate['diffmaglim']
                          flag = False
                        lo.Detections.append({'mjd':mjd, 'mag':mag, 'fid':fid, 'error':error, 'detect_flag': flag})
            return lo
        except Exception as e:
            print(repr(e))
            return None
    
    def gather_metadata(self, ramean, decmean, dirpath):
        #logger = logging.getLogger("Panstamps")
        fitsPaths, jpegPaths, colorPath = downloader(
                log=logging.getLogger(__name__),
                settings=False,
                fits=False,
                jpeg=True,
                arcsecSize=75,
                filterSet='gri',
                color=True,
                singleFilters=False,
                ra=ramean,
                dec=decmean,
                imageType="stack",
                downloadDirectory=dirpath,
                mjdStart=False,
                mjdEnd=False,
                window=False
        ).get()

        return colorPath
    

    def build_plots(self, lasair_object, data_dir):

        mjd_red = []
        mag_red = []
        yerr_red = []
        mjd_red_limit = []
        mag_red_limit = []
        mjd_blue = []
        mag_blue = []
        yerr_blue = []
        mjd_blue_limit = []
        mag_blue_limit = []
    
        mjd_first = np.inf
        print(len(lasair_object.Detections))
        for detection in lasair_object.Detections:
          print(detection)
      
          if detection['detect_flag']  == True:
            if detection['mjd'] < mjd_first:
              mjd_first = detection['mjd']
            if detection['fid'] == 2:
              mjd_red.append(detection['mjd'])
              mag_red.append(detection['mag'])
              yerr_red.append(detection['error'])
            elif detection['fid'] == 1:
              mjd_blue.append(detection['mjd'])
              mag_blue.append(detection['mag'])
              yerr_blue.append(detection['error'])
          elif detection['detect_flag'] == False:
            if detection['fid'] == 2:
              mjd_red_limit.append(detection['mjd'])
              mag_red_limit.append(detection['mag'])
            elif detection['fid'] == 1:
              mjd_blue_limit.append(detection['mjd'])
              mag_blue_limit.append(detection['mag'])

        dirpath = os.path.join(data_dir, time.strftime("%m-%d-%Y", time.gmtime()))

        font = {'family' : 'normal',
                'size'   : 22}

        matplotlib.rc('font', **font)

        fig = plt.figure(figsize=(12,9))
        ax = fig.add_subplot(111)

        ax.errorbar(np.array(mjd_red) - mjd_first,
                    mag_red,
                    yerr=yerr_red,
                    marker='o',
                    markersize=10,
                    color='#D1495B',
                    ls='none')

        ax.errorbar(np.array(mjd_blue) - mjd_first,
                    mag_blue,
                    yerr=yerr_blue,
                    marker='D',
                    markersize=10,
                    color='#26547C',
                    ls='none')

        ax.scatter(np.array(mjd_red_limit) - mjd_first,
                   mag_red_limit,
                   marker='v',
                   s=100,
                   color='#DE7C89')

        ax.scatter(np.array(mjd_blue_limit) - mjd_first,
                   mag_blue_limit,
                   marker='v',
                   s=100,
                   color='#4489C5')

        ax.set_xlabel('Days since First Detection')
        ax.set_ylabel('Difference Magnitude')

        plt.grid()
        plt.gca().invert_yaxis()
        plt.savefig(os.path.join(dirpath, "%s_light_curve.jpeg"%(lasair_object.objectId)))


        #now get the panstamps image
        colorPath = self.gather_metadata(lasair_object.ramean,lasair_object.decmean, dirpath)

        #put crosshairs on the panstamps image
        self.draw_crosshairs(lasair_object.ramean,lasair_object.decmean, colorPath)

        light_curve = os.path.join(dirpath, "%s_light_curve.jpeg"%(lasair_object.objectId))
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
        draw.line(l, fill='#00ff00', width=lineWidth)
        for l in lines:
            draw.line(l, fill='#00ff00', width=lineWidth)

        del draw

        im.thumbnail((300, 300), Image.ANTIALIAS)
        im.save(colorPath[0], "JPEG")
