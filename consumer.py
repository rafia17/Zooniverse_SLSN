from confluent_kafka import Consumer, KafkaError, Message
import time
import random
import time
import json
import wget
from astropy.time import Time

import plotly.graph_objs as go

class Detection:
    
  def __init__(self, mjd, mag, fid):
    self.mjd = mjd
    self.mag = mag
    self.fid = fid


class msgConsumer():
    def __init__(self, kafka_server, group_id):
        self.group_id = group_id

        conf = {
            'bootstrap.servers': kafka_server,
            'group.id': self.group_id,
            'default.topic.config': {'auto.offset.reset': 'smallest'}
        }
        self.streamReader = Consumer(conf)

    def topics(self):
        t = self.streamReader.list_topics()
        t = t.topics
        t = t.keys()
        t = list(t)
        return t

    def subscribe(self, topic):
        self.streamReader.subscribe([topic])

    def poll(self):
        try:
            msg = self.streamReader.poll(timeout=30)
            return msg.value()
        except:
            return None

    def close(self):
        self.streamReader.close()

# this method parses the msg from Kafka stream for each objectId 
# and gets all the individual detections for the objectId and saves it in a json file
def get_objectData(msg):

        msgString= msg.decode("utf-8")
        step_0 = msgString.split(',')
        for pair in step_0:
            step_1 = pair.split(':')
            if step_1[0][2:9] =='objectI':
                objectname = step_1[1].replace('"', '')
                objectname = objectname.replace(' ', '')

        url = "https://lasair-dev.roe.ac.uk/object/%s/json/" % (objectname)
        wget.download(url, objectname + '.json')
        return objectname
    
#this function opens a json file and reads candidate data and make a light curve from it
def make_scatter_plot(objectId):

    f=open(objectId + '.json', "r")
    data = json.load(f)
    f.close()

    mjd = []
    mag = []
    fid = []
    error = []
    dml = []
    dmlcolor = []
   
    for key, value in data.items(): 
        if key == 'ramean':
            ramean = value
        if key == 'decmean':
            decmean = value   
        if key == 'candidates':
            candidates = value           
            for candidate in candidates:
                for ckey, cvalue in candidate.items():
                    if ckey == 'mjd':
                        mjd.append(cvalue)
                    elif ckey == 'sigmapsf':
                        error.append(cvalue)
                    elif ckey == 'fid':
                        if cvalue == 1:
                            fid.append('blue')
                            dmlcolor.append('lightskyblue')
                        if cvalue == 2:
                            fid.append('red')
                            dmlcolor.append('lightpink')
                    elif ckey == "magpsf":
                        mag.append(cvalue)
                    elif ckey == "diffmaglim":
                        dml.append(cvalue)
                
                
    mjd = [i for i in mjd if i != 0]
    mag = [i for i in mag if i != 0]

    trace1 = go.Scatter(x=mjd, y=mag, marker={'color': fid,  'size': 15}, error_y= { 'type': 'data', 'array': error, 'visible': True },
                    mode="markers",  text=["one","two","three"], name='1st Trace')

    trace2 = go.Scatter(x=mjd, y=dml, marker={'color': dmlcolor,  'size': 15},
                    mode="markers",  text=["one","two","three"], name='1st Trace')
    data=go.Data([trace1, trace2])
    layout=go.Layout(title="Light curve for %s" % (objectId), xaxis={'title':'Mean Julian Date'}, yaxis={'title':'Magnitude', 'autorange': 'reversed'})

    figure=go.Figure(data=data,layout=layout)
    figure.write_html('first_figure.html', auto_open=True)  

################
import sys
import os


kafka_server = 'lasair-dev.roe.ac.uk:9092'
topic = '2SN-likethings'
group_id = 'LASAIR5'

c = msgConsumer(kafka_server, group_id)
c.subscribe(topic)
print('Topics are ', c.topics())
start = time.time()

#while 1:

i=0
while i<10:
        msg = c.poll()
        print(msg)
        objectId = get_objectData(msg)
        make_scatter_plot(objectId)
        if not msg:
            break
        i = i+1
print('======= %.1f seconds =========' % ((time.time()-start)))

print('done')
