import ujson
import caesar_external as ce

class Classification(object):
  def __init__(self,
               id,
               subject_id,
               annotation):
      
    self.id = int(id)
    self.subject_id = int(subject_id)
    self.label = self.parse(annotation)

  def parse(self, annotation):
    label = {'T0': annotation['T0'][0]['value']}
    try:
      label['T1'] = annotation['T1'][0]['value']
    except KeyError:
      return label
    return label

class CaesarConsumerConfig(object):
  # example CaesarConsumerConfig object
  def __init__(self):
    raise NotImplementedError

class CaesarConsumer(object):
  def __init__(self, config, caesar_config_name, db_path):
    self.config=config # CaesarConsumer config object
    self.caesar_config_name = caesar_config_name # this could be read from a CaesarConsumer config
    self.db_path = db_path
    
    self.subjects = {}
    self.last_classification_id = 0
    self.db_exists = False
    try:
      self.create_db()
    except sqlite3.OperationalError:
      print('Database exits.')
      self.db_exists = True
      self.load()

  def create_db(self):
    # initialise database at self.db_path
    # e.g. https://github.com/miclaraia/swap-2/blob/7d2ed2feb417c02ddd60e22f7977727d09de8ebb/swap/data/__init__.py#L30
    #raise NotImplementedError
    print('Build database')
  
  def load(self):
    # load state from self.db_path
    # e.g. https://github.com/miclaraia/swap-2/blob/7d2ed2feb417c02ddd60e22f7977727d09de8ebb/swap/utils/control.py#L86
    #raise NotImplementedError
    print('Load from database')
  
  def save(self):
    # save state to self.db_path
    # e.g. https://github.com/miclaraia/swap-2/blob/7d2ed2feb417c02ddd60e22f7977727d09de8ebb/swap/utils/control.py#L113
    #raise NotImplementedError
    print('Save to database')
  
  def recieve(self, ce):
    data = ce.Extractor.next()
    haveItems = False
    subject_batch = []
    for i, item in enumerate(data):
      haveItems = True
      print(item)
      id = int(item['id'])
      if id < self.last_classification_id:
        continue
      subject_id = int(item['subject'])
      annotation = item['annotations']
      cl = Classification(id, subject_id, annotation)
      self.process_classification(cl)
      self.last_id = id
      subject_batch.append(subject_id)
    return haveItems, subject_batch

  def consume(self):
    ce.Config.load(self.caesar_config_name)
    try:
      while True:
        haveItems, subject_batch = self.recieve(ce)
        print(haveItems, subject_batch)
        if haveItems:
          self.save()
          ce.Config.instance().save()
          # load the just saved ce config
          ce.Config.load(self.caesar_config_name)
          self.send(subject_batch)
    except KeyboardInterrupt as e:
      print('Received KeyboardInterrupt {}'.format(e))
      self.save()
      print('Terminating SWAP instance.')
      exit()

  def process_classification(self, cl):
    print(cl)
  
  def send(self, subject_batch):
    to_retire = self.retire(subject_batch)
    if to_retire != []:
      self.send_panoptes(to_retire)
      self.send_lasair(to_retire)

  def retire(self, subject_batch):
    pass

  def send_panoptes(self, to_retire):
    pass

  def send_lasair(self, to_retire):
    pass

consumer = CaesarConsumer(None, 'slsn_online', None)
consumer.consume()
