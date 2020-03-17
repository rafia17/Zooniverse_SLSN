import caesar_external as ce

class Classification(object):
  def __init__(self,
               id,
               user_id,
               subject_id,
               object_id,
               annotation):
      
    self.id = int(id)
    try:
      self.user_id = int(user_id)
    except ValueError:
      self.user_id = user_id
    self.subject_id = int(subject_id)
    self.object_id = object_id # ZTF object id
    self.label = self.parse(annotation)

  def parse(self, annotation):
    # convert annotations to hard label assignment i.e. 0=Not SLSNe or 1 = SLSNe
    raise NotImplementedError

class CaesarConsumerConfig(object):
  # example CaesarConsumerConfig object
  def __init__(self):
    raise NotImplementedError

class CaesarConsumer(object):
  def __init__(self, config, caesar_config_name, db_path):
    self.config=config # CaesarConsumer config object
    self.caesar_config_name = caesar_config_name # this could be read from a CaesarConsumer config
    self.db_path = db_path
    
    self.last_classification_id = 0
    self.db_exists = False
    try:
      self.create_db()
    except sqlite3.OperationalError:
      print('Database exits.')
      self.db_exists = True

  def create_db(self):
    # initialise database at self.db_path
    # e.g. https://github.com/miclaraia/swap-2/blob/7d2ed2feb417c02ddd60e22f7977727d09de8ebb/swap/data/__init__.py#L30
    raise NotImplementedError
  
  def load(self):
    # load state from self.db_path
    # e.g. https://github.com/miclaraia/swap-2/blob/7d2ed2feb417c02ddd60e22f7977727d09de8ebb/swap/utils/control.py#L86
    raise NotImplementedError
  
  def save(self):
    # save state to self.db_path
    # e.g. https://github.com/miclaraia/swap-2/blob/7d2ed2feb417c02ddd60e22f7977727d09de8ebb/swap/utils/control.py#L113
    raise NotImplementedError
  
  def recieve(self, ce):
    data = ce.Extractor.next()
    haveItems = False
    subject_batch = []
    for i, item in enumerate(data):
      haveItems = True
      id = int(item['classification_id'])
      if id < self.last_classification_id:
        continue
      try:
        user_id = int(item['user_id'])
      except ValueError:
        user_id = item['user_id']
      subject_id = int(item['subject_ids'])
      object_id = item['object_id']
      annotation = ujson.loads(item['annotations'])
      cl = Classification(id, user_id, subject_id, object_id, annotation)
      self.process_classification(cl)
      self.last_id = id
      subject_batch.append(subject_id)
    return haveItems, subject_batch

  def consumer(self):
    ce.Config.load(caesar_config_name)
    try:
      while True:
        haveItems, subject_batch = self.recieve(ce)
        print(haveItems, subject_batch)
        if haveItems:
          self.save()
          ce.Config.instance().save()
          # load the just saved ce config
          ce.Config.load(caesar_config_name)
          self.send(ce, subject_batch)
    except KeyboardInterrupt as e:
      print('Received KeyboardInterrupt {}'.format(e))
      self.save()
      print('Terminating SWAP instance.')
      exit()

  def process_classification(self, cl):
  
  def send(self, subject_batch)
    to_retire = self.retire(subject_batch)
    if to_retire != []:
      self.send_panoptes(to_retire)
      self.send_lasair(to_retire)

  def retire(self, subject_batch):
