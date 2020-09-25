import ujson
import caesar_external as ce
import sqlite3

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
  def __init__(self, config, caesar_config_name, db_path, workflow_id, N_cl_limit):
    self.config=config # CaesarConsumer config object
    self.caesar_config_name = caesar_config_name # this could be read from a CaesarConsumer config
    self.db_path = db_path
    self.workflow = workflow_id # required to retire subjects?
    #self.workflow = Workflow.find(workflow_id)

    self.subjects = {}
    self.last_classification_id = 0
    self.db_exists = False
    try:
      self.create_db()
    except sqlite3.OperationalError:
      print('Database exits.')
      self.db_exists = True
      self.load()

    self.N_cl_limit=N_cl_limit # number of classifications before retiring subjects

  # Check if db is empty? If all subjects are removed then the db exists but is empty

  def create_db(self):
    # initialise database at self.db_path
    # e.g. https://github.com/miclaraia/swap-2/blob/7d2ed2feb417c02ddd60e22f7977727d09de8ebb/swap/data/__init__.py#L30
    print('Build database')

    # Create database with several tables
    conn = sqlite3.connect(self.db_path)
    conn.execute('''CREATE TABLE cl_db (id, subject_id, T0, T1,processed)''') # table of all incoming classifications
    conn.execute('''CREATE TABLE sub_db (subject_id, T0_tally, T1_tally, N_cl, retire)''') # table of the subjects with running tally of T0 and T1 for each subject
    conn.close()

  def load(self):
    # load state from self.db_path
    # e.g. https://github.com/miclaraia/swap-2/blob/7d2ed2feb417c02ddd60e22f7977727d09de8ebb/swap/utils/control.py#L86
    print('Load from database')

    # connect to the existing db and update the last classification id
    conn = sqlite3.connect(self.db_path)
    ids=conn.execute("SELECT id FROM cl_db")
    # N.B. that the loaded db might be empty (e.g. if all subjects were retired)
    try:
        last_id=int(ids.fetchall()[-1][0])
    except:
        last_id=0
    print("the last id in the db is: {}".format(last_id))
    self.last_classification_id=last_id

  def save(self):
    # save state to self.db_path
    # e.g. https://github.com/miclaraia/swap-2/blob/7d2ed2feb417c02ddd60e22f7977727d09de8ebb/swap/utils/control.py#L113
    # print('Save to database')
    # This currently does nothing! More important if SWAP is implemented?
    return

  def recieve(self, ce):
    data = ce.Extractor.next()
    haveItems = False
    subject_batch = []

    for i, item in enumerate(data):
      haveItems = True
      print(item)
      id = int(item['id'])

      # Ensure that we skip over classifications that have already been processed
      # Assumes that ids are always increasing
      # if id <= self.last_classification_id:
      #   print("already processed")
      #   continue

      # Connect to the database and record the cl details
      conn = sqlite3.connect(self.db_path)
      c = conn.cursor()
      c.execute("SELECT COUNT(1) FROM cl_db where id={}".format(id)) # Insert a row of data
      cl_id_check=c.fetchone()[0]
      print("cl_id_check = {}".format(cl_id_check))
      conn.commit()

      if cl_id_check!=0:
          print("already processed")
          continue

      subject_id = int(item['subject'])
      annotation = item['annotations']
      # print("\nsubject_id: {}, annotation: {}\n".format(subject_id,annotation))
      cl = Classification(id, subject_id, annotation)
      self.process_classification(cl)
      self.last_classification_id = id # update the id
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

          # break # COMMENT THIS LINE OUT FOR LIVE RUNS add an exit to stop infinite loop through the one classifaction file! (normally there would a continuous stream)

    except KeyboardInterrupt as e:
      print('Received KeyboardInterrupt {}'.format(e))
      self.save()
      # print('Terminating SWAP instance.') # we are not doing SWAP right now

  def process_classification(self, cl):

    id=cl.id
    subject_id=cl.subject_id
    T0_val=cl.label['T0']

    print("process cl: {}".format(id))

    # T0 should always have an answer, otherwise there must have been an error
    # Throw away for now, but possibly move this so that the dodgy cl is recorded in the db?
    if T0_val=='None':
        print("Classification has T0='None', there should be an answer -> Skip!")
        return

    # T1 is not always answered
    try:
        T1_val=cl.label['T1']
    except:
        T1_val="None"

    # Some of the cls have "None" as a value. *** Count these as zero in the tally (or discount them?) **
    # N.B. how does counting None as zero affect the N_cl count?
    T_tally=[]
    for T_val in [T0_val,T1_val]:
        if T_val=="None":
            T_tally.append(0)
        else:
            T_tally.append(T_val)

    process=0 # flag to check which cls have been processed into the sub_db table

    # Connect to the database and record the cl details
    conn = sqlite3.connect(self.db_path)
    c = conn.cursor()
    c.execute("INSERT INTO cl_db VALUES (?,?,?,?,?)",(id,subject_id,T0_val,T1_val,process)) # Insert a row of data
    conn.commit()

    # Check if this subject already exists in sub_db
    c.execute("SELECT COUNT(1) FROM sub_db WHERE subject_id=(?)",(subject_id,))
    subject_check=c.fetchone()[0]

    if subject_check==1: # the subject is already in sub_db
        # check if we have already retired the subject
        c.execute("SELECT retire FROM sub_db WHERE subject_id=(?)",(subject_id,))
        retire_check=c.fetchone()[0]
        if retire_check==1:
            print("Subject {} has already been retired, skip cl {}".format(subject_id,id))
            conn.close()
            return
        # subject exists and is not retired, update values
        print("Update subject {}".format(subject_id))
        c.execute("UPDATE sub_db SET T0_tally = T0_tally + ?, T1_tally = T1_tally + ?, N_cl = N_cl + 1 WHERE subject_id = ?",(T_tally[0],T_tally[1],subject_id))
        conn.commit()

    elif subject_check==0: # the subject is not in sub_db
        # add the new subject to sub_db table
        # initialise with the T0/T1 results of this first classification and start the counts at 1
        print("New subject {}".format(subject_id))
        N_cl=1 # count number of classifications
        retire=0 # flag to decide what to retire (0 = keep, 1 = retire)

        c.execute("INSERT INTO sub_db VALUES (?,?,?,?,?)",(subject_id,T_tally[0],T_tally[1],N_cl,retire))
        conn.commit()

    else: # catch weird error if subject_check does not equal 1 or 0
        raise ValueError("subject_check does not equal 1 or 0, error in subject table?") # if subject_check>1 we might have multiple entries for a subject?

    # mark the classification as processed
    c.execute("UPDATE cl_db SET processed = 1 WHERE subject_id=(?)",(subject_id,))
    conn.commit()

    conn.close()
    return


  def send(self, subject_batch):
    to_retire = self.retire(subject_batch) #return list of subjects to retire (could be emtpy list/list of one or more)
    print("subjects to retire = {}".format(to_retire))
    if to_retire != []:

      # Perform analysis here: classify (SLSN yes or no?) using simple vote fraction
      self.send_panoptes(to_retire) # tell panoptes which subjects to retire
      self.send_lasair(to_retire) # post back to Lasair

      # # remove retired subjects from SQL table(s) here?
      # conn = sqlite3.connect(self.db_path)
      # c = conn.cursor()
      # for tab in ["sub_db","cl_db"]: # If we remove from both tables then the db might still exist but be empty. Pass list of tables as a property of the db class?
      #     c.execute("DELETE FROM {} WHERE subject_id IN {}".format(tab,tuple(to_retire)))
      #     conn.commit()
      # conn.close()
      # # INSTEAD OF DELETING WE COULD ADD A "PROCESSED" FLAG TO cl_db? do this in "retire" method


  def retire(self, subject_batch):
    #check what needs retired (e.g. number of classifications=10 for T0)
    # use the N_cl field in the sub_db table
    #flag retired in subject db
    to_retire = []

    conn = sqlite3.connect(self.db_path)
    c = conn.cursor()

    # find all subjects in this batch that have been classified N_cl_limit times
    if len(subject_batch)==1:
        c.execute("SELECT subject_id FROM sub_db WHERE subject_id={} AND N_cl >= {}".format(subject_batch[0],self.N_cl_limit))
    else:
        c.execute("SELECT subject_id FROM sub_db WHERE subject_id IN {} AND N_cl >= {}".format(tuple(subject_batch),self.N_cl_limit))

    # read the retire query
    for row in c:
        to_retire.append(row[0])

    # update retire flag for these subjects
    if len(to_retire)==1:
        c.execute("UPDATE sub_db SET retire = 1 WHERE subject_id={}".format(to_retire[0]))
    else:
        c.execute("UPDATE sub_db SET retire = 1 WHERE subject_id IN {}".format(tuple(to_retire)))
    conn.commit()

    conn.close()
    return to_retire

  def send_panoptes(self, to_retire):
    # Tell panoptes which subjects to retire
    # UNCOMMENT/ADD CODE HERE
    # retire = [PanoptesSubject().find(sid) for sid in to_retire]
    # self.workflow.retire_subjects(retire)
    return

  def send_lasair(self, to_retire):
    # accepts list of subject_ids to retire, need to associate this list with a lasair object id
    print("Post subject classifications back to lasair: {}".format(to_retire))
    conn = sqlite3.connect(self.db_path)
    c = conn.cursor()
    for sid in to_retire:
        # for each subject_id
        c.execute("SELECT T0_tally FROM sub_db WHERE subject_id=(?)",(sid,))
        T0_tally=c.fetchone()[0]
        # print(T0_tally)
        c.execute("SELECT T1_tally FROM sub_db WHERE subject_id=(?)",(sid,))
        T1_tally=c.fetchone()[0]
        # print(T1_tally)
        c.execute("SELECT N_cl FROM sub_db WHERE subject_id=(?)",(sid,))
        N_cl=c.fetchone()[0]
        # print(N_cl)

        # calculate the results for T0 and T1 as a fraction
        T0_result=float(T0_tally)/float(N_cl)
        T1_result=float(T1_tally)/float(N_cl)
        print("subject_id={}, T0_result={}, T1_result={}".format(sid,T0_result,T1_result))

        # ADD CODE HERE:
        # match subject_id to LasairURL (use panoptes to retrieve the subject meta-data?) and pass the T0_result and T1_result

    conn.close()
    return

# db_path="example.db"
# db_path="live_test.db"
# db_path="live_test_18_07_2020.db"
db_path="live_test_05_08_2020.db"
consumer = CaesarConsumer(config=None, caesar_config_name='slsn_online', db_path=db_path, workflow_id=None, N_cl_limit=3)
consumer.consume()

# Read the classifications in the db file
conn = sqlite3.connect(db_path)

# use the sqlite way
c = conn.cursor()
print("\nclassification db:")
c.execute("SELECT * FROM cl_db")
names = [description[0] for description in c.description]
print(names)
for row in c:
    print(row)
print("\nsubject db:")
c.execute("SELECT * FROM sub_db")
names = [description[0] for description in c.description]
print(names)
for row in c:
    print(row)

# # use the pandas way
# import pandas as pd
# df_cl=pd.read_sql("SELECT * FROM cl_db",conn)
# print(df_cl)
#
# df_sub=pd.read_sql("SELECT * FROM sub_db",conn)
# print(df_sub)

# N.B. that the sub_db table can always be recalculated, e.g. if N_cl_limit is changed

conn.close()
