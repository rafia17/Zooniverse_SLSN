from panoptes_client import Panoptes, Project, SubjectSet, Subject, User, ProjectPreferences
import numpy as np
import pandas as pd

def create_proto_subjects(subject_data_csv,file_path,file_header="filename"):
    '''
    Create a dictionary of the subject filename (for the image or other data file) and the subject metadata.
    The filenames are under file_header in the csv and are found at location file_path.
    Add remote file capabilities to file_path?

    Keyword Arguments:
    subject_data_csv -- the csv containing subject filename and metadata columns
    file_path -- location of the subject files
    file_header -- the csv column containing the subject filenames (all other columns are metadata)
    '''

    # load the csv containing filenames and metadata
    df_subjects=pd.read_csv(subject_data_csv)
    csv_headers=np.array(list(df_subjects))
    meta_headers=csv_headers[~(csv_headers==file_header)] # retrieve the headers of only the metadata

    # get the list of file names from the csv
    files=df_subjects[file_header]

    # generate the metadata dict from all other columns
    metadata=df_subjects[meta_headers].to_dict(orient='records')
    print(metadata)

    # create the subject data: metadata dictionary from the csv file info
    proto_subjects={}
    for i in range(len(files)):
        proto_subjects['{}/{}'.format(file_path,files[i])]={} # create the dict key
        proto_subjects['{}/{}'.format(file_path,files[i])].update(metadata[i]) # add the nested dict metadata

    return proto_subjects


def create_subjects_and_link_to_project(proto_subjects, project_id, subject_set_id):
    ''' find the project and relevant subject set. Get the existing subject data and compare to the new proto_subjects.
    Upload any instances of nbew subjects to the project

    Keyword Arguments:
    proto_subjects -- dictionary structure containing subject filepath+filename, and associated metadata
    project_id -- identifier to find and link with the project
    subject_set_id -- identifier for the subject set of interest
    '''

    # get the project object
    project = Project.find(project_id)

    # set up subject_set
    if subject_set_id == None:
        subject_set = SubjectSet() # create empty subject_set
        subject_set.links.project = project
        subject_set.display_name = 'new subject set'
        subject_set.save()
        project.reload()
    else:
        subject_set = SubjectSet().find(subject_set_id) # find the existing subject_set

    # Create a list of the existing subject metadata
    meta_list=[]
    print("existing subjects:")
    for subject in subject_set.subjects:
        print(subject.id,subject.metadata)
        meta_list.append(subject.metadata)

    # When making list of subjects to add, check to see if the metadata of the subject you want to add is already in the set
    print("new subjects:")
    new_subjects = []
    for filename, metadata in proto_subjects.items():

        # check if this subject is already in the subject set
        if np.isin(metadata,meta_list):
            print("{}, subject already in set".format(metadata))
            # In this case we skip over the subject that already exists.
            # N.B. you may want to remove an existing subject and update it with the new one
            continue

        # Otherwise we can add the subject to the new subject list
        else:
            subject = Subject()

            subject.links.project = project
            subject.add_location(filename)

            subject.metadata.update(metadata)

            subject.save()
            new_subjects.append(subject)
            print("{}, new subject add to list".format(metadata))

    print("new subjects to add: {}".format(new_subjects))

    # add the new subject list (data and metadata) to the already defined project subject set
    # subject_set.add(new_subjects)

    return

################

# Connect to zooniverse account, insert your username and password
# Panoptes.connect(username='user', password='password')
# Or put the user details in a separate file for anonymity
f=open("user_details.txt","r")
user_dat=[d.strip() for d in f.readlines()]
Panoptes.connect(username=user_dat[0], password=user_dat[1])

# add the project id to access all project/subject details
project_id="10997"
# retrieve the user/project details
user = User.find(project_id)
project = Project.find(project_id)

# # look at any project workflows
# for workflow in project.links.workflows:
#     print(workflow.display_name,workflow.id)

# retrieve the subject set id
print(project.links.subject_sets)
subject_set_id=project.links.subject_sets[-1].id # this is the first subject set (if there are multiple?)
print("subject set id = {}".format(subject_set_id))

# Add some new subjects to the list
subject_data_csv="tutorial_project_subjects.csv"
df_subjects=pd.read_csv(subject_data_csv)

# print(df_subjects)

# define the subject file data
file_path="/Users/jrobinson/xq1_grav_cloud/binary_stability/orbit_results/orbit_results_plots/all_plots_fixed/main_analysis/binary_nodes"

proto_subjects=create_proto_subjects(subject_data_csv=subject_data_csv,file_header="filename",file_path=file_path)

# print(proto_subjects)

create_subjects_and_link_to_project(proto_subjects=proto_subjects, project_id=project_id, subject_set_id=subject_set_id)
