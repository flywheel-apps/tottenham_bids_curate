# -*- coding: utf-8 -*-
"""Copy of tottenham_lab_BIDS_notebook.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1laJ6P-WHGlnK58SWepZDkDUybbpW36Fd
"""

#@title Set API Key

#@markdown 1. Run this cell
#@markdown 2. Enter the api key
#@markdown 3. Hit enter
#@markdown 3. Run the other cells
#@markdown 5. __Remember to `Runtime > Reset all runtimes...` when done with notebook__
import os
from getpass import getpass
api_key = os.environ['CUMC_API']

#@title Install Flywheel SDK package
#@markdown This cell needs to run in order to use the Flywheel SDK
flywheel_SDK_version = '14.6.6'  #@param {type: "string"}

#!pip install flywheel-sdk~={flywheel_SDK_version}

# Commented out IPython magic to ensure Python compatibility.
#@title Import packages
#@markdown Run this cell prior to the SDK functions
import pandas as pd
import time
import json
import re
import time
import logging
import datetime

import flywheel 
from pandas.io.json import json_normalize
# %load_ext google.colab.data_table

#@title log_progress widget code
#@markdown Run this cell prior to the SDK functions

def log_progress(sequence, every=None, size=None, name='Items'):
    from ipywidgets import IntProgress, HTML, VBox
    from IPython.display import display

    is_iterator = False
    if size is None:
        try:
            size = len(sequence)
        except TypeError:
            is_iterator = True
    if size is not None:
        if every is None:
            if size <= 200:
                every = 1
            else:
                every = int(size / 200)     # every 0.5%
    else:
        assert every is not None, 'sequence is iterator, set every'

    if is_iterator:
        progress = IntProgress(min=0, max=1, value=1)
        progress.bar_style = 'info'
    else:
        progress = IntProgress(min=0, max=size, value=0)
    label = HTML()
    box = VBox(children=[label, progress])
    display(box)

    index = 0
    try:
        for index, record in enumerate(sequence, 1):
            if index == 1 or index % every == 0:
                if is_iterator:
                    label.value = '{name}: {index} / ?'.format(
                        name=name,
                        index=index
                    )
                else:
                    progress.value = index
                    label.value = u'{name}: {index} / {size}'.format(
                        name=name,
                        index=index,
                        size=size
                    )
            yield record
    except:
        progress.bar_style = 'danger'
        raise
    else:
        progress.bar_style = 'success'
        progress.value = index
        label.value = "{name}: {index}".format(
            name=name,
            index=str(index or '?')
        )
log = logging.getLogger('notebook')

#@title Initialize `pacct_2_project`
#@markdown If you get a  Client version does not match server version error: 
#@markdown 1. Update the number in the second cell to match version listed
#@markdown by `WARNING:Flywheel:Use "pip install flywheel-sdk~=x.x.x"`
#@markdown 2. Edit > Clear all outputs
#@markdown 3. File > Save
#@markdown 4. Runtime > Reset all runtimes...
#@markdown 5. Re-run the code cells before and including this one.
#@markdown Don't forget to enter your API key!
fw = flywheel.Client(api_key)
del api_key
pacct_2_project = fw.get_project('5cace5acb2baaf0030809b02')

#@title Define some functions 
def add_func_to_moco_label(fw_client, moco_obj):
    # Calculate a range around the timestamp
    time_1 = moco_obj.timestamp-datetime.timedelta(seconds=20)
    time_str1 = time_1.strftime('%Y-%m-%dT%H:%M:%S')
    time_2 = moco_obj.timestamp+datetime.timedelta(seconds=20)
    time_str2 = time_2.strftime('%Y-%m-%dT%H:%M:%S')
    # Format a query for selecting appropriate functionals
    query = (
        'session={},'
        'timestamp>{},'
        'timestamp<{},'
        'files.classification.Features!=Derived,'
        'files.classification.Measurement=T2*,'
        'files.classification.Intent=Functional'
    )
    query = query.format(moco_obj.session,time_str1,time_str2)
    # Try to find one and return none if 0 or more than 1
    try:
        # The actual search
        func_match = fw_client.acquisitions.find_one(query)
        # Requested format is <func_label>-MoCoSeries
        update_label = '-'.join([func_match.label,moco_obj.label])
        result = moco_obj.update(label=update_label)
        return {moco_obj.id: result}
    except ValueError as e:
        print("Found wrong number of functional scans for {}! {}".format(moco_obj.id,e))
        print(create_session_link(fw_client, fw.get_session(moco_obj.session)))
        return None
    
    
def create_session_link(fw_client, session):
    api_url = fw_client.get_config().site.api_url
    #session = fw_client.get_session(session.id)
    site = re.search(r'\/\/(?P<site>.*?)(:|$)',api_url).group('site')

    sess_string = 'https://{}/#/projects/{}/sessions/{}'
    
    session_link = sess_string.format(site, session.project, session.id)
    return session_link


def format_session_label(fw_client, session_id):
    # Set up some patterns for checking labels
    number_patt = re.compile('.*([Pp][Aa]).*(?P<num>[\d]{3,}).*')
    visit_patt = re.compile('.*(?P<visit>[Vv][\d]+_?[Ww][\d]+$)')
    session_label_patt = re.compile('^PA[\d]*_V[\d]W[\d]$')
    # Get the session object
    session = fw_client.get_session(session_id)
    if session.id == '61be4e5cd68321a1a8ff918e':
        print('no way')
        return None
   
    # Use lastname to determine code and visit
    lastname = session.info['subject_raw']['lastname']

    # If cant find the code, return None
    if not number_patt.match(lastname):
        print(
            'Error: session PatientName {} '
            'does not contain PA followed by a 3-digit number.\n'
            '{}\n'.format(lastname, create_session_link(fw_client, session))
        )
        return None
    else:
        subject_code = 'PA' + number_patt.match(lastname).group('num')
    # If it's wave 2, the visit string will be in the label
    if visit_patt.match(lastname):
        visit = visit_patt.match(lastname).group('visit')
        visit = visit.replace('_','')
    # Assume V1W1 if no matches on lastname
    else:
        visit = 'V1W1'
    session_label = '_'.join([subject_code, visit])
    if subject_code not in session.subject.code.upper():
        print(
            'Error: session PatientName {} does not '
            'match subject code {} \n'
            '{} \n'.format(lastname, session.subject.code,
                          create_session_link(fw_client,session))
        )   
        return None
    else:
        if not session.label == session_label:
            print(
                'Updating session label {} to {}'.format(session.label, session_label)
            )
            assert session_label_patt.match(session_label)
            update = session.update(label = session_label)
            return update
        else:
            print('session label is already {}'.format(session_label))
            
            
def update_session_subject(fw_client, session):
    subjects=None
    session_label_patt = re.compile('^(?P<sub_code>PA[\d]*)_V[\d]W[\d]$')
    if not session_label_patt.match(session.label):
        print('ERROR: {} does not match the pattern for PACCT sessions'.format(
            session.label        
        ))
        print(create_session_link(fw_client,session))
        print('Please try updating session label first.')
        return None
    else:
        new_subject_code = session_label_patt.match(session.label).group('sub_code')
    if new_subject_code == session.subject.code:
        print('Session {} has the correct label {}'.format(session.label,
                                                           new_subject_code))
        return None
    
    query = 'project={},code={}'.format(session.project,new_subject_code)
    subjects = fw.subjects.find(query)
    if subjects:
        print('Moving {} to existing subject {}'.format(session.label,
                                                        new_subject_code))
        subject = subjects[0]
    else: 
        print('Creating new subject {} for session {}'.format(new_subject_code,
                                                             session.label))
        subject = fw.get(session.project).add_subject(code=new_subject_code)
    former_subject = fw_client.get(session.subject.id)
    update = session.update({'subject': {'_id': subject.id}})
    former_subject = former_subject.reload()
    if not former_subject.files and not former_subject.sessions():
        print('Removing orphan subject: {}'.format(former_subject.code))
        fw_client.delete_subject(former_subject.id)
    return update


#@title Define function for submitting a classifier job
def submit_job(fw_client, file_name, acq_obj,gear_name):
    # Get the classifier gear
    gear = fw.lookup('gears/{}'.format(gear_name))
    
    # Set inputs
    if gear_name == 'dcm2niix':
        inputs = {'dcm2niix_input': acq_obj.get_file(file_name)}
    elif gear_name == 'dicom-mr-classifier':
        inputs = {'dicom': acq_obj.get_file(file_name)}
    else:
        print('Gear name not recognized: {}'.format(gear_name))
        return None
    # Schedule the job
    job_id = gear.run(inputs=inputs, destination=acq_obj)
    return job_id

from flywheel import Flywheel, models
def run_curate_bids_job(fw, session_id=None, project_id=None, reset=False, entire_project=False):
    config = {"reset": reset, 'entire_project': entire_project}
    gear_id = fw.lookup('gears/curate-bids').id
    destination = models.job_destination.JobDestination(type="session", id=session_id)
  
    job = models.job.Job(gear_id=gear_id, destination=destination,
                         config=config)
    return fw.add_job(body=job)



#@title Update Session Labels
count = 0 
updates = list()
session_label_patt = re.compile('^PA[\d]*_V[\d]W[\d]$')
for session in log_progress(pacct_2_project.sessions(),name='Session'):
    if not session_label_patt.match(session.label):
        
        count += 1
        
        update = format_session_label(fw, session.id)
        if update:
            updates.append(update)
        else:
            print('Failed to update session: {}'.format(session.id))

# Check if all updated
if count == 0:
    print('No sessions to update!')
elif count == len(updates):
    print('All session labels were updated successfully!')
else:
    failures = count-len(updates)
    print('{} updates of {} failed'.format(
        failures, count))

#@title Update Session Subject Codes
query = 'project={}'.format(pacct_2_project.id)
sessions = [session for session in fw.sessions.iter_find(query)]
session_label_patt = re.compile('^(?P<sub_code>PA[\d]*)_V[\d]W[\d]$')
updates = list()
count = 0
for session in log_progress(sessions,name='session'):
    if session_label_patt.match(session.label):
        new_subject_code = session_label_patt.match(session.label).group(
            'sub_code')
        if new_subject_code == session.subject.code:
            continue
        else:
            count += 1
            session=fw.get(session.id)
            print('Attempting to update session {} to {}'.format(session.label,new_subject_code))
            update  = update_session_subject(fw,session)
            if update:
                updates.append(update)
            else: 
                print('Failed to update session {} subject'.format(session.label))
                print(create_session_link(fw,session))
failures = count - len(updates)
if failures == 0 and count > 0:
    print('Updated {} session subject codes'.format(count))
elif failures > 1:
    print('ERROR: Failed to update {} of {} session.subjects'.format(failures, 
                                                                     count))
else:
    print('All subject codes are correct!')

#@title Submit classifier jobs for unclassified DICOMS
query = (
    'parents.project=5cace5acb2baaf0030809b02,'
    'files.type!=nifti'
)
acqs = [acq for acq in fw.acquisitions.iter_find(query)]
if not acqs:
    print('No files to classify')
for acq in acqs:
    for file in acq.files:
        if file.type == 'qa' or file.classification:
            continue
        if file.classification == {} and file.type == 'dicom':
            # Submit job if missing classification, but context has it
            if acq.label in pacct_2_project.info['context']['classifications'].keys():
                count +=1
                print('Submitting a classifier job for {}'.format(acq.id))
                job_id = submit_job(fw,file.name,acq,'dicom-mr-classifier')
                job = fw.get_job(job_id)
                attempt = 0
                while job.state in ['pending', 'running']:
                    attempt += 1
                    print('Waiting for job {} to complete ({}/10)'.format(
                        job_id,attempt),end='\r')
                    job = fw.get_job(job_id)
                    if attempt == 10:
                        
                        break
                if job.state == 'complete':
                    print()
                    print('Job {} is complete!'.format(job_id))
                else:
                    print()
                    print(
                        'Job {} is still {}. Wait to run the next code'
                        ' cell until it completes:\n'
                        'https://cmrrc.zi.columbia.edu/#/jobslog/job/{}'
                        '\n'.format(job_id,job_state,job_id)
                    )
            else:
                print('Error! {} does not have a context classification!'.format(acq.label))
                print('Please add classification to context here:')
                print(
                    'https://docs.flywheel.io/hc/en-us/articles/360018072554-'
                    'How-to-use-custom-project-info-to-define-custom-classifications')

#@title Update MoCoSeries acquisition labels to be (func label)-MoCoSeries
query = (
    'label=MoCoSeries,'
    'parents.project={}'.format(pacct_2_project.id)
)

mocos = [acq for acq in fw.acquisitions.iter_find(query)]
if mocos:
    result_list = list()
    for moco in log_progress(mocos, name='Renaming MocoSeries'):
        result = add_func_to_moco_label(fw, moco)
        if result:
            result_list.append(result)
    if len(result_list) == len(mocos):
        print('Successfully updated {} of {} MoCoSeries!'.format(
            len(result_list),len(mocos)))
    else:
        print(len(result_list))
        
        failures = len(mocos) - len(result_list)
        count = len(mocos)
        
        print('Failed to update {} of {} MoCoSeries'.format(failures,count))
else:
    print('No MoCoSeries acquisitions to relabel!')

#@title Run BIDS Curation gear on the whole project
run_curate_bids_job(fw, session_id=pacct_2_project.sessions()[0].id, reset=False, entire_project=True)

#@title De-duplicate acquisition labels
bids_paths = list()
sessions = [session for session in fw.sessions.iter_find(
    'parents.project={}'.format(pacct_2_project.id))]
for session in log_progress(sessions,name='Scanning session files'):
    for acq in session.acquisitions():
        for file in acq.files:
            if file.type == 'nifti' and isinstance(file.info.get('BIDS'),dict):
                if file.info['BIDS'].get('Filename') and not file.info['BIDS']['ignore']:
                    bids_paths.append('/'.join([file.info['BIDS']['Path'],file.info['BIDS']['Filename']]))
paths_duplicates_unique = list(set([path for path in bids_paths if bids_paths.count(path)>1]))
if paths_duplicates_unique:
    session_list = list()
    for path in log_progress(paths_duplicates_unique):
        
        path, name = path.rsplit('/',1)
        acqs = None
        query = (
            'parents.project={},'
            'files.info.BIDS.Filename={},'
            'files.info.BIDS.Path={}'.format(pacct_2_project.id, name, path)
        )
    
        acqs = fw.acquisitions.find(query, sort='timestamp:asc')
        count = 0
        for acq in acqs:
            #print(acq.session)
            session_list.append(acq.session)
            #print(acq.label)
            
            for file in acq.files:
                if file.type == 'nifti':
                    file_info_dict = file.info
                    if isinstance(file_info_dict.get('BIDS'), dict):
                        if not file_info_dict['BIDS']['ignore']:
                            if not 'dupx' in file_info_dict['BIDS'].get('Acq'):
                                count += 1
                                file_info_dict['BIDS']['Acq'] = 'dupx'.join(filter(None,[file_info_dict['BIDS'].get('Acq'), str(count)]))
                                acq.update_file_info(file.name,file_info_dict)
                            else:
                                print('ERROR: "dupx" has already been added to acquisition label')
                        

        session_list = list(set(session_list))
        for session in session_list:
            
            run_curate_bids_job(fw, session_id=session, reset=False, entire_project=False)

"""END HERE. below is testing only..."""
#
# #@title Deface
# def submit_deface_job(fw_client, acquisition):
#
#     gear = fw_client.lookup('gears/mri-deface')
#     # use the acquisition's session as the destination
#     destination = fw_client.get_session(acquisition.session)
#
#     # set the config options
#     config = {'output_mgh': False, 'output_nifti': True}
#
#     # get list of nifti files in acquisition
#     nifti_files = [file_obj for file_obj in acquisition.files if file_obj.type == 'nifti']
#
#     # Something is off if more than one nifti file is in the acquisition
#     #need logic to deal with 2. also need logic to deal with W1 scans, labeled differently.
#     if len(nifti_files) == 1:
#         analysis_label = f'deface_acquisition_{acquisition.id}'
#         # get the first nifti file
#         inputs = {'anatomical': nifti_files[0]}
#         analysis_id = gear.run(analysis_label=analysis_label, destination=destination, config=config, inputs=inputs)
#         return analysis_id
#     else:
#         print(f'acquisition {acquisition.id} has {len(nifti_files)} nifti files. Expected 1')
#         return None
#
# #test_acq = fw.lookup('tottenham/PACCT_study_w2/PA170/PA170_V2W2/ABCD_T1w_MPR_vNav').reload()
# #submit_deface_job(fw, test_acq)
#
# #@title Deface - David query
# query = f'parents.project={pacct_2_project.id},label=ABCD_T1w_MPR_vNav,files.info.ImageType=NORM'
# acq_list = acq_list = [acq for acq in fw.acquisitions.iter_find(query)]
# test_acq = acq_list[0]
# print(test_acq)
# #submit_deface_job(fw, test_acq)
#
# #add in code for uploading EDITED poke and faces csvs
# #add in code to convert those files to TSVS
# #export project to elvis