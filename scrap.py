import logging

import flywheel
import pandas as pd
import flywheel_gear_toolkit


import os

fw = flywheel.Client(os.environ["CUMC_API"])

#
# ses = fw.get_session("61b603bdcbd37589e5ff8ef8")
#
# acq=ses.acquisitions.find('label=MOVIE-MoCoSeries')
# acq=acq[0]
#
# file_new=[f for f in acq.files if f.name == "1.3.12.2.1107.5.2.43.166010.2021121210021311138824100.0.0.0.nii.gz"]
# query = (
#     "file.info.FrameOfReferenceUID = None AND "
#     "file.info.ImageType IN [ORIGINAL, PRIMARY, FMRI, MB, ND, FILTERED, MOSAIC] AND "
#     "session._id = '61b603bdcbd37589e5ff8ef8' AND "
#     "acquisition.timestamp >= 2021-12-12T15:03:00.000Z AND "
#     "acquisition.timestamp <= 2021-12-12T15:03:02.000Z AND "
#     "NOT file.info.ImageType IN [DERIVED, MOCO]"
# )
#
# query = (
#     "file.info.FrameOfReferenceUID = None AND "
#     "file.info.ImageType IN [ORIGINAL, PRIMARY, FMRI, MB, ND, FILTERED, MOSAIC] AND "
#     "session._id = '61b603bdcbd37589e5ff8ef8' AND "
#     "NOT file.info.ImageType IN [DERIVED, MOCO]"
# )
#
#
# query = "file.info.FrameOfReferenceUID = 1.3.12.2.1107.5.2.43.166010.1.20211212091318098.0.0.0 AND file.info.ImageType IN [ORIGINAL, PRIMARY, FMRI, MB, ND, FILTERED, MOSAIC] AND session._id = '61b603bdcbd37589e5ff8ef8' AND acquisition.timestamp >= 2021-12-12T14:32:43.000Z AND acquisition.timestamp <= 2021-12-12T14:32:45.000Z AND NOT file.info.ImageType IN [DERIVED, MOCO]"

fw_client = fw
#results = fw_client.search({"structured_query": query, "return_type": "file"})

import re


def string_to_list_honestlywtf(file_new, string):
    string = string[string.find('[')+1:string.find(']')]
    string = string.replace("'","")
    string_list = string.split(', ')
    file_new.update_info({'ImageType': string_list})
    return string_list


def validate_file(file_: flywheel.FileEntry):

    #print(f"checking {file_.parent.parents.session} {file_.parent.label} {file_.name}")

    invalid_re = re.compile('.*(vNav|Scout|Phoenix|Start|t-Map|GLM|Design).*')
    if invalid_re.match(file_.info.get('SeriesDescription','')):
        #print(f"Not curating {file_.info.get('SeriesDescription')}")
        return False

    if file_.type not in ['dicom']:
        return False

    if file_.parent.container_type != 'acquisition':
        return False

    if file_.modality != 'MR':
        return False

    return True





acqs = fw.acquisitions.iter_find('parents.project=5cace5acb2baaf0030809b02')
invalid_re = re.compile('.*(vNav|Scout|Phoenix|Start|t-Map|GLM|Design).*')

for acq in acqs:
    if invalid_re.match(acq.label):
        continue

    acq = acq.reload()


    for file_ in acq.files:

        if not validate_file(file_):
            #print(f"Not curating {file_.name}")
            continue


        if not file_.info.get('SeriesDescription'):
            print(f'No Series description for {file_.parent.parents.session} {file_.parent.label} {file_.name}')
            print(file_.info)

        info = file_.info

        if info['SeriesDescription'] != 'MoCoSeries':
            continue


        #file_.add_tag('CuratorValid')
        curated = file_.info.get('Hierarchy_Curated','DNE')
        acq.update_info({'Hierarchy_Curated': curated})


# need pip install flywheel-sdk~=14.6.9



import flywheel
import datetime
from dateutil.parser import parse
import pytz

fw = flywheel.Client('<YOUR_API_KEY>')
users = fw.get_all_users()

################################################################
#       Example 1, users not logged in since a date            #
################################################################
time_1 = datetime.datetime(year=2019,month=1,day=1,tzinfo=pytz.utc)
users_last_login_date = [u for u in users if u.lastlogin is not None and parse(u.lastlogin) < time_1]
# Note that we must parse u.lastlogin because that is stored as a string in flywheel
# Note that we must ensure that lastlogin exists for comparison (discussed at the end of these examples)

print(f'\n\nThese users have not logged in since {time_1}:\n')
for user in users_last_login_date:
    print(f"{user.id}\t{user.lastlogin}")
    # Uncomment these lines to disable users:
    # fw.modify_user(user.id, {'disabled':True})


################################################################
#       Example 2, users not logged in for the past month      #
################################################################
time_2 = datetime.datetime.now(pytz.utc) - datetime.timedelta(days=60)
users_last_login_2months = [u for u in users if u.lastlogin is not None and parse(u.lastlogin) < time_2]

print('\n\nThese users have not logged in during the past two months:\n')
for user in users_last_login_2months:
    print(f"{user.id}\t{user.lastlogin}")
    # Uncomment these lines to disable users:
    # fw.modify_user(user.id, {'disabled':True})

################################################################
#       Example 3, users created a year ago                    #
################################################################
time_3 = datetime.datetime.now(pytz.utc) - datetime.timedelta(days=365)
users_created_1year = [u for u in users if u.created < time_3]
# Note that we do not need to parse created because that is stored as a datetime...yeah idk why either...

print('\n\nThese users were created over a year ago:\n')
for user in users_created_1year:
    print(f"{user.id}\t{user.lastlogin}")
    # Uncomment these lines to disable users:
    # fw.modify_user(user.id, {'disabled':True})


################################################################
#  NOTE, some users don't have last login info, possibly a problem in reporting that info. Investigate these manually.
################################################################
users_without_info = [u for u in users if u.lastlogin is None]

print('\n\nThese users may not have logged in/login info is not reporting properly\n')
for user in users_without_info:
    print(f"{user.id}\t{user.created}")



