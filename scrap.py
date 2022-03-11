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