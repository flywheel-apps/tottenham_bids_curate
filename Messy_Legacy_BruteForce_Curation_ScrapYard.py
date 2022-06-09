import flywheel
import os
from flywheel_gear_toolkit.utils.curator import HierarchyCurator
import copy
import datetime
import re
import tempfile
import pydicom
from pathlib import Path

MERGE_EXISTING = False

class Curator(HierarchyCurator):
    def __init__(self, **kwargs):
        # Install backoff
        super().__init__(**kwargs)
        # curate depth first
        self.config.depth_first = True
        # self.fw_client = self.context.client
        self.fw_client = flywheel.Client(os.environ["CUMC_API"])

    def curate_project(self, project: flywheel.Project):
        pass

    def curate_subject(self, subject: flywheel.Subject):
        pass
        # if subject.id in ['61be4e5cd68321a1a8ff918d']:
        #     print('no way')
        #     return None
        #
        # orig_subject = subject.reload()
        # subjects = None
        # print(f'checking subject {orig_subject.label}')
        # number_patt = re.compile('.*([Pp][Aa]).*(?P<num>[\d]{3,}).*')
        # lastname = orig_subject.lastname
        # if not lastname:
        #     session = subject.sessions.find_first()
        #     if not session:
        #         print(f'ERROR no session for subject {subject.label}')
        #         return None
        #     ses_label = session.label
        #     number_patt.match(ses_label).group('num')
        #     new_subject_code = 'PA' + number_patt.match(ses_label).group('num')
        #     subject.update({'lastname':f"Tottenhamlab/Pacct/{new_subject_code}"})
        # else:
        #     match = number_patt.match(lastname)
        #     if not match:
        #         print(f'ERROR bad format for subject {subject.label}')
        #         return None
        #     new_subject_code = 'PA' + match.group('num')
        # if new_subject_code == subject.code:
        #     print('Subject {} has the correct label'.format(orig_subject.label))
        #     return None
        #
        # query = 'project={},code={}'.format(orig_subject.project, new_subject_code)
        # subjects = self.fw_client.subjects.find(query)
        # if subjects:
        #     print('Moving {} to existing subject {}'.format(orig_subject.label,
        #                                                     new_subject_code))
        #     new_subject = subjects[0]
        # else:
        #     print('Creating new subject {} for session {}'.format(new_subject_code,
        #                                                           orig_subject.label))
        #     print("subject = fw_client.get(session.project).add_subject(code=new_subject_code)")
        #     new_subject = self.fw_client.get(subject.project).add_subject(code=new_subject_code)
        #     new_subject.firstname = orig_subject.firstname
        #     new_subject.lastname = orig_subject.lastname
        #     new_subject.sex = orig_subject.sex
        #
        #
        # for session in orig_subject.sessions():
        #     update = session.update({'subject': {'_id': new_subject.id}})
        #     print("update = session.update({'subject': {'_id': subject.id}})")
        # print(new_subject.id)
        # update = None
        # former_subject = orig_subject.reload()
        # if not former_subject.files and not former_subject.sessions():
        #     print('Removing orphan subject: {}'.format(former_subject.code))
        #     self.fw_client.delete_subject(former_subject.id)
        # return update

    def curate_session(self, session: flywheel.Session):
        pass
        # session = session.reload()
        # if session.id in ["61be4e5cd68321a1a8ff918e", "61be4e5cd68321a1a8ff918e"]:
        #     print("no way")
        #     return None
        #
        # if session.info.get('subject_raw', {}).get('curated', False):
        #     return None
        #
        # number_patt = re.compile('.*([Pp][Aa]).*(?P<num>[\d]{3,}).*')
        # visit_code = re.compile('.*(?P<visit>[Vv][\d]+_?[Ww][\d]+$)')
        # session_label_patt = re.compile('^(?P<sub_code>PA[\d]*)_V[\d]W[\d]$')
        #
        # sub_label = session.subject.label
        # if session_label_patt.match(sub_label):
        #     sub_id = session_label_patt.match(sub_label).group('sub_code')
        #     visit = visit_code.match(sub_label).group('visit')
        #
        # elif session_label_patt.match(session.subject.lastname):
        #     lastname = session.subject.lastname
        #     sub_id = session_label_patt.match(lastname).group('sub_code')
        #     visit = visit_code.match(lastname).group('visit')
        # else:
        #     sub_id = None
        #     visit = None
        #     print(f'Can not Decipher subject/session from {session.subject.label}')
        #     return None
        #
        # new_session_label = f"{sub_id}_{visit}"
        # new_subject_label = sub_id
        #
        # print(f'new session label {new_session_label}')
        #
        # find_subject = self.fw_client.subjects.find(f'project={session.parents.project},label={new_subject_label}')
        # if len(find_subject) > 1:
        #     print(f'ERROR MULTIPLE SUBJECTS WITH LABEL {new_subject_label} found!')
        #     return None
        # elif len(find_subject) == 0:
        #     print(f'no subject with label {new_subject_label} found, renaming')
        #     subject = session.subject
        #     subject.update({'label': new_subject_label})
        #     subject.firstname = session.subject.firstname
        #     subject.lastname = session.subject.lastname
        #     subject.sex = session.subject.sex
        #     subject = subject.reload()
        # else:
        #     subject = find_subject[0]
        #
        #
        # subject_raw = {'firstname': subject.firstname,
        #                'lastname': subject.lastname,
        #                'sex': subject.sex,
        #                'ground_truth': '',
        #                'curated':False}
        #
        # print('checking to see if new session label exists in subject')
        # find_sessions = subject.sessions.find(f'label={new_session_label}')
        # if len(find_sessions) > 1:
        #     print(f'ERROR MULTIPLE SESSIONS WITH LABEL {new_session_label} in {new_subject_label} found!')
        #     return None
        # elif len(find_sessions) == 0:
        #     print(f'no session with label {new_session_label} found.  Will rename and Move')
        #     session.update({'label': new_session_label})
        #     session.update({'subject': subject.id})
        #     subject_raw['ground_truth'] = new_session_label
        #     subject_raw['curated'] = True
        #
        # else:
        #     new_session = find_sessions[0]
        #     print(f'Session with {new_session_label} in {new_subject_label} found! to merge, set merge_existing = True')
        #     if MERGE_EXISTING:
        #         print('merging')
        #         for acq in session.acquisitions():
        #             acq.upadte({'session': new_session.id})
        #
        #         subject_raw['curated'] = True
        #
        #     else:
        #         print('skipping')
        #
        #
        # session.update_info({'subject_raw': subject_raw})




    def curate_session_gatherinfo(self, session: flywheel.Session):
        session = session.reload()
        if session.id in ["61be4e5cd68321a1a8ff918e", "61be4e5cd68321a1a8ff918e"]:
            print("no way")
            return None

        info = session.info
        if 'subject_raw' not in info:
            print(f'No subject raw in {session.subject.label}/{session.label}')
            return None

        if 'ground_truth' not in info['subject_raw']:
            print(f'No ground_truth in {session.subject.label}/{session.label}')
            return None

        ground_truth = info['subject_raw']['ground_truth']
        visit = ground_truth.split('_')[-1]

        if info.get('BIDS'):
            label = info['BIDS'].get('Label')
            if visit != label:
                print(f'BIDS label {label} does not match {visit} {session.subject.label}/{session.label}')


        pass
        # print(f'checking session {session.label}')
        # session = session.reload()
        # print(session.label)
        # print(session.info)
        # # Set up some patterns for checking labels

        # # Get the session object
        # if session.id in ['61be4e5cd68321a1a8ff918e','61be4e5cd68321a1a8ff918e']:
        #     print('no way')
        #     return None
        #
        # # Use lastname to determine code and visit
        # lastname = session.info.get('subject_raw',{}).get('lastname')
        # if not lastname:
        #     print(f'No lastname for {session.id}, updating...')
        #
        #     subject = session.subject.reload()
        #     ses_info = {'subject_raw':
        #                     {'firstname':subject.firstname,
        #                      'lastname':subject.lastname,
        #                      'sex':subject.sex}}
        #     session.update_info(ses_info)
        #     session = session.reload()
        #     lastname = session.info.get('subject_raw', {}).get('lastname')
        #
        # # If cant find the code, return None
        # if not number_patt.match(lastname):
        #     print(
        #         'Error: session PatientName {} '
        #         'does not contain PA followed by a 3-digit number.\n'
        #         '{}\n'.format(lastname, session.label)
        #     )
        #     return None
        # else:
        #     subject_code = 'PA' + number_patt.match(lastname).group('num')
        # # If it's wave 2, the visit string will be in the label
        # if visit_code.match(lastname):
        #     visit = visit_code.match(lastname).group('visit')
        #     visit = visit.replace('_', '')
        # # Assume V1W1 if no matches on lastname
        # else:
        #     visit = 'V1W1'
        # session_label = '_'.join([subject_code, visit])
        # if subject_code not in session.subject.code.upper():
        #     print(
        #         'Error: session PatientName {} does not '
        #         'match subject code {} \n'
        #         '{} \n'.format(lastname, session.subject.code,
        #                        session.label)
        #     )
        #     return None
        # else:
        #     if not session.label == session_label:
        #         print(
        #             'Updating session label {} to {}'.format(session.label, session_label)
        #         )
        #         assert session_label_patt.match(session_label)
        #         print("update = session.update(label=session_label)")
        #         print(session_label)
        #         update = session.update(label=session_label)
        #         update=None
        #         return update
        #     else:
        #         print('session label is already {}'.format(session_label))

    def curate_acquisition_old_old(self, acquisition: flywheel.Acquisition):
        session = self.fw_client.get_session(acquisition.parents.session)
        if session.id in ["61be4e5cd68321a1a8ff918e", "61be4e5cd68321a1a8ff918e"]:
            print("no way")
            return None

        acquisition = acquisition.reload()

        csl = acquisition.info['curation']['current']['session_label']

        current_sub = acquisition.info['curation']['current']['subject_label']
        rec_visit = acquisition.info['curation']['recommended']['visit'].upper()
        rsl = f'{current_sub}_{rec_visit}'

        new_ses = None
        if rsl != csl:

            print(f'mismatch for {session.label}/{acquisition.label}')

            # WE ARE IGNORING SUBJECT MISMATCHES, THEY HAVE BEEN GONE OVER MANUALLY

            current_sub = acquisition.info['curation']['current']['subject_label']
            rec_visit = acquisition.info['curation']['recommended']['visit'].upper()
            true_ses_label = f'{current_sub}_{rec_visit}'

            new_ses = find_or_create_session(self.fw_client, acquisition.parents.project, acquisition.parents.subject, true_ses_label, session)
            acquisition.update({'session': new_ses.id})
            curation = acquisition.info['curation']
            curation['current']['session_label'] = new_ses.label
            acquisition.update_info({'curation': curation})


        else:
            print(f'OK {session.label}/{acquisition.label}')
            new_ses = session

        update_session_info(new_ses, acquisition)




    def curate_acquisition(self, acquisition: flywheel.Acquisition):
        number_patt = re.compile(".*([Pp][Aa]).*(?P<num>[\d]{3,}).*")
        visit_patt = re.compile(".*(?P<visit>[Vv][\d]+_?[Ww][\d]+$)")
        session_label_patt = re.compile("^PA[\d]*_V[\d]W[\d]$")
        # Get the session object

        acquisition = acquisition.reload()

        session = self.fw_client.get_session(acquisition.parents.session)
        subject = self.fw_client.get_subject(acquisition.parents.subject)
        project = self.fw_client.get_project(acquisition.parents.project)

        if session.id in ["61be4e5cd68321a1a8ff918e", "61be4e5cd68321a1a8ff918e"]:
            print("no way")
            return None

        curate_dict = identify_acq_visit(self.fw_client, acquisition)
        curate_dict["current"] = {
            "session_label": session.label,
            "subject_label": subject.label,
        }

        acq_org_info = {"curation": curate_dict}
        #print(curate_dict)
        acquisition.update_info(acq_org_info)
        visit = curate_dict["recommended"]["visit"]
        ses_id = curate_dict["recommended"]["session_label"]
        subject_id = curate_dict["recommended"]["subject_label"]

        if not visit:
            print(f'{subject_id}/{ses_id}/{acquisition.label} CANT CURATE')
            return None

        mismatch = ""

        if session.label == ses_id and subject.label == subject_id:
            print(f'{subject_id}/{ses_id}/{acquisition.label} good match')
            return None

        ses_mismatch = False
        sub_mismatch = False
        if session.label != ses_id:
            mismatch += "SESSION"
            ses_mismatch = True
            print(f'{ses_id=}, {session.label=}')
        if subject.label != subject_id:
            mismatch += " SUBJECT"
            sub_mismatch = True
            print(f'{subject_id=}, {subject.label=}')


        print(
            f"{mismatch} Mismatch:\n{subject.label}/{session.label}/{acquisition.label} vs \n{subject_id}/{ses_id}/{acquisition.label}\n"
        )

        if sub_mismatch:
            subject = find_subject(project, subject, subject_id)

        if ses_mismatch:
            ses = find_session(subject, ses_id)
            print(f" would move {acquisition.label} to {subject_id}/{ses_id}")

        #
        #
        # acq_files = acquisition.files
        # if len(acq_files) == 0:
        #     print(f'ERROR no files in {subject.label}/{session.label}/{acquisition.label}')
        #     return None
        #
        # sample_file = acq_files[0]
        # finfo = sample_file.info
        # patient_id = finfo.get('PatientID', None)
        #
        # if not patient_id:
        #     print(f'No Patient ID in {subject.label}/{session.label}/{acquisition.label}')
        #     return None
        #
        #
        # number_patt = re.compile('.*([Pp][Aa]).*(?P<num>[\d]{3,}).*')
        # visit_code = re.compile('.*(?P<visit>[Vv][\d]+_?[Ww][\d]+$)')
        # session_label_patt = re.compile('^PA[\d]*_V[\d]W[\d]$')

        pass

    def curate_analysis(self, analysis: flywheel.AnalysisOutput):
        pass

    def validate_file(self, file_: flywheel.FileEntry):

        # print(f"checking {file_.parent.parents.session} {file_.parent.label} {file_.name}")

        invalid_re = re.compile(".*(vNav|Scout|Phoenix|Start|t-Map|GLM|Design).*")
        if invalid_re.match(file_.info.get("SeriesDescription", "")):
            # print(f"Not curating {file_.info.get('SeriesDescription')}")
            return False

        if file_.type not in ["dicom"]:
            return False

        if file_.parent.container_type != "acquisition":
            return False

        if file_.modality != "MR":
            return False

        return True

    def curate_file(self, file_: flywheel.FileEntry):
        pass

    ######################################################################
    ## OLD SECTIONS OF CODE
    ## These aren't run and will only be necessary if things change in the future
    ######################################################################

    def curate_file_old(self, file_: flywheel.FileEntry):
        """This only needs to be run if you need to curate the motion correction files for BIDS."""

        if not self.validate_file(file_):
            print(f"Not curating {file_.name}")
            return

        parent = file_.parent
        parent = parent.reload()
        file_new = [f for f in parent.files if f.id == file_.id][0]

        if not file_new.info.get("SeriesDescription"):
            print(
                f"No Series description for {file_new.parent.parents.session} {file_new.parent.label} {file_new.name}"
            )
            print(file_new.info)

        info = copy.deepcopy(file_new.info)

        if info["SeriesDescription"] != "MoCoSeries":
            return

        if file_new.info.get("Hierarchy_Curated", False):
            print("skipping curated file")
            return

        parent_ses = self.fw_client.get_session(parent.parents["session"])
        content_time = file_new.parent.timestamp
        current_inc = 1
        min_time = content_time - datetime.timedelta(seconds=current_inc)
        max_time = content_time + datetime.timedelta(seconds=current_inc)
        acqs = parent_ses.acquisitions.find(
            f"_id!={parent.id},label!=StartFMRI,timestamp<={max_time.strftime('%Y-%m-%dT%H:%M:%S')},timestamp>={min_time.strftime('%Y-%m-%dT%H:%M:%S')}"
        )

        if len(acqs) == 1:
            acq = acqs[0]
            acq = acq.reload()
            file_match = [f for f in acq.files if f.type == "dicom"][0]
            update_acq_from_file(parent, file_match)
            print("Updating curator key")
            print(file_new.update_info({"Hierarchy_Curated": True}))
            return

        # print(info)
        # print(file_new.name)

        foruid = info.get("FrameOfReferenceUID")
        image_type = info.get("ImageType")
        if isinstance(image_type, str):
            image_type = string_to_list_honestlywtf(file_new, image_type)
            print("modifying")
            print(file_.name)
            print(file_.parent.label)
            print(file_.parent.id)

        # return
        to_pop = ["MOCO", "DERIVED", "FILTERED"]
        for tp in to_pop:
            if tp in image_type:
                image_type.pop(image_type.index(tp))

        new_image_type = ["ORIGINAL"]
        new_image_type.extend(image_type)

        new_image_type = "[" + ", ".join(new_image_type) + "]"
        content_time = file_new.parent.timestamp

        current_inc = 0

        results = []

        while not results:
            current_inc += 1

            if current_inc > 5 * 60:
                print("No matching files found within 5 min of:")
                print(
                    f"ses: {file_new.parent.session} acq: {file_new.parent.label} file: {file_new.name}"
                )
                return -1

            min_time = content_time - datetime.timedelta(seconds=current_inc)
            max_time = content_time + datetime.timedelta(seconds=current_inc)

            print(
                f"Searcing for files acquired near {file_new.parent.label} +/- {current_inc} second"
            )

            query = (
                f"file.info.FrameOfReferenceUID = {foruid} AND "
                f"file.info.ImageType IN {new_image_type} AND "
                f"session._id = '{file_new.parent.parents.session}' AND "
                f"acquisition.timestamp >= {min_time.strftime('%Y-%m-%dT%H:%M:%S')} AND "
                f"acquisition.timestamp <= {max_time.strftime('%Y-%m-%dT%H:%M:%S')} AND "
                f"NOT file.info.ImageType IN [DERIVED, MOCO]"
            )
            #   f"acquisition.timestamp <= {max_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')} AND " \
            # print(query)
            results = self.fw_client.search(
                {"structured_query": query, "return_type": "file"}
            )

            if not results:
                print(query)

        files = [
            ff
            for f in results
            for ff in self.fw_client.get_acquisition(f.parent["_id"]).files
            if ff.name == f.file.name
        ]

        if len(results) > 1:
            print("More than one file found in results")
            print(f"ses: {files[0].parent.session}")
            print([f.parent.label for f in files])
            return

        file_match = files[0]
        update_acq_from_file(parent, file_match)

        print("Updating curator key")
        print(file_new.update_info({"Hierarchy_Curated": True}))




def find_or_create_session(fw, project_id, subject_id, ses_label, current_session):

    sess = fw.sessions.find(f"project={project_id},subject={subject_id},label={ses_label}")
    print(f"project={project_id},subject={subject_id},label={ses_label}")
    if len(sess) > 1:
        print(f'ERROR MORE THAN ONE SES FOUND WITH LABEL {ses_label} for sub {subject_id}')
        raise Exception('BAD BAD BAD')

    if not sess:
        print(f'Creating session {ses_label}')
        subject = fw.get_subject(subject_id)
        ses = subject.add_session(label=ses_label)

        ses.update_info(current_session.info)

        ses.update({'age': current_session.age,
                        'weight': current_session.weight,
                        'timestamp': current_session.timestamp,
                        'timezone': current_session.timezone})

    else:
        print('session_exists')
        ses = sess[0]

    ses = ses.reload()

    return ses

def update_session_info(current_ses, acq):

    subject_raw = current_ses.info.get('subject_raw')
    ground_truth = f"{acq.info['curation']['current']['subject_label']}_{acq.info['curation']['recommended']['visit']}"
    if not subject_raw:
        print('no subject raw, adding')
        subject_raw = {'lastname': current_ses.subject.lastname,
                        'firstname': current_ses.subject.firstname,
                        'sex': current_ses.subject.sex,
                        'ground_truth': ground_truth}

    elif not subject_raw.get('ground_truth'):
        print('no ground truth, adding')
        subject_raw['ground_truth'] = ground_truth

    current_ses.update_info({'subject_raw': subject_raw})












def update_acq_from_file(acq, file_match):
    series_name = file_match.info.get("SeriesDescription")
    new_name = f"{series_name}-MoCoSeries"

    old_name = acq.label
    if old_name != new_name:
        print(f"WARNING! Old name {old_name} does not match new name {new_name}")
        print("file_.update_info({'old_acquisition_label': old_name})")
        print(old_name)
        acq.update_info({"old_acquisition_label": old_name})

        print("Would Run file_.parent.update({'label': new_name})")
        print(new_name)
        acq.update({"label": new_name})
    else:
        print(f"{acq.label} already curated")


def string_to_list_honestlywtf(file_new, string):
    string = string[string.find("[") + 1 : string.find("]")]
    string = string.replace("'", "")
    string_list = string.split(", ")
    file_new.update_info({"ImageType": string_list})
    return string_list


def check_patient_id_for_visit(fw, acq):

    number_patt = re.compile(".*([Pp][Aa][Cc]?[Cc]?[Tt]?).*(?P<num>[\d]{3,}).*")
    visit_patt = re.compile(".*(?P<visit>[Vv][\d]+_?[Ww][\d]+$)")
    # Get the session object
    return_dict = {
        "number": None,
        "visit": None,
        "SOPInstanceUID": None,
        "SeriesInstanceUID": None,
        "StudyInstanceUID": None,
        "PatientID": None,
    }

    files = acq.files
    if len(files) == 0:
        print(f"no files on {acq.label}")
        return None

    for file in files:

        if not return_dict["PatientID"]:
            patientid = file.info.get("PatientID")
            if not patientid:
                print("DEBUG no patientID")
                continue

            number_match = number_patt.match(patientid)
            if not number_match:
                print("checking patient name")
                patientid = file.info.get("PatientName")
                number_match = number_patt.match(patientid)
                if not number_match:
                    continue
            return_dict["PatientID"] = patientid

        else:
            patientid = return_dict["PatientID"]

        if not return_dict["SOPInstanceUID"]:
            return_dict["SOPInstanceUID"] = file.info.get("SOPInstanceUID")
        if not return_dict["SeriesInstanceUID"]:
            return_dict["SeriesInstanceUID"] = file.info.get("SeriesInstanceUID")
        if not return_dict["StudyInstanceUID"]:
            return_dict["StudyInstanceUID"] = file.info.get("StudyInstanceUID")

        if not return_dict["number"]:
            number_match = number_patt.match(patientid)
            if not number_match:
                print(
                    f"no subject ID match for {patientid} on {acq.label} in session {acq.parents.session}"
                )
                return_dict["number"] = None
            else:
                return_dict["number"] = number_match.group("num")

        if not return_dict["visit"]:
            visit_match = visit_patt.match(patientid)
            if not visit_match:
                print(f"no visit match with {patientid}")
                return_dict["visit"] = check_file_info_special_cases(fw, acq, file)
                if not return_dict["visit"]:
                    print(
                        f"no visit ID match for {patientid} on {acq.label} in session {acq.parents.session}"
                    )
            else:
                return_dict["visit"] = visit_match.group("visit")

        if all(return_dict.values()):
            break

    return return_dict


def check_file_info_special_cases(fw, acq, file):
    print("Checking special Cases for visit")
    print(acq.label)
    session_label_patt = re.compile("^PA[\d]*_V[\d]W[\d]$")
    visit_patt = re.compile(".*(?P<visit>[Vv][\d]+_?[Ww][\d]+$)")

    visit = None

    if file.info.get("PatientName"):
        patient_name = file.info.get("PatientName")
        if patient_name:
            session_match = visit_patt.match(patient_name)
            if session_match:
                visit = session_match.group("visit")
                return visit.upper()
            print(f"no match with {patient_name}")

        session = fw.get_session(acq.parents.session)
        session_match = session_label_patt.match(session.label)
        if not session_match:
            subject = fw.get_subject(acq.parents.subject)
            subject_match = visit_patt.match(subject.label)
            if not subject_match:
                visit = None
            else:
                visit = subject_match.group("visit")
        else:
            visit = visit_patt.match(session.label).group("visit")


    elif file.info.get(
        "StudyDescription", file.info.get("PerformedProcedureStepDescription")
    ) in [
        "Tottenham^Child",
        "Tottenham_Child",
        "Tottenham^PACCT_study",
        "Tottenham_PACCT_study",
    ]:
        visit = "V1W1"
        print("DEBUG V1 match based off of old study description!")

    elif file.info.get(
        "StudyDescription", file.info.get("PerformedProcedureStepDescription")
    ) in [
        "Tottenham^PACCT_study_w2",
        "Tottenham_PACCT_study_w2",
    ]:
        visit = "V2W2"
        print("DEBUG V2 match based off of old study description!")


    return visit


def retry_patient_id_with_dicom(acq):
    dicom_file = [f for f in acq.files if f.type == "dicom"][0]
    zip_info = acq.get_file_zip_info(dicom_file.name)
    members = zip_info["members"]
    member_names = [m for m in members if m["path"].endswith(".dcm")]
    target_file = member_names[0]["path"]

    number_patt = re.compile(".*([Pp][Aa][Cc]?[Cc]?[Tt]?).*(?P<num>[\d]{3,}).*")
    visit_patt = re.compile(".*(?P<visit>[Vv][\d]+_?[Ww][\d]+$)")

    return_dict = {
        "number": None,
        "visit": None,
        "SOPInstanceUID": None,
        "SeriesInstanceUID": None,
        "StudyInstanceUID": None,
        "PatientID": None,
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        temp_file = temp_path / "temp_dicom.dcm"
        acq.download_file_zip_member(dicom_file.name, target_file, temp_file)

        dc = pydicom.read_file(temp_file)

        patientid = str(dc.get("PatientID", ""))
        number_match = number_patt.match(patientid)
        if not number_match:
            patientid = str(dc.get("PatientName", ""))
            number_match = number_patt.match(patientid)
            print("Dunno what else to check here")

        return_dict["PatientID"] = str(patientid)
        return_dict["SOPInstanceUID"] = str(dc.get("SOPInstanceUID"))
        return_dict["SeriesInstanceUID"] = str(dc.get("SeriesInstanceUID"))
        return_dict["StudyInstanceUID"] = str(dc.get("StudyInstanceUID"))

        if not return_dict["number"]:
            number_match = number_patt.match(patientid)
            if not number_match:
                print(
                    f"no subject ID match for {patientid} on {acq.label} in session {acq.parents.session}"
                )
                return_dict["number"] = None
            else:
                return_dict["number"] = number_match.group("num")

        if not return_dict["visit"]:
            visit_match = visit_patt.match(patientid)
            if not visit_match:
                # session_label_patt = re.compile('^PA[\d]*_V[\d]W[\d]$')
                patient_name = str(dc.get("PatientName"))
                if patient_name:
                    visit_match = visit_patt.match(patient_name)
                    if visit_match:
                        return_dict["visit"] = visit_match.group("visit").upper()

                elif dc.get(
                    "StudyDescription", dc.get("PerformedProcedureStepDescription")
                ) in [
                    "Tottenham^Child",
                    "Tottenham_Child",
                    "Tottenham^PACCT_study",
                    "Tottenham_PACCT_study",
                ]:
                    return_dict["visit"] = "V1W1"
                elif dc.get(
                        "StudyDescription", dc.get("PerformedProcedureStepDescription")
                    ) in [
                             "Tottenham^PACCT_study_w2",
                             "Tottenham_PACCT_study_w2",
                         ]:
                    return_dict["visit"] = "V2W2"
                    print("DEBUG V2 match based off of old study description!")

            else:
                return_dict["visit"] = visit_match.group("visit").upper()

    return return_dict


def identify_acq_visit(fw, acq):
    acq_info = check_patient_id_for_visit(fw, acq)
    if not acq_info["number"]:
        print(
            f"no guesses for {acq.label} in session {acq.parents.session}, downloading dicom"
        )
        retry_acq_info = retry_patient_id_with_dicom(acq)
        print(retry_acq_info)

        for key in retry_acq_info.keys():
            if acq_info[key]:
                if retry_acq_info[key]:
                    if acq_info[key] != retry_acq_info[key]:
                        print(
                            f"Original value {key}:{acq_info[key]} does not equal new value {retry_acq_info[key]}"
                        )
            else:
                acq_info[key] = retry_acq_info[key]

    if acq_info["visit"]:
        acq_info["visit"] = acq_info["visit"].upper()

    curate_dict = {
        "recommended": {
            "session_label": f"PA{acq_info['number']}_{acq_info['visit']}",
            "subject_label": f"PA{acq_info['number']}",
            "visit": acq_info["visit"],
            "number": acq_info["number"],
        },
        "SOPInstanceUID": acq_info["SOPInstanceUID"],
        "StudyInstanceUID": acq_info["StudyInstanceUID"],
        "SeriesInstanceUID": acq_info["SeriesInstanceUID"],
        "PatientID": acq_info["PatientID"],
    }

    return curate_dict


def find_subject(project, orig_subject, new_subject_code):

    subjects = project.subjects.find(f'label="{new_subject_code}"')

    if subjects:
        subject = subjects[0]
        print(f"Mismatched subject {subject.label} exists")
    else:
        print(f"Mismatched subject {new_subject_code} needs to be created")
        # print('Creating new subject {} for session {}'.format(new_subject_code,
        #                                                       orig_subject.label))
        # print("subject = fw_client.get(session.project).add_subject(code=new_subject_code)")
        # subject = project.add_subject(code=new_subject_code)
        # subject.firstname = orig_subject.firstname
        # subject.lastname = orig_subject.lastname
        # subject.sex = orig_subject.sex
        subject = orig_subject

    return subject


def find_session(subject, ses_id):
    sessions = subject.sessions.find(f"label={ses_id}")
    if not sessions:
        print(f"Mismatched session {subject.label}/{ses_id} needs to be created")
    else:
        print(f"Mismatched session {ses_id} exists")

    return None


def add_subraw(ses_id):
    session = fw.get_session(ses_id)
    sraw = session.info.get('subject_raw',{})

    if 'firstname' not in sraw:
        sraw['firstname'] = session.subject.firstname
    if 'lastname' not in sraw:
        sraw['lastname'] = session.subject.lastname
    if 'sex' not in sraw:
        sraw['sex'] = session.subject.sex
    if 'ground_truth' not in sraw:
        sraw['ground_truth'] = session.label
    if 'curated' not in sraw:
        sraw['curated'] = False

    session.update_info({'subject_raw':sraw})


