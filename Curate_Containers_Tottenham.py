from dataclasses import dataclass
import flywheel
from flywheel_gear_toolkit.utils.curator import HierarchyCurator
from flywheel_gear_toolkit.utils.reporters import AggregatedReporter, BaseLogRecord
import os
import re
import tempfile
from pathlib import Path
from flywheel.rest import ApiException

# Version 1.0.5
version = "1.0.5"
print(f"{version=}")


## Changelog:

# Version 1.0.5
# moved old subject cleanup to a more robust try...except I hope

# Version 1.0.4
# Added more initial validation checks

# Version 1.0.3
# In new API, empty subjects are automatically deleted
# This script now accounts for that and avoids an API exception.

# Version 1.0.2
# Added global config options


# Version 1.0.1
# added a fix for new SDK session update rules

"""
Constants to be set for run:

- MERGE_EXISTING: if a new subject has a session/subject id that matches an existing session/subject ID, merge them together
- DELETE_EMPTY: If a session or subject is moved to another container, delete the old container if it's empty
- LOG_ONLY: Do not make any changes on flywheel, simply log what you would do in the gear log. 

NOTE: Set these in the project by checking or unchecking the boxes in:

project.info.HierarchyCurator.Tottenham_Container_Curator


"""
MERGE_EXISTING = 'MERGE_EXISTING'
DELETE_EMPTY = "DELETE_EMPTY"
LOG_ONLY = 'LOG_ONLY'

# Declare patterns to be used
session_label_patt = re.compile('^(?P<sub_code>PA[\d]*)_V[\d]W[\d]$')
sub_id_code = re.compile('.*([Pp][Aa]).*(?P<num>[\d]{3,}).*')
visit_code = re.compile('.*(?P<visit>[Vv][\d]+_?[Ww][\d]+$)')

CURATOR_NAME = "Tottenham_Container_Curator"
NAMESPACE = "HierarchyCurator"

CONNECT_TIMEOUT = int(10)
REQUEST_TIMEOUT = int(120)


def get_global_settings(fw_client, destination_id):
    global MERGE_EXISTING, DELETE_EMPTY, LOG_ONLY

    dest = fw_client.get(destination_id)
    if dest.container_type == "project":
        project = dest
    else:
        project = fw_client.get_project(dest.parents["project"])

    info = project.info

    if NAMESPACE not in project.info:
        print(f"{NAMESPACE} not in project info, creating")
        info[NAMESPACE] = {}

    if CURATOR_NAME not in info[NAMESPACE]:
        print(f"{CURATOR_NAME} not in project info, creating")
        info[NAMESPACE][CURATOR_NAME] = {
            LOG_ONLY: True,
            DELETE_EMPTY: False,
            MERGE_EXISTING: False,
        }

    pipeline_settings = info[NAMESPACE][CURATOR_NAME]

    LOG_ONLY = pipeline_settings[LOG_ONLY]
    DELETE_EMPTY = pipeline_settings[DELETE_EMPTY]
    MERGE_EXISTING = pipeline_settings[MERGE_EXISTING]
    print(f"Pipeline Settings: \n {pipeline_settings}")



@dataclass
class MapLogRecord(BaseLogRecord):
    """
    This is simply the format of the output log report file that the curator will generate.
    """

    old_ses_id: str = ""
    new_ses_id: str = ""
    old_ses_label: str = ""
    new_ses_label: str = ""
    old_subject_id: str = ""
    new_subject_id: str = ""
    old_subject_label: str = ""
    new_subject_label: str = ""
    moved_sub: bool = False
    moved_ses: bool = False
    msg: str = ""




class Curator(HierarchyCurator):
    def __init__(self, **kwargs):
        # Install backoff
        super().__init__(**kwargs)
        # curate depth first
        self.config.multi = False
        self.config.depth_first = False
        self.config.stop_level = "session"
        self.config.report = True
        self.config.format = MapLogRecord
        self.config.path = "/flywheel/v0/output/curation_report.csv"
        #self.config.path = "/Users/davidparker/Documents/Flywheel/Clients/Columbia/Tottenham/bids-curate/report.csv"
        self.legacy = True
        self.depth_first = self.config.depth_first

        if self.config.report:
            self.reporter = AggregatedReporter(
                self.config.path, format=self.config.format)



        self.fw_client = self.context.client
        get_global_settings(self.fw_client, self.context.destination.get("id"))

        #self.fw_client = flywheel.Client(os.environ["CUMC_API"])

    def curate_project(self, project: flywheel.Project):
        pass

    def curate_subject(self, subject: flywheel.Subject):
        pass

    def validate_subject(self, subject: flywheel.Subject):
        if subject.id == '61be4e5cd68321a1a8ff918d':
            return False
        return True

    def curate_session(self, session: flywheel.Session):

        # THis cursed session still needs attention
        if session.id in ["61be4e5cd68321a1a8ff918e", "61be4e5cd68321a1a8ff918e"]:
            print("no way")
            return None

        try:
            session = session.reload()
        except Exception as e:
            print(f'problem with session {session.id}')
            print(e)
            return None
        print(f'Starting session {session.subject.label}/{session.label}')

        # First we will refresh the container object so the metadata is complete and up to date.
        # We will then go through special cases that we will want to skip.
        msg = ""
        old_ses_id = session.id
        old_ses_label = session.label
        old_subject_label = session.subject.label
        # Get the original subject ID
        original_subject_id = session.subject.id



        # If it's already marked "curated" in session.info.subject_raw.curated, then skip
        if session.info.get('subject_raw', {}).get('curated', False):
            return None

        # If "ignore-BIDS" is in the label, then skip.
        if "ignore-BIDS" in session.label:
            return None


        # Check if the session looks curated
        # The session looks curated if the label matches the format defined in 'session_label_patt', and if there is
        # a 'session.info.subject_raw.ground_truth" metadata value that matches the actual label

        ses_label = session_label_patt.match(session.label)
        if ses_label and session.label == session.info.get('subject_raw',{}).get('ground_truth'):
            print(f'{session.label} looks curated')
            subject_raw = session.info.get('subject_raw',{})
            subject_raw['curated'] = True
            session.update_info({'subject_raw':subject_raw})
            return None

        # If it doesn't "look curated", then get the "subject_raw" info so we can update it as we curate
        subject_raw = session.info.get('subject_raw',{})
        subject_raw['curated'] = False
        session.update_info({'subject_raw':subject_raw})

        # Attempt to find the subject id and the visit id from the session metadata
        sub_id, m = get_subject_label(session)
        msg += m

        visit, m = get_session_label(session)
        msg += m

        # Matching problems are reported
        if not sub_id or not visit:
            print(f'Can not Decipher subject/session from {session.subject.label}')
            print(f'Best guess: {sub_id}/{visit}')
            msg = "Unable to extract pattern from subject label or lastname"
            self.reporter.append_log(old_ses_id=old_ses_id, old_ses_label=old_ses_label,
                                     old_subject_id=original_subject_id,old_subject_label = old_subject_label,
                                     msg=msg,
                                     new_ses_id="",
                                     new_ses_label="",
                                     new_subject_id="",
                                     new_subject_label="",
                                     moved_sub=False,
                                     moved_ses=False)

            return None

        # Set up the new session and subject label
        new_session_label = f"{sub_id}_{visit}"
        new_subject_label = sub_id


        # See if a subject with the new ID exists.  If not, simply rename this one.
        subject = find_or_create_subject(self.fw_client, session, new_subject_label)
        if not subject:
            msg += f"multiple subjects with {new_subject_label} found. "

            self.reporter.append_log(old_ses_id=old_ses_id,
                                     new_ses_id="",
                                     old_ses_label=old_ses_label,
                                     new_ses_label="",
                                     old_subject_id=original_subject_id,
                                     new_subject_id="",
                                     old_subject_label=new_subject_label,
                                     new_subject_label=new_subject_label,
                                     moved_sub=False,
                                     moved_ses=False,
                                     msg=msg)

            return None

        new_subject_id = subject.id
        new_subject_label = subject.label

        # See if a session with the new session label exists in the previously located subject.
        # The function will throw errors and skip if there are issues.
        session, subject_raw = find_or_create_session(self.fw_client, subject, session, new_session_label, new_subject_label)
        if not session:
            msg += f"multiple sessions with {new_session_label} found. "

            self.reporter.append_log(old_ses_id=old_ses_id,
                                     new_ses_id="",
                                     old_ses_label=old_ses_label,
                                     new_ses_label="",
                                     old_subject_id=original_subject_id,
                                     new_subject_id=new_subject_id,
                                     old_subject_label=new_subject_label,
                                     new_subject_label=new_subject_label,
                                     moved_sub=original_subject_id != new_subject_id,
                                     moved_ses=False,
                                     msg=msg)


            return None

        if LOG_ONLY:
            print(f'would update subject_raw on session: {subject_raw}')
        else:
            session.update_info({'subject_raw': subject_raw})


        if subject.id != original_subject_id and DELETE_EMPTY:
            # Cleanup old subject:
            try:
                old_subject = self.fw_client.get_subject(original_subject_id)
                if not old_subject.files and not old_subject.sessions():
                    print(f'Removing orphan subject: {old_subject.label}')
                    if LOG_ONLY:
                        print("*delete*")
                    else:
                        self.fw_client.delete_subject(old_subject.id)

            except ApiException as e:
                print('Already removed')



        # This is all just for logging
        new_ses_id = session.id
        new_ses_label = session.label

        if LOG_ONLY:
            new_subject_label = sub_id

        self.reporter.append_log(old_ses_id = old_ses_id,
            new_ses_id = new_ses_id,
            old_ses_label = old_ses_label,
            new_ses_label = new_ses_label,
            old_subject_id = original_subject_id,
            new_subject_id = new_subject_id,
            old_subject_label = new_subject_label,
            new_subject_label = new_subject_label,
            moved_sub = original_subject_id != new_subject_id,
            moved_ses = new_ses_id != old_ses_id,
            msg = msg)


    def curate_analysis(self, analysis: flywheel.AnalysisOutput):
        pass

    def curate_file(self, file_: flywheel.FileEntry):
        pass




def find_or_create_subject(fw_client, session, new_subject_label):
    """
        We actually never create a new subject here - we either rename the current one,
        or we move the sessions to a pre-existing one.

    """

    # Look for a subject with the patient ID we've identified.
    find_subject = fw_client.subjects.find(f'project={session.parents.project},label={new_subject_label}')
    if len(find_subject) > 1:
        # IF more than one subject with this ID exists, there is a pre-existing problem
        # It must be fixed.  This script can't do it.
        print(f'ERROR MULTIPLE SUBJECTS WITH LABEL {new_subject_label} found!')
        return None
    elif len(find_subject) == 0:
        # If we did not find any existing subjects with that ID, simply
        # rename the current subject and return it

        print(f'no subject with label {new_subject_label} found, renaming')
        print(f"fw_client.subjects.find(f'project={session.parents.project},label={new_subject_label}')")
        subject = session.subject
        if LOG_ONLY:
            print(f'would update subject label {subject.label} -> {new_subject_label}')
        else:
            subject.update({'label': new_subject_label})
    else:
        # IF we found one other subject with this label, return that subject.
        subject = find_subject[0]
        if session.subject.id == subject.id:
            print(f"Same subject, no moving")
        else:
            print(f'Will move sessions to existing subject {new_subject_label}')

    return subject.reload()


def find_or_create_session(fw_client, subject, session, new_session_label, new_subject_label):

    """
        Takes a flywheel session and subject, and a new session label to change the
        given session to.

        If a session with that label already exists in the subject,
        the acquisitions are moved over ot that existing session.
        if there is no session with "new_session_label" as the name in the
        given subject, then the entire session is simply renamed and moved.



    """


    # Initialize our subject_raw metdata dict.
    subject_raw = {'firstname': subject.firstname,
                   'lastname': subject.lastname,
                   'sex': subject.sex,
                   'ground_truth': '',
                   'curated': False}

    # NOTE that "subject" is the subject found in the previous "find subject" step.
    # This means that the subject is either:
    # 1. the original subject
    # 2. an existing subject with the same ID that this session will be moved to.


    print('checking to see if new session label exists in subject')
    find_sessions = subject.sessions.find(f'label={new_session_label}')
    if len(find_sessions) > 1:
        # If we find more than one session with this label, something has already
        # Gone wrong and must be manually addressed.
        print(f'ERROR MULTIPLE SESSIONS WITH LABEL {new_session_label} in {new_subject_label} found!')
        session = None
    elif len(find_sessions) == 0:
        # If not sessions with that name are found, we just need to move this session.
        # First update to the new label, then move to the new subject.
        # Note that if the "new" subject is still the original subject,
        # Then there is no moving as that command makes no change.
        print(f'no session with label {new_session_label} found.  Will rename and Move')
        if LOG_ONLY:
            print(f'would update session label {session.label} -> {new_session_label}')
            print(f'wound update session subject {session.subject.label} -> {subject.label}')
            subject_raw['ground_truth'] = new_session_label
            subject_raw['curated'] = True
        else:
            session.update({'label': new_session_label})

            if subject.id != session.subject.id:
                session.update({'subject': {'_id': subject.id}})

            subject_raw['ground_truth'] = new_session_label
            subject_raw['curated'] = True
            session = session.reload()

    else:
        # Otherwise, if we found one session with that name already, we have to move
        # Acquisitions over.
        new_session = find_sessions[0]
        if new_session.id == session.id:
            print('Same session, no merging')
            subject_raw['curated'] = True
            subject_raw['ground_truth'] = new_session_label
            session = new_session
            return session, subject_raw

        print(f'Session with {new_session_label} in {new_subject_label} found! to merge, set merge_existing = True')
        if MERGE_EXISTING:
            print('merging')
            # Move acquisitions
            for acq in session.acquisitions():
                if LOG_ONLY:
                    print(f'Would merge {acq.label} from {session.label} to {new_session.label}')
                else:
                    acq.upadte({'session': new_session.id})

            session = session.reload()

            # Move files
            for file in session.files:
                if LOG_ONLY:
                    print(f'Would move file {file.name}')
                else:
                    with tempfile.TemporaryDirectory() as tmpdir:
                        temp_path = Path(tmpdir)
                        temp_file = temp_path / file.name
                        session.doanload_file(file.name, temp_file)
                        new_session.upload_file(temp_file)

            new_session = new_session.reload()
            # check to see if all the files made it over and delte the old
            # Empty session.
            if DELETE_EMPTY:
                new_session_files = [f.name for f in new_session.files]
                if all([f.name in new_session_files for f in session.files]) and len(session.acquisitions()) == 0:
                    print("all files moved successfully")
                    print(f'Removing orphan session: {session.label}')
                    if LOG_ONLY:
                        print('*delete*')
                    else:
                        fw_client.delete_session(session.id)

            session = new_session
            subject_raw['curated'] = True
            subject_raw['ground_truth'] = new_session_label
        else:
            print('skipping')
            session = None

    return session, subject_raw



def get_session_label(session):
    """
        This is how the subject ID is determined:

        1. if the session label is already in "PA###_V#W#", but V and W have different numbers, we assume that this
            is a custom curation, and we leave it alone.
        2. if the subject label has a "V#W#" in the name, take that
        4. check the "PatientName" in the dicom fields of the acquisitions, if it has "V#W#", take that.
        5. Check the "StudyDescription", match to V1W1, V2W2, V3W3 if it's "Tottenham^PACCT_study",
            "Tottenham^PACCT_study_w2", "Tottenham^PACCT_study_w3"


    """

    visit = None
    msg = ""
    if session_label_patt.match(session.label):
        print('Session label already matches pattern')
        visit = visit_code.match(session.label).group('visit')
        msg += f"visit code {visit} in session label. "
        if visit[1] != visit[3]:
            msg += "Session is unique, leaving. "
            print('V code and W code mismatch, keeping and assuming custom curation')
            return visit, msg
        print('Double checking to make sure V and W are correct.')

    # First we'll check to see if the subject label as a visit in it
    sub_label = session.subject.label
    if visit_code.match(sub_label):
        print('Visit in subject label')
        visit = visit_code.match(sub_label).group('visit')
        msg += f"vist code {visit} in subject label."
        return visit, msg

    msg += f"no vist code found in labels, checking dicom headers. "
    # Otherwise we'll have to go to the dicom headers for information.
    # Currently only check the dicom header "StudyDescription"
    fields_to_check = ['PatientName', 'StudyDescription']
    for dicom_field in fields_to_check:
        extracted_value = ""
        # Load and iterate through the acquisitions
        for acq in session.acquisitions.iter():
            acq = acq.reload()
            # Find the first dicom file
            dicom_file = [f for f in acq.files if f.type == 'dicom']
            if not dicom_file:
                continue

            # Search that file for the header info
            dicom_file = dicom_file[0]
            extracted_value = dicom_file.info.get(dicom_field, "")
            if extracted_value:
                # IF the header exists, break the loop and go check that for a visit
                break

        if dicom_field == 'StudyDescription':
            # Yes, this means that if the first dicom with a filled StudyDescription field
            # DOESN'T match any of the criteria below, but another dicom would, we will
            # miss it.  There just comes a point where complexity is our enemy.

            # If it matches these criteria, we assign a visit manually.  not the best way.
            if extracted_value in ["Tottenham^PACCT_study", "Tottenham^Child"]:
                visit = "V1W1"

            elif extracted_value == "Tottenham^PACCT_study_w2":
                visit = "V2W2"

            elif extracted_value == "Tottenham^PACCT_study_w3":
                visit = "V3W3"

            if visit:
                msg += f"visit code {visit} found matching StudyDescription. "
                break


        elif dicom_field == 'PatientName':

            if isinstance(extracted_value, str) and visit_code.match(extracted_value):
                visit = visit_code.match(extracted_value).group('visit')

            if visit:
                msg += f"visit code {visit} found matching PatientName. "

                break
    if not visit:
        msg += "no visit code found. "

    return visit, msg

def get_subject_label(session):

    sub_id = None
    msg = ""
    # We first check the subject label to see if matches the pattern "PA###" (case insensitive) anywhere
    sub_label = session.subject.label
    if sub_id_code.match(sub_label):
        # If we match, we take the number out from here
        sub_id = sub_id_code.match(sub_label).group('num')
        msg += f"Found sub id {sub_id} in subject label. "

    # IF there's no match at the subject label we will go to the subject lastname and check
    elif isinstance(session.subject.lastname, str) and session_label_patt.match(session.subject.lastname):
        lastname = session.subject.lastname
        sub_id = sub_id_code.match(lastname).group('num')
        msg += f"Found sub id {sub_id} in subject lastname. "

    # If we still have no match, we need to look at dicom headers to find information.
    # Currently only looking at "PatientName" for simplicity
    else:
        msg += "Searched Dicoms for id. "
        # Load the acquisitions in the session
        for acq in session.acquisitions.iter():
            acq = acq.reload()
            # Extract dicom files
            dicom_file = [f for f in acq.files if f.type == 'dicom']
            if not dicom_file:
                continue

            dicom_file = dicom_file[0]
            # Read the info for PatientName
            patient_name = dicom_file.info.get('PatientName')
            if patient_name:
                break

        # If we get a patient name we'll try to match it to our pattern
        sub_id = sub_id_code.match(sub_label)
        if sub_id:
            msg += f"found {sub_id.group('num')} in 'PatientName' "
            sub_id = sub_id.group('num')

    # If we found something from these steps, we'll recreate the subject label with a capital PA
    if sub_id:
        sub_id = f"PA{sub_id}"
    else:
        msg += "No id found. "

    return sub_id, msg

