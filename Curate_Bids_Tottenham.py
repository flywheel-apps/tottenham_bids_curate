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
DELETE_EMPTY = False


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

    def curate_session(self, session: flywheel.Session):
        session = session.reload()
        if session.id in ["61be4e5cd68321a1a8ff918e", "61be4e5cd68321a1a8ff918e"]:
            print("no way")
            return None

        if session.info.get('subject_raw', {}).get('curated', False):
            return None

        number_patt = re.compile('.*([Pp][Aa]).*(?P<num>[\d]{3,}).*')
        visit_patt = re.compile('.*(?P<visit>[Vv][\d]+_?[Ww][\d]+$)')
        session_label_patt = re.compile('^(?P<sub_code>PA[\d]*)_V[\d]W[\d]$')

        sub_label = session.subject.label
        if session_label_patt.match(sub_label):
            sub_id = session_label_patt.match(sub_label).group('sub_code')
            visit = visit_patt.match(sub_label).group('visit')

        elif session_label_patt.match(session.subject.lastname):
            lastname = session.subject.lastname
            sub_id = session_label_patt.match(lastname).group('sub_code')
            visit = visit_patt.match(lastname).group('visit')
        else:
            sub_id = None
            visit = None
            print(f'Can not Decipher subject/session from {session.subject.label}')
            return None

        new_session_label = f"{sub_id}_{visit}"
        new_subject_label = sub_id

        print(f'new session label {new_session_label}')

        original_subject_id = session.subject.id

        subject = find_or_create_subject(self.fw_client, session, new_subject_label)
        if not subject:
            return None

        session, subject_raw = find_or_create_session(self.fw_client, subject, session, new_session_label, new_subject_label)
        if not session:
            return None

        session.update_info({'subject_raw': subject_raw})
        if subject.id != original_subject_id:
            # Cleanup old subject:
            old_subject = self.fw_client.get_subject(original_subject_id)
            if not old_subject.files and not old_subject.sessions():
                print(f'Removing orphan subject: {old_subject.label}')
                self.fw_client.delete_subject(old_subject.id)



    def curate_analysis(self, analysis: flywheel.AnalysisOutput):
        pass

    def curate_file(self, file_: flywheel.FileEntry):
        pass








def find_or_create_subject(fw_client, session, new_subject_label):
    find_subject = fw_client.subjects.find(f'project={session.parents.project},label={new_subject_label}')
    if len(find_subject) > 1:
        print(f'ERROR MULTIPLE SUBJECTS WITH LABEL {new_subject_label} found!')
        return None
    elif len(find_subject) == 0:
        print(f'no subject with label {new_subject_label} found, renaming')
        subject = session.subject
        subject.update({'label': new_subject_label})
        subject.firstname = session.subject.firstname
        subject.lastname = session.subject.lastname
        subject.sex = session.subject.sex
        subject = subject.reload()
    else:
        subject = find_subject[0]

    return subject


def find_or_create_session(fw_client, subject, session, new_session_label, new_subject_label):

    subject_raw = {'firstname': subject.firstname,
                   'lastname': subject.lastname,
                   'sex': subject.sex,
                   'ground_truth': '',
                   'curated': False}

    print('checking to see if new session label exists in subject')
    find_sessions = subject.sessions.find(f'label={new_session_label}')
    if len(find_sessions) > 1:
        print(f'ERROR MULTIPLE SESSIONS WITH LABEL {new_session_label} in {new_subject_label} found!')
        session = None
    elif len(find_sessions) == 0:
        print(f'no session with label {new_session_label} found.  Will rename and Move')
        session.update({'label': new_session_label})
        session.update({'subject': subject.id})
        subject_raw['ground_truth'] = new_session_label
        subject_raw['curated'] = True

    else:
        new_session = find_sessions[0]

        print(f'Session with {new_session_label} in {new_subject_label} found! to merge, set merge_existing = True')
        if MERGE_EXISTING:
            print('merging')
            for acq in session.acquisitions():
                acq.upadte({'session': new_session.id})

            session = session.reload()

            for file in session.files:
                with tempfile.TemporaryDirectory() as tmpdir:
                    temp_path = Path(tmpdir)
                    temp_file = temp_path / file.name
                    session.doanload_file(file.name, temp_file)
                    new_session.upload_file(temp_file)

            new_session = new_session.reload()

            if DELETE_EMPTY:
                new_session_files = [f.name for f in new_session.files]
                if all([f.name in new_session_files for f in session.files]):
                    print("all files moved successfully")
                    print(f'Removing orphan session: {session.label}')
                    fw_client.delete_session(session.id)

            session = new_session
            subject_raw['curated'] = True
        else:
            print('skipping')
            session = None

    return session, subject_raw