import flywheel
import os
from flywheel_gear_toolkit.utils.curator import HierarchyCurator
import copy
import datetime
import re


class Curator(HierarchyCurator):
    def __init__(self, **kwargs):
        # Install backoff
        super().__init__(**kwargs)
        # curate depth first
        self.config.depth_first = True
        #self.fw_client = self.context.client
        self.fw_client = flywheel.Client(os.environ['CUMC_API'])



    def curate_project(self, project: flywheel.Project):
        pass
    def curate_subject(self, subject: flywheel.Subject):
        #pass
        subject = subject.reload()
        subjects = None
        session_label_patt = re.compile('^(?P<sub_code>PA[\d]*)_V[\d]W[\d]$')
        session = subject.sessions.find_first()
        if not session:
            print(f'No session for subject {subject.label}')
            return

        if not session_label_patt.match(session.label):
            print('ERROR: {} does not match the pattern for PACCT sessions'.format(
                session.label
            ))
            print(session.label)
            print('Please try updating session label first.')
            return None
        else:
            new_subject_code = session_label_patt.match(session.label).group('sub_code')
        if new_subject_code == session.subject.code:
            print('Session {} has the correct label {}'.format(session.label,
                                                               new_subject_code))
            return None

        query = 'project={},code={}'.format(session.project, new_subject_code)
        subjects = fw_client.subjects.find(query)
        if subjects:
            print('Moving {} to existing subject {}'.format(session.label,
                                                            new_subject_code))
            subject = subjects[0]
        else:
            print('Creating new subject {} for session {}'.format(new_subject_code,
                                                                  session.label))
            print("subject = fw_client.get(session.project).add_subject(code=new_subject_code)")
            subject = fw_client.get(session.project).add_subject(code=new_subject_code)
        former_subject = fw_client.get(session.subject.id)
        update = session.update({'subject': {'_id': subject.id}})
        print("update = session.update({'subject': {'_id': subject.id}})")
        print(subject.id)
        update = None
        former_subject = former_subject.reload()
        if not former_subject.files and not former_subject.sessions():
            print('Removing orphan subject: {}'.format(former_subject.code))
            fw_client.delete_subject(former_subject.id)
        return update


    def curate_session(self, session: flywheel.Session):
        #pass
        session = session.reload()
        print(session.label)
        print(session.info)
        # Set up some patterns for checking labels
        number_patt = re.compile('.*([Pp][Aa]).*(?P<num>[\d]{3,}).*')
        visit_patt = re.compile('.*(?P<visit>[Vv][\d]+_?[Ww][\d]+$)')
        session_label_patt = re.compile('^PA[\d]*_V[\d]W[\d]$')
        # Get the session object
        if session.id == '61be4e5cd68321a1a8ff918e':
            print('no way')
            return None

        # Use lastname to determine code and visit
        lastname = session.info.get('subject_raw',{}).get('lastname')
        if not lastname:
            print(f'No lastname for {session.id}')
            return

        # If cant find the code, return None
        if not number_patt.match(lastname):
            print(
                'Error: session PatientName {} '
                'does not contain PA followed by a 3-digit number.\n'
                '{}\n'.format(lastname, session.label)
            )
            return None
        else:
            subject_code = 'PA' + number_patt.match(lastname).group('num')
        # If it's wave 2, the visit string will be in the label
        if visit_patt.match(lastname):
            visit = visit_patt.match(lastname).group('visit')
            visit = visit.replace('_', '')
        # Assume V1W1 if no matches on lastname
        else:
            visit = 'V1W1'
        session_label = '_'.join([subject_code, visit])
        if subject_code not in session.subject.code.upper():
            print(
                'Error: session PatientName {} does not '
                'match subject code {} \n'
                '{} \n'.format(lastname, session.subject.code,
                               session.label)
            )
            return None
        else:
            if not session.label == session_label:
                print(
                    'Updating session label {} to {}'.format(session.label, session_label)
                )
                assert session_label_patt.match(session_label)
                print("update = session.update(label=session_label)")
                print(session_label)
                update = session.update(label=session_label)
                update=None
                return update
            else:
                print('session label is already {}'.format(session_label))


    def curate_acquisition(self, acquisition: flywheel.Acquisition):
        pass
    def curate_analysis(self, analysis: flywheel.AnalysisOutput):
        pass

    def validate_file(self, file_: flywheel.FileEntry):

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


    def curate_file(self, file_: flywheel.FileEntry):



        if not self.validate_file(file_):
            print(f"Not curating {file_.name}")
            return


        parent = file_.parent
        parent = parent.reload()
        file_new = [f for f in parent.files if f.id == file_.id][0]

        if not file_new.info.get('SeriesDescription'):
            print(f'No Series description for {file_new.parent.parents.session} {file_new.parent.label} {file_new.name}')
            print(file_new.info)

        info = copy.deepcopy(file_new.info)

        if info['SeriesDescription'] != 'MoCoSeries':
            return
        #print(info)
        #print(file_new.name)

        foruid = info.get('FrameOfReferenceUID')
        image_type = info.get('ImageType')
        if isinstance(image_type, str):
            image_type = string_to_list_honestlywtf(file_new, image_type)
            print('modifying')
            print(file_.name)
            print(file_.parent.label)
            print(file_.parent.id)

        #return

        image_type.pop(image_type.index('MOCO'))
        image_type.pop(image_type.index('DERIVED'))
        new_image_type = ['ORIGINAL']
        new_image_type.extend(image_type)

        new_image_type = '[' +', '.join(new_image_type) + ']'
        content_time = file_new.parent.timestamp

        current_inc = 0

        results = []

        while not results:
            current_inc += 1

            if current_inc > 5*60:
                print("No matching files found within 5 min of:")
                print(f"ses: {file_new.parent.session} acq: {file_new.parent.label} file: {file_new.name}")
                return -1

            min_time = content_time - datetime.timedelta(seconds=current_inc)
            max_time = content_time + datetime.timedelta(seconds=current_inc)

            print(f"Searcing for files acquired near {file_new.parent.label} +/- {current_inc} second")

            query = f"file.info.FrameOfReferenceUID = {foruid} AND " \
                    f"file.info.ImageType IN {new_image_type} AND " \
                    f"session._id = '{file_new.parent.parents.session}' AND " \
                    f"acquisition.timestamp >= {min_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')} AND " \
                    f"acquisition.timestamp <= {max_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')} AND " \
                    f"NOT file.info.ImageType IN [DERIVED, MOCO]"
            #print(query)
            results = self.fw_client.search({'structured_query': query, 'return_type': 'file'})

            if not results:
                print(query)

        files = [ff for f in results for ff in self.fw_client.get_acquisition(f.parent['_id']).files if ff.name == f.file.name]

        if len(results) > 1:
            print('More than one file found in results')
            print(f'ses: {files[0].parent.session}')
            print([f.parent.label for f in files])
            return

        file_match = files[0]
        series_name = file_match.info.get('SeriesDescription')
        new_name = f"{series_name}-MoCoSeries"

        old_name = file_new.parent.label
        if old_name != new_name:
            print(f'WARNING! Old name {old_name} does not match new name {new_name}')
            print("file_.update_info({'old_acquisition_label': old_name})")
            print(old_name)
            file_new.update_info({'old_acquisition_label': old_name})

            print("Would Run file_.parent.update({'label': new_name})")
            print(new_name)
            file_new.parent.update({'label': new_name})
        print('Updating curator key')
        print(file_new.update_info({'Hierarchy_Curated':True}))





def string_to_list_honestlywtf(file_new, string):
    string = string[string.find('[')+1:string.find(']')]
    string = string.replace("'","")
    string_list = string.split(', ')
    file_new.update_info({'ImageType': string_list})
    return string_list

