import logging

import flywheel
from flywheel_gear_toolkit.utils import walker
from Curate_Bids_Tottenham import Curator
import os
from importlib import metadata




"""
To Run this code:

1a. Add your flywheel api key as an environmental variable named "CUMC_API":

    ```
    > CUMC_API='my_api_key_string'
    > export CUMC_API
    ```
1b. OR replace line 41 with your api key:
    ` fw = flywheel.Client('my_api_key_string') `

2. Ensure that the project ID in line 42 is correct for the project you want to run this on 

3. open a terminal window in this directory

4. Run the command:
    ` python RunCurator.py `
    
NOTE that this was built and tested with python 3.8

"""

print(metadata.version('flywheel_gear_toolkit'))


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("root")

fw = flywheel.Client(os.environ['CUMC_API'])
project = fw.get_project('5cace5acb2baaf0030809b02')
my_walker = walker.Walker(project)
# context = flywheel_gear_toolkit.context
# curator_path = '/Users/davidparker/Documents/Flywheel/Clients/Columbia/Tottenham/bids-curate/Curate_Bids_Tottenham.py'
# curator = c.get_curator(context, curator_path)

bids_curator = Curator()
for container in my_walker.walk():
    bids_curator.curate_container(container)