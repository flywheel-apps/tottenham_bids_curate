import logging

import flywheel
import pandas as pd
import flywheel_gear_toolkit
from flywheel_gear_toolkit.utils import walker
from Curate_Bids_Tottenham import Curator
from flywheel_gear_toolkit.utils import curator as c


from importlib import metadata
print(metadata.version('flywheel_gear_toolkit'))


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("root")

import os
fw = flywheel.Client(os.environ['CUMC_API'])
project = fw.get_project('5cace5acb2baaf0030809b02')
my_walker = walker.Walker(project)
# context = flywheel_gear_toolkit.context
# curator_path = '/Users/davidparker/Documents/Flywheel/Clients/Columbia/Tottenham/bids-curate/Curate_Bids_Tottenham.py'
# curator = c.get_curator(context, curator_path)

bids_curator = Curator()
for container in my_walker.walk():
    bids_curator.curate_container(container)