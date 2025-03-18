# import argparse

# parser = argparse.ArgumentParser()
# parser.add_argument(
# 	'-s',
# 	'--sources',
# 	help="IDs of sources to process, delimited by ','",
# )
# parser.add_argument(
# 	'-g',
# 	'--groups',
# 	help="groups (SourceName) to process, delimited by ','",
# )
# parser.add_argument(
# 	'-S',
# 	'--exclude-sources',
# 	help="sources to be processed except specified one, delimited by ','",
# )
# parser.add_argument(
# 	'-G',
# 	'--exclude-groups',
# 	help="groups to be processed except specified one, delimited by ','",
# )
# parser.add_argument(
# 	'-t',
# 	'--object_type',
# 	help='specify object type Dimension or Fact',
# )
# parser.add_argument(
# 	'-i',
# 	'--calling_sequence',
# 	help='run only failed items in the Control table',
# )
# parser.add_argument(
# 	'-l',
# 	'--loadfrequency',
# 	default='Daily',
# 	help='run only failed items in the Control table',
# )
# parser.add_argument(
# 	'-f',
# 	'--failed',
# 	action='store_true',
# 	help='run only failed items in the Control table',
# )
# parser.add_argument(
# 	'-d',
# 	'--delimiter',
# 	help=(
# 		'character used to specify multiple sources in "--sources" '
# 		'switch (default: ,)'
# 	),
# 	default=',',
# )
# parser.add_argument(
# 	'--list_sources',
# 	action='store_true',
# 	help='list all Source IDs available in the Control Table',
# )
# parser.add_argument(
#     '-p',
#     '--parallel',
#     action='store_true',
#     help='spawn separate process for handling each source',
# )
# parser.add_argument(
# 	'-u',
# 	'--user_agent',
# 	help="User Agent",
# 	default='terminal',
# )
# parser.add_argument(
#     '-x',
#     '--dataflowflag',
#     help="Specify the data flow flag",
# )

import random
import string
import argparse
parser = argparse.ArgumentParser()
parser.add_argument(
    '-s',
    '--sources',
    help="IDs of sources to process, delimited by ','",
)
parser.add_argument(
    '-g',
    '--groups',
    help="groups (SourceName) to process, delimited by ','",
)
parser.add_argument(
    '-S',
    '--exclude-sources',
    help="sources to be processed except specified one, delimited by ','",
)
parser.add_argument(
    '-G',
    '--exclude-groups',
    help="groups to be processed except specified one, delimited by ','",
)
parser.add_argument(
    '-t',
    '--object_type',
    help='specify object type Dimension or Fact',
)
parser.add_argument(
    '-i',
    '--calling_sequence',
    help='run only failed items in the Control table',
)
parser.add_argument(
    '-l',
    '--loadfrequency',
    default='Daily',
    help='run only failed items in the Control table',
)
parser.add_argument(
    '-f',
    '--failed',
    action='store_true',
    help='run only failed items in the Control table',
)
parser.add_argument(
    '-d',
    '--delimiter',
    help=(
        'character used to specify multiple sources in "--sources" '
        'switch (default: ,)'
    ),
    default=',',
)
parser.add_argument(
    '-ls',
    '--list_sources',
    action='store_true',
    help='list all Source IDs available in the Control Table',
)
parser.add_argument(
    '-p',
    '--parallel',
    action='store_true',
    help='spawn separate process for handling each source',
)
parser.add_argument(
    '-u',
    '--user_agent',
    help="User Agent",
    default='terminal',
)
parser.add_argument(
    '-mc',
    '--model_create',
    action='store_true',
    help="Model Create"
)
parser.add_argument(
    '-df',
    '--dataflowflag',
    help="dataflowflag"
)
 