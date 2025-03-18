'''Move data from dwh schema to click schema'''
import sys
import time
import concurrent.futures
import pandas as pd
from common_utils.onesource import ControlEntries
from silvertogold import parser
from silvertogold.db import CallSP
from common_utils.logs import logger

def main(args):

    onesource = ControlEntries()
    sp_caller = CallSP()
    records = onesource.get_control_entries(
        'SLVtoGLD',
        args.sources and args.sources.split(args.delimiter),
        args.groups and args.groups.split(args.delimiter),
        args.exclude_sources and args.exclude_sources.split(args.delimiter),
        args.exclude_groups and args.exclude_groups.split(args.delimiter),
        args.object_type and args.object_type.split(args.delimiter),
        args.failed,
    )

    df = pd.DataFrame(records)
    
    if df.empty:
        logger.info("No entries in records")
        sys.exit(0)

    if args.user_agent == 'airflow':
        filt = (df.loadfrequency == 'once') & (df.loadenddatetime.dt.strftime('%Y-%m-%d') == time.strftime('%Y-%m-%d'))
        df = df[~filt]
        if df.empty:
            logger.info("No entries in records")
            sys.exit(0)

    logger.info("%d entries in records", len(records))

    if args.list_sources:
        for record in records:
            logger.info(record.objectname)
        sys.exit(0)

    for i in df.sourcecallingseq.sort_values().unique():
        logger.info("Started running calling sequence %d", i)
        df_seq = df[df.sourcecallingseq == i]

        time_start = time.perf_counter()

        if args.parallel:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(records), 4)) as executor:
                for row in df_seq.itertuples():
                    executor.submit(sp_caller.call_sp, row)
        else:
            for row in df_seq.itertuples():
                
                sp_caller.call_sp(row)

        time_end = time.perf_counter()
        logger.info(f"Finished running calling sequence {i} in {time_end - time_start} seconds")

if __name__ == '__main__':
    cli_args = parser.parse_args()
    try:
        main(cli_args)
    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)
        sys.exit(1)
