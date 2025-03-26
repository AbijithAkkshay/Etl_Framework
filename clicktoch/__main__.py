'''Move data from click schema to ClickHouse'''
import sys
import time
import concurrent.futures
import pandas as pd

from clicktoch import parser
from clicktoch.db import ClickToCH
from common_utils.logs import logger

from common_utils.onesource import ControlEntries

def main(args):
    onesource = ControlEntries()
    clicktoch = ClickToCH()
    
    records = onesource.get_control_entries(    
        None,
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
            # logger.info(f"{record.sourceschema}.{record.targettablename}")
            logger.info(f"{record.sourceschema}.{record.targetobject}")
        sys.exit(0)
    
    for i in df.sourcecallingseq.sort_values().unique():
        logger.info("Started running calling sequence %d", i)
        df_seq = df[df.sourcecallingseq == i]
       
        time_start = time.perf_counter()
#if row.targetdatabase == 'datawarehouse': (check for parellel)
        if args.parallel:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(records), 8)) as executor:
                for row in df_seq.itertuples():
                    executor.submit(clicktoch.click_to_ch, row)
        else:
            
            for row in df_seq.itertuples():
               
                # if row.targetdatabase == 'datawarehouse':
                #     #dwh_to_mssql(row)
                #     clicktoch.click_to_ch(row)
                # else:
                    clicktoch.click_to_ch(row)

        time_end = time.perf_counter()
        logger.info(f"Finished running calling sequence {i} in {time_end - time_start} seconds")


if __name__ == '__main__':
    cli_args = parser.parse_args()
    main(cli_args)
