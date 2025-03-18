
from collections import namedtuple
from sqlalchemy import text
import connection
from common_utils.logs import logger
# from stagingtodwh import parser, etl_batch_id

# cli_args = parser.parse_args()
class ControlEntries:
    def __init__(self):
        self.engine = connection.new_db_connection('postgresql')
        self.Record = None
    
    def get_control_entries(self, dataflowflag=None, sources=None, groups=None, exclude_sources=None, exclude_groups=None, object_type=None, calling_sequence=None, load_frequency=None, failed_only=False):
        
        query = self._control_table_query(sources, groups, exclude_sources, exclude_groups, object_type, calling_sequence, load_frequency, failed_only)
        params = self._build_query_params(dataflowflag, sources, groups, exclude_sources, exclude_groups, object_type, calling_sequence, load_frequency)
        
        print(query)
        print(params)

        with self.engine.connect() as conn:
            result = conn.execute(text(query), params)
            rows = result.fetchall()
            self.Record = namedtuple('Record', result.keys())
            data = [self.Record(*row) for row in rows]
            
        return data
    
    def _build_query_params(self, dataflowflag, sources, groups, exclude_sources, exclude_groups, object_type, calling_sequence, load_frequency):
        params = {'dataflowflag': (dataflowflag,) if dataflowflag else ('SLVtoCH','GLDtoCH'), 'load_frequency': load_frequency}
        
        if sources:
            for i, source in enumerate(sources):
                params[f'source{i}'] = source
        if exclude_sources:
            for i, source in enumerate(exclude_sources):
                params[f'exclude_source{i}'] = source
        if groups:
            for i, group in enumerate(groups):
                params[f'group{i}'] = group
        if exclude_groups:
            for i, group in enumerate(exclude_groups):
                params[f'exclude_group{i}'] = group
        if object_type:
            for i, obj in enumerate(object_type):
                params[f'object_type{i}'] = obj
        if calling_sequence:
            for i, seq in enumerate(calling_sequence):
                params[f'calling_sequence{i}'] = seq
        
        return params
    
    def _control_table_query(self, sources=None, groups=None, exclude_sources=None, exclude_groups=None, object_type=None, calling_sequence=None, load_frequency=None, failed_only=False) -> str:
        query = '''
            SELECT
                header.sourceid,
                detail.sourcename,
                detail.sourcetype,
                detail.sourceobject,
                detail.sourceschema,
                header.depsource,
                detail.loadtype,
                detail.loadfrequency,
                header.connectionstr,
                header.objecttype,
                header.sourcedelimiter,
                detail.sourcequery,
                detail.etllastrundate,
                detail.targetname,
                detail.targetobject,
                header.sourcecallingseq,
                detail.dataflowflag,
                detail.latestbatchid,
                detail.targetschemaname,
                detail.targetprocedurename,
                detail.intervaldays
            FROM ods.controlheader header
            JOIN ods.controldetail detail
                ON header.SourceId = detail.SourceId
            WHERE detail.DataflowFlag IN :dataflowflag
            AND detail.IsReadyForExecution = 1
            AND detail.Isapplicable = 1'''

        if sources:
        # Modify the IN clause to work with SQL Server
            source_placeholders = ', '.join([f':source{i}' for i in range(len(sources))])
            query += f"\n\tAND detail.SourceId IN ({source_placeholders})"
            
        if load_frequency:
            query += "\n\tAND detail.LoadFrequency = :load_frequency"

        if exclude_sources:
            # Dynamically generate placeholders for each exclude_source in the list
            exclude_source_placeholders = ', '.join([f':exclude_source{i}' for i in range(len(exclude_sources))])
            query += f"\n\tAND detail.SourceId NOT IN ({exclude_source_placeholders})"

        if groups:
            # Dynamically generate placeholders for each group in the list
            group_placeholders = ', '.join([f':group{i}' for i in range(len(groups))])
            query += f"\n\tAND header.SourceId IN (SELECT DISTINCT SourceId FROM ods.sourcegroupingdtl WHERE schemaname='dwh' AND SourceGroup IN ({group_placeholders}))"

        if exclude_groups:
            # Dynamically generate placeholders for each exclude_group in the list
            exclude_group_placeholders = ', '.join([f':exclude_group{i}' for i in range(len(exclude_groups))])
            query += f"\n\tAND header.SourceId NOT IN (SELECT DISTINCT SourceId FROM ods.sourcegroupingdtl WHERE schemaname='dwh' AND SourceGroup IN ({exclude_group_placeholders}))"

        if object_type:
            # Dynamically generate placeholders for each object_type in the list
            object_type_placeholders = ', '.join([f':object_type{i}' for i in range(len(object_type))])
            query += f"\n\tAND header.ObjectType IN ({object_type_placeholders})"

        if calling_sequence:
            # Dynamically generate placeholders for each calling_sequence in the list
            calling_sequence_placeholders = ', '.join([f':calling_sequence{i}' for i in range(len(calling_sequence))])
            query += f"\n\tAND header.SourceCallingSeq IN ({calling_sequence_placeholders})"

        if failed_only:
            query += "\n\tAND detail.FlowStatus = 'Failed'"       
            

        # Order by `Id`
        query += '\n\tORDER BY header.Id'
        

        return query
