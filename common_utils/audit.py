# common/audit.py
import random
import string
from sqlalchemy import text
import functools
import connection
from common_utils.logs import logger
from common_utils import logs
from silvertogold import parser,etl_batch_id

cli_args = parser.parse_args()

def auditable(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        engine = connection.new_db_connection('postgresql')
        try:

            # Initialize variables for record identification
            record, classObj, nametup = None, None, None
            source_count = 0

            for i in args:
                if isinstance(i, tuple) and hasattr(i, "_fields"):  # Check if namedtuple
                    nametup = i
                    source_count += 1
                elif hasattr(i, "__class__"):  # Check if it's a class instance
                    classObj = i

            if classObj is not None and nametup is None:
                record = getattr(classObj, "record", None)  # Safely get 'record' attribute
            else:
                record = nametup

            
            # preprocess
            latestbatchid = audit_start(
                record.sourceid,
                record.targetobject,
                record.dataflowflag,
                # args and len(args[0]) or 0,
                0,
                cli_args.user_agent,
                etl_batch_id
            )
            #print(latestbatchid)
            #print(record)
            #print(function)
            # process
            record = record._replace(latestbatchid=latestbatchid)
            source_count, insert_count, update_count = function(*args, **kwargs)
            #print(source_count, insert_count, update_count)
            # postprocess
            audit_end(
                record.sourceid,
                record.targetobject,
                record.dataflowflag,
                record.latestbatchid,
                source_count,
                insert_count,
                update_count,
            )
            return source_count, insert_count, update_count
        except Exception as exc:
            err_info = logs.error_info(exc)
            logger.info('{tb}'.format(**err_info))
            logs.log_error(
                err_info,
                src=f'{function.__module__}.{function.__name__}',
                obj_type=record.objecttype,
                sourceid=record.sourceid,
                target_file=record.targetobject,
                latestbatchid=record.latestbatchid,
            )
            
            audit_error(
                record.sourceid,
                record.targetobject,
                record.dataflowflag,
                record.latestbatchid,
                function.__name__,
                f'sourcetostaging.{function.__module__}',
                -1,
                '{type}: {args}'.format(**err_info),
                exc.__traceback__.tb_lineno,
            )           
            logs.save()
    return wrapper


def audit_start(sourceid, targetobject, dataflowflag, source_count, user_agent, etl_batch_id):
   
    engine = connection.new_db_connection('postgresql')
    query = "CALL ods.usp_etlpreprocess (:sourceid, :targetobject, :dataflowflag, :sourcegroupflag, :source_count, :user_agent, :etl_batch_id, NULL)"
    params = {
        'sourceid': sourceid, 
        'targetobject': targetobject, 
        'dataflowflag': dataflowflag, 
        'sourcegroupflag': 1 if cli_args.groups else 0, 
        'source_count': source_count, 
        'user_agent': user_agent, 
        'etl_batch_id': etl_batch_id
        }
        
   
    with engine.begin() as conn:
        result = conn.execute(text(query),params)
        #conn.commit()
        #print(result.fetchone())
        latestbatchid_row = result.fetchone()
        if latestbatchid_row:
            latestbatchid = latestbatchid_row[0]
            #print(latestbatchid)
        return latestbatchid

def audit_end(sourceid, targetobject, dataflowflag, latestbatchid, source_count, insert_count, update_count):
    #engine = connections.new_db_connection('source1')
    engine = connection.new_db_connection('postgresql')
    query = "CALL ods.usp_etlpostprocess (:sourceid, :targetobject, :dataflowflag, :sourcegroupflag, :latestbatchid, :source_count, :insert_count, :update_count)"
    params = {
        'sourceid': sourceid, 
        'targetobject': targetobject, 
        'dataflowflag': dataflowflag, 
        'sourcegroupflag': 1 if cli_args.groups else 0, 
        'latestbatchid': latestbatchid, 
        'source_count': source_count, 
        'insert_count': insert_count, 
        'update_count': update_count
        }
    #print(params)
    with engine.connect() as conn:
        #with conn.begin():
        conn.execute(text(query), params)
        conn.commit()

# @logs.handle_error
def audit_error(sourceid, targetobject, dataflowflag, latestbatchid, task, package, error_id, error_desc, error_line):
    #engine = connections.new_db_connection('source1')
    engine = connection.new_db_connection('postgresql')
    query = "CALL ods.usp_etlerrorinsert (:sourceid, :targetobject, :dataflowflag, :latestbatchid, :task, :package, :error_id, :error_desc, :error_line)"
    params = {
        'sourceid': sourceid, 
        'targetobject': targetobject, 
        'dataflowflag': dataflowflag, 
        'latestbatchid': latestbatchid, 
        'task': task, 
        'package': package, 
        'error_id': error_id, 
        'error_desc': error_desc, 
        'error_line': error_line
        }
    with engine.begin() as conn:
        #with conn.begin():
        conn.execute(text(query), params)
        conn.close() 
		


# def audit_start(table_name):
#     engine = connection.new_db_connection('postgresql')
#     query = """
#     UPDATE ods.dwhtoclickcontroldtl
#     SET status='started', loadstartdatetime=NOW()::TIMESTAMP
#     WHERE dataflowflag IN ('DWtoCH', 'ClicktoCH') AND targettablename = :table_name;

#     UPDATE ods.sourcegroupingdtl
#     SET status='started', loadstartdatetime=NOW()::TIMESTAMP
#     WHERE sourceid = :table_name AND schemaname = 'CH' AND isapplicable = 1;
#     """
#     with engine.begin() as conn:
#         conn.execute(text(query), {'table_name': table_name})

# def audit_end(table_name):
#     engine = connection.new_db_connection('postgresql')
#     query = """
#     UPDATE ods.dwhtoclickcontroldtl
#     SET status='completed', loadenddatetime=NOW()::TIMESTAMP, executionflag = 0
#     WHERE dataflowflag IN ('DWtoCH', 'ClicktoCH') AND targettablename = :table_name;

#     UPDATE ods.sourcegroupingdtl
#     SET status='Completed', lastrundatetime=NOW()::TIMESTAMP, loadenddatetime=NOW()::TIMESTAMP
#     WHERE sourceid = :table_name AND schemaname = 'CH' AND isapplicable = 1;
#     """
#     with engine.begin() as conn:
#         conn.execute(text(query), {'table_name': table_name})

# def audit_error(table_name):
#     engine = connection.new_db_connection('postgresql')
#     query = """
#     UPDATE ods.dwhtoclickcontroldtl
#     SET status='failed', loadenddatetime=NOW()::TIMESTAMP
#     WHERE dataflowflag IN ('DWtoCH', 'ClicktoCH') AND targettablename = :table_name;
#     """
#     with engine.begin() as conn:
#         conn.execute(text(query), {'table_name': table_name})



# def auditable(function):
#     '''Decorator to mark status of execution of a function before and after execution'''
#     @functools.wraps(function)
#     def wrapper(*args, **kwargs):
        
#         # Initialize variables for record identification
#         record, classObj, nametup = None, None, None
#         source_count = 0

#         for i in args:
#             if isinstance(i, tuple) and hasattr(i, "_fields"):  # Check if namedtuple
#                 nametup = i
#                 source_count += 1
#             elif hasattr(i, "__class__"):  # Check if it's a class instance
#                 classObj = i

#         if classObj is not None and nametup is None:
#             record = getattr(classObj, "record", None)  # Safely get 'record' attribute
#         else:
#             record = nametup


#         table_name = record.targettablename
#         try:
#             # mark status as started
#             audit_start(table_name)

#             # run function
#             result = function(*args, **kwargs)

#             # mark status as completed
#             audit_end(table_name)

#             return result
#         except Exception as exception:
#             logger.error("%s: %s: %s", table_name, exception.__class__.__name__, exception)
#             # mark status as failed
#             audit_error(table_name)
#             engine = connection.new_db_connection('postgresql')
#             error_sp = "CALL ods.usp_etlerrorinsert(:sourceid, :targetobject, :dataflowflag, :latestbatchid, :task, :package, :error_id, :error_desc, :error_line);"
#             schema_name = record.sourceschema
#             params = {
#                 'sourceid': table_name,
#                 'targetobject': table_name, 
#                 'dataflowflag': 'ClicktoCH' if schema_name == 'click' else 'DWtoCH',
#                 'latestbatchid': 1, 
#                 'task': 'ClicktoCH' if schema_name == 'click' else 'DWtoCH', 
#                 'package': 'clicktoch.__main__.click_to_ch', 
#                 'error_id': -1, 
#                 'error_desc': str(exception),
#                 'error_line': exception.__traceback__.tb_lineno
#                 }
#             with engine.begin() as conn:
#                 conn.execute(text(error_sp), params)
#             logs.save()
#             return "Failed"
        
#     return wrapper
