import mysql.connector
from mysql.connector import Error
import requests
import json
from python_main_config import read_main_config
import logging as log
import time
class StatusParams:
    not_started = 'not_started'
    pending = 'pending'
    migrate_failure = 'migrate_failure'
    file_not_found = 'file_not_found'
    done = 'done'

def execute_sql(sql, query_type):
    main_config = read_main_config()
    print('### Connecting to MySQL database... ###')
    conn = mysql.connector.connect(
        host= main_config['host'],
        database= main_config['database'],
        user= main_config['user'],
        password= main_config['password'],
    )

    try:
        cursorObject = conn.cursor()

        print("### Connected to db! ###")
        cursorObject.execute(sql)
        print(sql)
        output = ""
        err_reason = ""
        if query_type == "get":
            output = cursorObject.fetchall()
        else:
            conn.commit()
            output = "{0} record(s) affected".format(cursorObject.rowcount)
            print(cursorObject.rowcount, "record(s) affected")
    except Exception as err:
        print("###### DB Error !!!! ######",err)
        err_reason = err
    finally:
        print("db closed!")
        conn.close()

    if output and not err_reason:
        print("db sql execute success!!")
        return {"success": True, "result": output, "error": ""}
    else:
        print("db sql execute error", err_reason)
        return {"success": False, "result": "", "error": "SQL failed during execution!, Reason: {0}".format( err_reason )}

def connect():
    """ Connect to MySQL database """
    main_config = read_main_config()
    conn = None
    try:
        print('### get API auth token ... ###')
        auth_data = {'username': main_config['migration_auth_user'],'password': main_config['migration_auth_pass']}
        auth_url_host = main_config['auth_api_host']

        get_auth = requests.post(auth_url_host, json=auth_data)
        auth_access_token = False
        if get_auth.ok:
            print("\n### auth status_code: {0} ###".format(get_auth.status_code))
            get_auth.json()["access_token"]
            auth_access_token = get_auth.json()["access_token"]
        print('### end API auth token! ###')
        if get_auth.ok:
            ##### SQL selecting query #####
            query ="SELECT aid FROM attachs WHERE status NOT IN ('{}','{}')".format(StatusParams().done, StatusParams().file_not_found)
            myresult = execute_sql(query, 'get')
            if myresult['success']:
                myresult = myresult['result']
            else:
                raise Exception(myresult['error'])

            print(len(myresult))
            xxx = 0
            for x in myresult:
                xxx += 1
                print('\n\n########### {0} ###########'.format(x[0]))
                new_status = StatusParams().pending
                query1 ="UPDATE attachs SET status = '{}' WHERE aid = '{}'".format(new_status, x[0])
                outputmsql = execute_sql(query1, 'update')
                print("output of mysql update: ",outputmsql)
                if outputmsql['success']:
                    outputmsql = outputmsql['result']
                    print(outputmsql)
                else:
                    print("an error occured during the process: ", outputmsql)
                    raise Exception(myresult['error'])

                checke_xistence_tktmedia_api_host = main_config['migration_api_host'] + x[0]
                checke_existence_attached_file = requests.get(checke_xistence_tktmedia_api_host)
                if checke_existence_attached_file.ok and not checke_existence_attached_file.history:
                    migrate_url_host = main_config['migration_api_host'] + x[0] + "/transfer?configurationKey=tickets3attachments"
                    headers = {'Authorization': 'Bearer ' + auth_access_token}
                    put_migrate_file = requests.put(migrate_url_host, headers=headers)
                    time.sleep(5)
                    print("### API put_migrate_file status:                 ",put_migrate_file.status_code)
                    print("### API put_migrate_file body:                   ",put_migrate_file.content)
                    print("### API put_migrate_file history:                ",put_migrate_file.history and put_migrate_file.history[0].url)
                    print("### API put_migrate_file new url:                ",put_migrate_file.url)
                    print("### API put_migrate_file is redirected:          ",put_migrate_file.is_redirect)
                    print('### API migrated file! ###')
                    print('### API confirming the file migration ... ###')
                    tktmedia_api_host = main_config['migration_api_host'] + x[0]
                    get_attached_file = requests.get(tktmedia_api_host)
                    if get_attached_file.ok and not get_attached_file.history:
                        print('### API file existence confirmed ... ###')
                        new_status = StatusParams().migrate_failure
                    elif get_attached_file.ok and get_attached_file.history:
                        new_status = StatusParams().done

                    else:
                        new_status = StatusParams().file_not_found

                    query1 ="UPDATE attachs SET status = '{}' WHERE aid = '{}'".format(new_status, x[0])
                    outputmsql = execute_sql(query1, 'update')
                    print("output of mysql update: ",outputmsql)
                    if outputmsql['success']:
                        outputmsql = outputmsql['result']
                        print(outputmsql)
                    else:
                        print("an error occured during the process: ", outputmsql)
                        raise Exception(myresult['error'])

                    print("\n\n")

                    print("### API get_attached_file status:                 ",get_attached_file.status_code)
                    # print("### API get_attached_file body:                   ",get_attached_file.json())
                    print("### API get_attached_file history:                ",get_attached_file.history and get_attached_file.history[0].url)
                    print("### API get_attached_file new url:                ",get_attached_file.url)
                    print("### API get_attached_file prev url:               ",get_attached_file.headers)
                    print("### API get_attached_file is redirected:          ",get_attached_file.is_redirect)
                elif checke_existence_attached_file.ok and checke_existence_attached_file.history:
                    new_status = StatusParams().done
                    time.sleep(3)
                else:
                    new_status = StatusParams().file_not_found

                print("### progress status: ", new_status)

                print("\n\n")

                print("### API checke_existence_attached_file status:        ",checke_existence_attached_file.status_code)
                # print("### API checke_existence_attached_file body:        ",checke_existence_attached_file.json())
                print("### API checke_existence_attached_file history:       ",checke_existence_attached_file.history and checke_existence_attached_file.history[0].url)
                print("### API checke_existence_attached_file new url:       ",checke_existence_attached_file.url)
                print("### API checke_existence_attached_file is redirected: ",checke_existence_attached_file.is_redirect)

                query1 ="UPDATE attachs SET status = '{}' WHERE aid = '{}'".format(new_status, x[0])
                outputmsql = execute_sql(query1, 'update')
                print("output of mysql update: ",outputmsql)
                if outputmsql['success']:
                    outputmsql = outputmsql['result']
                    print(outputmsql)
                else:
                    print("an error occured during the process: ", outputmsql)
                    raise Exception(myresult['error'])

                # test the first entry only
                if main_config['env'] == 'dev' and xxx > 2: break

            print('### Connection established! ###')

        else:
            print('### Connection failed! ###')

    except Exception as err:
        print("error occured,",err)

    finally:
        if conn is not None and conn.is_connected():
            print("### finished! and database connection closed ###")
            conn.close()

if __name__ == '__main__':
    connect()
