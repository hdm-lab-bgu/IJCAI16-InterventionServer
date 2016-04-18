__author__ = 'eran'
import MySQLdb
import os
import sys
import requests
import json
from dis_predictor import dis_predictor
import datetime
import logging
from logging.handlers import RotatingFileHandler
from Config import Config as mconf
cnf = mconf.Config().conf
from contextlib import closing


log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')
#logFile = '/home/ise/Logs/predictor.log'
logFile = cnf['predLog']

my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=1*1024*1024, backupCount=50, encoding=None, delay=0)
my_handler.setFormatter(log_formatter)
my_handler.setLevel(logging.INFO)

app_log = logging.getLogger('root')
app_log.setLevel(logging.INFO)
app_log.addHandler(my_handler)

Alg = None

def sql(query,params):
    # connect
    conn = MySQLdb.connect(host=cnf['host'], user=cnf['user'], passwd=cnf['password'], db=cnf['db'])
    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(query,params)
            conn.commit()
        except MySQLdb.Error as e:
            app_log.info("Unable to connect to DB.\n")
            app_log.info(e)
            conn.rollback()
    conn.close()

def sqlGet(query,params):
    # connect
    conn = MySQLdb.connect(host=cnf['host'], user=cnf['user'], passwd=cnf['password'], db=cnf['db'])
    rows = None
    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(query,params)
            conn.commit()
            rows = cursor.fetchall()
        except MySQLdb.Error as e:
            app_log.info("Unable to connect to DB.\n")
            app_log.info(e)
            conn.rollback()
        except MySQLdb.ProgrammingError as ex:
            if cursor:
                print "\n".join(cursor.messages)
                # You can show only the last error like this.
                # print cursor.messages[-1]
            else:
                print "\n".join(conn.messages)
                # Same here you can also do.
                # print self.db.messages[-1]
            conn.rollback()
    conn.close()
    return rows

def tmprint(txt):
    local_time = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    print "{0} - {1}".format(local_time, txt)

def main():
    try:
        global Alg
        Alg = dis_predictor()
        app_log.info("Algorithm initialization successful.\n")
    except:
        app_log.info("Error, unable to start algorithm.\n")
        app_log.info(sys.exc_info())
        app_log.info(os.getcwd())
        return
    while True:
        try:
            prediction_loop()
        except:
            app_log.info("Prediction Loop failed.\n")
            app_log.info(sys.exc_info())
            continue


def prediction_loop():
        local_time = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        app_log.info("predicting for %s"%local_time)
        while True:
            try:
                sql("update stream set intervention_id=%s where user_id=%s", ("-1", "Not Logged In"))
                rows = sqlGet("SELECT id,user_id,created_at FROM stream WHERE intervention_id IS NULL and local_time>=%s",[local_time])
                if len(rows) == 0:
                    continue
                for row in rows:
                    try:
                        id = row[0]
                        user_id = row[1]
                        created_at = row[2]
                        if user_id == "Not Logged In":
                            continue

                        created_at = created_at.strftime('%Y-%m-%d %H:%M:%S')
                        incentive = Alg.intervene(user_id, created_at)
                        if incentive[0] > 0:
                            intervention_id = send_intervention_for_user(user_id,incentive[0],incentive[1])
                            app_log.info("User:" + user_id + " intervention_id: " + intervention_id + " preconfigured_id:%s cohort_id: %s"%(incentive[0], incentive[1]))
                            sql("update stream set preconfigured_id=%s,cohort_id=%s,algo_info=%s,intervention_id=%s where id=%s", (incentive[0],incentive[1],incentive[2],intervention_id, id))
                            app_log.info("done execute\n")
                        else:
                            intervention_id = "None"
                            app_log.info("User:" + user_id + " intervention_id: " + intervention_id + " preconfigured_id:%s cohort_id: %s"%(incentive[0], incentive[1]))
                            sql("update stream set preconfigured_id=%s,cohort_id=%s,algo_info=%s,intervention_id=%s where id=%s", (incentive[0],incentive[1],incentive[2],intervention_id, id))
                            app_log.info("done execute\n")
                    except:
                        app_log.info("Error: ")
                        app_log.info(sys.exc_info())
                        sql("update stream set intervention_id=%s where id=%s", ("Failed", row[0]))
                        continue
            except:
                app_log.info("Error2: ")
                app_log.info(sys.exc_info())
                local_time = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                continue



def send_intervention_for_user(user_id,preconfigured_id,cohort_id):
    with requests.Session() as c:
        url = "http://experiments.zooniverse.org/users/" + user_id + "/interventions"
        #url = "http://experiments.staging.zooniverse.org/users/" + user_id + "/interventions"
        payload = {
            "project": "galaxy_zoo",
            "intervention_type": "prompt user about talk",
            "text_message": "please return",
            "cohort_id": cohort_id,
            "time_duration": 120,
            "presentation_duration": 15,
            "intervention_channel": "web message",
            "take_action": "after_next_classification",
            "preconfigured_id": preconfigured_id,
            "experiment_name": "Zooniverse-MSR-BGU GalaxyZoo Experiment 2"
        }

        request = c.post(url, data=payload)
        # app_log.info('send_intervention_for_user(' + userId + '):' + request.content
        try:
            if request.status_code == 500:
                content = json.loads(request.content)
                app_log.info('ERROR: Invalid Parameters Sent, send_intervention_for_user(' + user_id + '):'+content)
                return "-1"

            content = json.loads(request.content)
            intervention_id = content['intervention']['id']
            app_log.info('SUCCESS send_intervention_for_user(' + user_id + '):' + intervention_id)
            return intervention_id
        except:
            app_log.info('ERROR: send_intervention_for_user(' + user_id + '):')
            app_log.info(sys.exc_info())
            return "-1"





if __name__ == "__main__":
    main()
