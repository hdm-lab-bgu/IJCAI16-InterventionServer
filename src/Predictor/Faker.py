__author__ = 'eran'
import time
import MySQLdb
import datetime


def sql(user_id, city_name, country_name, project, subjects, created_at, intervention_id,cohort_id,preconfigured_id,algo_info):
    local_time = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    conn = MySQLdb.connect(host="localhost", user="root", passwd="9670", db="streamer")
    cursor = conn.cursor()
    try:
        cursor.execute("""INSERT INTO stream (user_id,project,subjects,created_at,country_name,city_name,local_time,intervention_id,preconfigured_id,cohort_id,algo_info) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                       (user_id,project,subjects,created_at,country_name,city_name,local_time,intervention_id,preconfigured_id,cohort_id,algo_info))
        conn.commit()
    except MySQLdb.Error as e:
        print e
        conn.rollback()
    conn.close()


def stream():
    while True:
        theTime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        sql("1","fake_city","fake_country","fake_project","fake_subjects",theTime,"1","fake","fake","fake")
        print "{0} - User:{1} Record added.\n".format(theTime, "1")
        time.sleep(5)
        theTime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        sql("2","fake_city2","fake_country2","fake_project2","fake_subjects2",theTime,"2","fake2","fake2","fake2")
        print "{0} - User:{1} Record added.\n".format(theTime, "2")
        time.sleep(5)

if __name__ == "__main__":
    stream()
