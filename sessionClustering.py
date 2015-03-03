__author__ = 'JanTore'

import numpy as np
import scipy.cluster.hierarchy as hac
import MySQLdb
import datetime
from decimal import Decimal
import scipy.spatial.distance as ssd
from scipy.sparse.csgraph import _validation
import sys
import threading
import mlpy
from wordcloud import WordCloud
from os import path

class Session(object):
    id = None
    hardware_id = None
    time_of_day_start = None
    time_of_day_stop = None
    day_of_week = None
    domains = []
    recordings = []
    keywords = []
    is_visited = False
    is_clustered = False
    is_noise = False

    def __init__(self, id, hardware_id, time_of_day_start, time_of_day_stop, day_of_week, domains, recordings, keywords):
        self.id = id
        self.hardware_id = hardware_id
        self.time_of_day_start = time_of_day_start
        self.time_of_day_stop = time_of_day_stop
        self.day_of_week = day_of_week
        self.domains = domains
        self.recordings = recordings
        self.keywords = keywords

matrix = np.empty(shape=(0, 0))
distArray = np.empty(shape=(0, 0))
sessions = []

time_format = '%Y-%m-%d %H:%M:%S'

def get_database():
    with open('credentials') as f:
        credentials = [x.strip().split(':') for x in f.readlines()]
    for ip, username, password, schema in credentials:
        return MySQLdb.connect(ip, username, password, schema)

def get_users():
    db = get_database()
    cursor = db.cursor()
    sql = "SELECT * FROM user"
    try:
        cursor.execute(sql)
        users = cursor.fetchall()
        db.close()
        return users
    except:
        print "Get Users Error"
        db.close()
        return []

def create_session_group(db, sessions, user_id):
    cursor = db.cursor()
    sql = "INSERT INTO session_group(userid) VALUES ('%s')" % (user_id)
    try:
        cursor.execute(sql)
        db.commit()
        set_session_group(db, sessions, cursor.lastrowid)
        return cursor.lastrowid
    except:
        print "Create session group error", sys.exc_info()
        db.rollback()
        return False

def create_new_session_groups(groups, user_id):
    groupIds = []
    db = get_database()
    cursor = db.cursor()
    sql = "DELETE FROM session_group WHERE userid = '%s'" % user_id
    try:
        cursor.execute(sql)
        db.commit()
    except:
        print "Delete session group error", sys.exc_info()
        db.rollback()

    for group in groups:
        groupIds.append(create_session_group(db, group, user_id))
    db.close()
    return groupIds

def set_session_group(db, sessions, session_group_id):
    for session in sessions:
        cursor = db.cursor()
        sql = "UPDATE session SET session_group_id = '%d' WHERE id = '%d'" % (int(session_group_id), int(session.id))
        try:
            cursor.execute(sql)
            db.commit()
        except:
            print "Set session group error", sys.exc_info()
            db.rollback()

def get_interest_keywords(db, interestid):
    keywords = []
    cursor = db.cursor()
    sql = "SELECT * FROM interest_keyword WHERE interestid = '%d'" % interestid
    try:
        cursor.execute(sql)
        db_keywords = cursor.fetchall()
        for keyword in db_keywords:
            keywords.append(keyword[2])
        return keywords
    except:
        print "Get Interest keywords Error"

def get_interests(db, sessionid):
    cursor = db.cursor()
    sql = "SELECT * FROM interest WHERE sessionid = '%s'" % sessionid
    try:
        cursor.execute(sql)
        db_interests = cursor.fetchall()
        return db_interests
    except:
        print "Get Interests Error"

def get_domains_text(db, domainid):
    cursor = db.cursor()
    sql = "SELECT * FROM domain WHERE id = '%s'" % domainid
    try:
        cursor.execute(sql)
        domains = cursor.fetchall()
        return domains
    except:
        print "Get Domains Text Error"

def get_domains(db, sessionid):
    cursor = db.cursor()
    sql = "SELECT * FROM session_domain WHERE sessionid = '%s'" % sessionid
    try:
        cursor.execute(sql)
        session_domains = cursor.fetchall()
        domains = []
        for session_domain in session_domains:
            domains.append(get_domains_text(db, session_domain[1]))
        return domains
    except:
        print "Get Domains Error"

def convert_datetime_to_seconds_from_midnight(t):
    return (t.hour * 3600) + (t.minute * 60) + t.second + (t.microsecond / 1000000.0)

def convert_datetime_to_days_since_new_week(t):
    return t.weekday()

def get_sessions(user_id):
    db = get_database()
    cursor = db.cursor()
    sql = "SELECT * FROM session WHERE userid = '%s'" % user_id
    try:
        cursor.execute(sql)
        db_sessions = cursor.fetchall()
        sessions = []
        for db_session in db_sessions:
            id = db_session[0]
            hardware_id = db_session[1]
            domains = get_domains(db, db_session[0])
            day_of_week = convert_datetime_to_days_since_new_week(db_session[3])
            recordings = []
            keywords = []

            interests = get_interests(db, db_session[0])
            for interest in interests:
                if convert_datetime_to_seconds_from_midnight(interest[1]) < convert_datetime_to_seconds_from_midnight(interests[0][1]):
                    recordings.append((convert_datetime_to_seconds_from_midnight(interest[1])+86400) / convert_datetime_to_seconds_from_midnight(interests[0][1]))
                else:
                    recordings.append(convert_datetime_to_seconds_from_midnight(interest[1])/convert_datetime_to_seconds_from_midnight(interests[0][1]))
            if len(interests) < 0:
                time_of_day_start = 0
                time_of_day_stop = 0
            else:
                time_of_day_start = convert_datetime_to_seconds_from_midnight(interests[0][1])
                time_of_day_stop = convert_datetime_to_seconds_from_midnight(interests[len(interests)-1][1])

            session = Session(id, hardware_id, time_of_day_start, time_of_day_stop, day_of_week, domains, recordings, keywords)
            if len(session.recordings) > 1:
                sessions.append(session)
        return sessions
        db.close()
    except:
        db.close()
        print "Get Sessions Error"

def get_keywords_for_session_group(groupId):
    keywords = []
    db = get_database()
    cursor = db.cursor()
    sql = "SELECT * FROM session WHERE session_group_id = '%d'" % groupId
    try:
        cursor.execute(sql)
        sessions = cursor.fetchall()
        for session in sessions:
            interests = get_interests(db, session[0])
            for interest in interests:
                keywords.extend(get_interest_keywords(db, interest[0]))
    except:
        print "Get Domains Error"
    finally:
        db.close()
    return keywords


def compute_distance_matrix(sessions):
    global matrix
    global distArray
    matrix = np.empty(shape=(len(sessions), len(sessions)))
    for i in range(len(sessions)):
        for j in range(len(sessions)):
            matrix[i][j] = calculate_session_distance(i, j, sessions)
    distArray = ssd.pdist(matrix, "euclidean")


def compare_session_location(location1, location2):
    if location1 == location2:
        return 0
    else:
        return 100

def compare_time_of_day(time1, time2):
    full_day_ms = Decimal(86400)
    if time1 > time2:
        distance = time1-time2
    elif time2 > time1:
        distance = time2-time1
    else:
        return 0
    if distance > Decimal(full_day_ms/2):
        distance = full_day_ms - Decimal(distance)
    return (Decimal(distance) / Decimal(full_day_ms))*200

def compare_day_of_week(day1, day2):
    full_week = Decimal(7)
    if day1 > day2:
        distance = day1 - day2
    elif day2 > day1:
        distance = day2 - day1
    else:
        return 0
    if distance > Decimal(full_week/2):
        distance = full_week - Decimal(distance)
    return (distance / full_week)*100

def compare_domains(domains1, domains2):
    domains1_length = len(domains1)
    domains2_length = len(domains2)
    # print "domain1_length: " + str(domains1_length) + " domains2_length: " + str(domains2_length)
    total_matches = 0
    for i in range(domains1_length):
        for j in range(domains2_length):
            if domains1[i] == domains2[j]:
                total_matches += 1
    # print "total_matches: " + str(total_matches)
    distance = 0
    if (domains1_length == 0 and domains2_length == 0) or total_matches == 0:
        return 0
    if domains1_length >= domains2_length:
        distance = 100-(Decimal(total_matches)/Decimal(domains2_length)*100)
    else:
        distance = 100-(Decimal(total_matches)/Decimal(domains1_length)*100)
    # print distance
    return distance

def compare_patterns(pattern1, pattern2):
    dist = mlpy.dtw_std(pattern1, pattern2, dist_only=True, squared=True)

    if dist > 1:
        return 100
    else:
        return Decimal(dist*100)

def calculate_session_distance(i, j, sessions):
    session1 = sessions[i]
    session2 = sessions[j]
    hardware = compare_session_location(session1.hardware_id, session2.hardware_id)
    time_of_day_start = compare_time_of_day(session1.time_of_day_start, session2.time_of_day_start)
    time_of_day_stop = compare_time_of_day(session1.time_of_day_stop, session2.time_of_day_stop)
    day_of_week = compare_day_of_week(session1.day_of_week, session2.day_of_week)
    domains = compare_domains(session1.domains, session2.domains)
    patterns = compare_patterns(session1.recordings, session2.recordings)

    return (hardware*5 + time_of_day_start*8 + time_of_day_stop*5 + day_of_week*0 + domains*8 + patterns*6)

def group_sessions_by_cluster(clusters):
    index = 0
    context_of_use_list = [[] for i in range(clusters.max())]
    for cluster in clusters:
        context_of_use_list[cluster-1].append(sessions[index])
        index += 1
    return context_of_use_list

def cluster(method):
    Y1 = hac.linkage(distArray, method=method)
    # knee = np.diff(Y1[::-1, 2], 2)
    # num_clust1 = knee.argmax() + 2
    return hac.fcluster(Y1, distArray.max()*0.5, 'distance')

def create_wordcloud_from_session_groups(session_group_ids):
    print "Creating wordcloud"
    d = "C:\Program Files\Apache Software Foundation\Tomcat 8.0\webapps\ROOT\images"
    for id in session_group_ids:
        keywords = get_keywords_for_session_group(id)
        wordcloud = WordCloud().generate(" ".join(keywords))
        # store to file
        wordcloud.to_file(path.join(d, "%s.png" % str(id)))
        print "created wordcloud for group " + str(id)

def run():
    print "\n\nStart clustering\n"
    global sessions
    users = get_users()
    for user in users:
        print "User: %s" % user[0]
        sessions = get_sessions(user[0])
        print "Sessions: %d" % len(sessions)
        if len(sessions) > 10:
            compute_distance_matrix(sessions)
            #methods: single, complete, average, weighted
            method = 'complete'
            clusters = cluster(method)
            print "Clusters: %d" % max(clusters)
            contexts_of_use = group_sessions_by_cluster(clusters)
            session_group_ids = create_new_session_groups(contexts_of_use, user[0])
            create_wordcloud_from_session_groups(session_group_ids)
        print "*****************"
    interval = 3600*1
    time = datetime.datetime.now()
    print "%s - Clusterign complete. Running again in %d seconds" % (str(time.hour)+":"+str(time.minute)+":"+str(time.second), interval)
    threading.Timer(interval, run).start()


if __name__ == '__main__':
    run()