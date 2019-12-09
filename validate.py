#!/usr/bin/env python
import snowflake.connector
import configparser
import os

CONFIG_FILE = 'config/config.cfg'
config = configparser.ConfigParser()


def init():
    config.read(os.path.expanduser(CONFIG_FILE))
    conn = snowflake.connector.connect(
        user=config.get('api', 'user'),
        password=config.get('api', 'pwd'),
        account=config.get('api', 'account'),
        )
    cs = conn.cursor()
    cs.execute("USE WAREHOUSE LOAD_WH")
    cs.execute("USE DATABASE PC_FIVETRAN_DB")
    cs.execute("USE SCHEMA GREENHOUSE")
    return cs


def validate(cs):
    # Gets the version
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
    cs.close()
    ctx.close()


def get_today(cur):
    applications_sql = config.get('sql', 'todays_applications')
    try:
        cur.execute(applications_sql)
        for (ID, CANDIDATE_ID, FIRST_NAME, LAST_NAME, APPLIED_AT, COMPANY, TITLE, JOB_NAME, FILE_NAME, RESUME_URL) \
                in cur:
            print('%s %s %s %s %s %s %s %s %s %s' % (ID, CANDIDATE_ID, FIRST_NAME, LAST_NAME, APPLIED_AT,
                                                     COMPANY, TITLE, JOB_NAME, FILE_NAME, RESUME_URL))
    finally:
        cur.close()


cursor = init()
# validate(cursor)
get_today(cursor)
