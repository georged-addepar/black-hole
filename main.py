import testdata
import snowflake.connector
import configparser
import os
import urllib.request
import urllib.parse
import tempfile
import textract
import logging
import datetime
from candidate import Candidate

# logging and config globals
CONFIG_FILE = 'config/config.cfg'
config = configparser.ConfigParser()
config.read(os.path.expanduser(CONFIG_FILE))
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO, datefmt='%Y/%m/%d %H:%M:%S')
logging.info('starting using config file %s', CONFIG_FILE)
tmp_dir = config.get('env', 'tmp')
logging.info('using tmp dir: %s', tmp_dir)
file_types = [s.lower() for s in config.get('env', 'file_types').split(',')]
logging.info('using file types: %s', file_types)
keywords = [s.replace('\t', '').lower().strip() for s in config.get('env', 'keywords').split(',')]
keywords = [s for s in keywords if s != '']
logging.info('using keywords: %s', keywords)


def get_today():
    # connect to snowflake.
    ret = []
    conn = snowflake.connector.connect(
        user=config.get('api', 'user'),
        password=config.get('api', 'pwd'),
        account=config.get('api', 'account'),
    )
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE LOAD_WH")
    cur.execute("USE DATABASE PC_FIVETRAN_DB")
    cur.execute("USE SCHEMA GREENHOUSE")
    # execute sql with today_ish's date
    # sql = config.get('sql', 'todays_applications').format('2019-11-01')
    today_ish = (datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    sql = config.get('sql', 'todays_applications').format(today_ish)
    logging.info('greenhouse query %s', sql)
    cur.execute(sql)
    # return candidates in a list of objects to decouple data storage implementation from the rest of the app.
    for (ID, CANDIDATE_ID, FIRST_NAME, LAST_NAME, APPLIED_AT, COMPANY, TITLE, JOB_NAME, FILE_NAME, RESUME_URL) \
            in cur:
        ret.append(Candidate(ID, CANDIDATE_ID, FIRST_NAME, LAST_NAME, APPLIED_AT,
                             COMPANY, TITLE, JOB_NAME, FILE_NAME, RESUME_URL))
    cur.close()
    return ret


# get resume from s3
# do NOT log the resume URI as it contains PII data (first and last name).
def get_resume(url):
    request = urllib.request.Request(url)
    try:
        response = urllib.request.urlopen(request)
        doc = response.read()
        return doc
    except urllib.error.URLError as e:
        logging.error('error retrieving resume %s', e)
        return None


# add resumes to the candiate objects
# do NOT log PII data from the candidate!
def attach_resumes(cands):
    for c in cands:
        if c.file_name is not None:
            extension = c.file_name[c.file_name.rfind('.') + 1:].lower()
            if extension in file_types:
                doc = get_resume(c.url)
                if doc is not None:
                    _, fname = tempfile.mkstemp()
                    with open(fname, 'w+b') as f:
                        f.write(doc)
                        f.close()
                    doc = textract.process(fname, extension=extension)
                    c.resume = doc.decode().lower()
                else:
                    logging.warning('resume not retrieved for application %s' % c.application_id)
            else:
                logging.warning('file type not recognized for extension %s application %s'
                                % (extension, c.application_id))
        else:
            logging.warning('no resume uploaded for application %s' % c.application_id)


def matches_keyword(candidate):
    if candidate.resume is None:
        return False
    matched = False
    for k in keywords:
        if ' ' + k + ' ' in candidate.resume:
            candidate.matched.append(k)
            matched = True
    return matched


def alert(cands):
    # will need to remove first/last name from the logs before we run this in public.
    # eliminate duplicates before alerting
    seen = []
    for c in cands:
        if (c.candidate_id, c.matched) not in seen:
            seen.append((c.candidate_id, c.matched))
            print(f'candidate {c.first_name} {c.last_name} matched on {c.matched}: {c.gh_link}')


if __name__ == '__main__':
    candidates = get_today()
    logging.info('%s applications retrieved from greenhouse.', len(candidates))
    # candidates = testdata.get_test_data(10)
    attach_resumes(candidates)
    filtered = [c for c in candidates if matches_keyword(c)]
    logging.info('%s applications matched out of a total of %s applications' % (len(filtered), len(candidates)))
    alert(filtered)



