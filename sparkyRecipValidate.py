#!/usr/bin/env python3

import argparse, time, csv, requests, io
from email_validator import validate_email, EmailNotValidError
from common import eprint, getenv_check, getenv, hostCleanup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests_futures.sessions import FuturesSession


def validateRecipients(f, fh, apiKey, snooze, count):
    """
    Validate a single recipient. Allows for possible future rate-liming on this endpoint.
    :param url: SparkPost URL including the endpoint
    :param apiKey: SparkPost API key
    :param recip: single recipient
    :return: dict containing JSON-decode of response
    """
    h = {'Authorization': apiKey, 'Accept': 'application/json'}
    session = FuturesSession()
    with tqdm(total=count) as pbar:
        for address in f:
            for i in address:
                thisReq = requests.compat.urljoin(url, i)
                futures = [session.get(thisReq,headers=h)]
                for future in as_completed(futures):
                    resp = future.result()
                    content = resp.json()
                    if resp.status_code == 200:
                        if content and 'results' in content:
                            row = content['results']
                            row['email'] = i
                            fh.writerow(row)
                        else:
                            eprint('Error: response', content)
                        pbar.update(1)
                    else:
                        print(resp.status_code)
                        print('Snoozing before trying again')
                        time.sleep(10)

def processFile(infile, outfile, url, apiKey, snooze, skip_precheck):
    """
    Process the input file - a list of email addresses to validate. Write results to outfile.
    Two pass approach. First pass checks file is readable and contains email addresses. Second pass calls validation.
    :param infile:
    :param outfile:
    :param url: str
    :param apiKey: str
    :param threads: int
    :param snooze: int
    """
    if infile.seekable() and not skip_precheck:
        # Check & report syntactically-OK & bad email addresses before we start API-based validation, if we can
        f = csv.reader(infile)
        count_ok, count_bad = 0, 0
        for r in f:
            if len(r) == 1:
                recip = r[0]
                try:
                    validate_email(recip, check_deliverability=False)
                    count_ok += 1
                except EmailNotValidError as e:
                    # email is not valid, exception message is human-readable
                    eprint(f.line_num, recip, str(e))
                    count_bad += 1
            else:
                count_bad += 1
        eprint('Scanned input {}, contains {} syntactically OK and {} bad addresses. Validating with SparkPost..'
               .format(infile.name, count_ok, count_bad))
        infile.seek(0)
    else:
        eprint('Skipping input file syntax pre-check. Validating with SparkPost..')

    f = csv.reader(infile)
    fList = ['email', 'valid', 'result', 'reason', 'is_role', 'is_disposable', 'is_free', 'did_you_mean']
    fh = csv.DictWriter(outfile, fieldnames=fList, restval='', extrasaction='ignore')
    fh.writeheader()

    validateRecipients(f,fh,apiKey,snooze,count_ok)

    infile.close()
    outfile.close()
    eprint('Done')

    infile.close()
    outfile.close()
    eprint('Done')


# -----------------------------------------------------------------------------------------
# Main code
# -----------------------------------------------------------------------------------------
parser = argparse.ArgumentParser(
    description='Validate recipients with SparkPost. \
        Checks a single email address, or reads from specified input file (or stdin). \
        Results to specified output file or stdout (i.e. can act as a filter).')
inp = parser.add_mutually_exclusive_group(required=False)
inp.add_argument('-i', '--infile', type=argparse.FileType('r'), default='-',
                    help='filename to read email recipients from (in .CSV format)')
inp.add_argument('-e', '--email', type=str, action='store',
                    help='email address to validate. May carry multiple addresses, comma-separated, no spaces')
parser.add_argument('-o', '--outfile', type=argparse.FileType('w'), default='-',
                    help='filename to write validation results to (in .CSV format)')
parser.add_argument('--skip_precheck', action='store_true', help='Skip the precheck of input file email syntax')
args = parser.parse_args()

apiKey = ''                     # API key is mandatory
host = hostCleanup(getenv('SPARKPOST_HOST', default='api.sparkpost.com'))
url = host + '/api/v1/recipient-validation/single/'

snooze=10
if args.email:
    cmdInfile = io.StringIO(args.email.replace(',', '\n'))
    cmdInfile.name = 'from command line'
    processFile(cmdInfile, args.outfile, url, apiKey, snooze, args.skip_precheck)
else:
    processFile(args.infile, args.outfile, url, apiKey, snooze, args.skip_precheck)
