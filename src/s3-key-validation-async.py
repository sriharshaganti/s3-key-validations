import re
import sys
import pathlib
from typing import IO

import asyncio
import logging
import urllib.error
import urllib.parse
import aiofiles
import aiohttp
import async_timeout
import aiobotocore
import botocore

from aiohttp import ClientSession,ClientError
from aiohttp.http_exceptions import HttpProcessingError

# Final image url statuses
status_url = set()

# file path
FILE_PATH = pathlib.Path(__file__).parent

# Output file path
OUTPUT_FILE_PATH = FILE_PATH.joinpath("resulturls")

# Semaphore limit
LIMIT_CONCURRENT_REQUEST_S3 = 4000

# Logger configuration
logger = logging.getLogger(__name__)

# S3 Bucket Name
S3_BUCKET_NAME = '<Bucket Name>'


async def fetch_s3_response(key: str,bucket: str, session) -> str:
    """ Function to fetch the response of the Key from S3 bucket
    """
    try:
        resp = await session.list_objects_v2(Bucket=bucket, Prefix=key)
        for keyval in resp['Contents']:
            if(keyval['Key']==key):
                break
    except (KeyError) as e:
        logger.error("Key: %s | Error: [%s]",key,'404')
        return '{}{}|{}|{}'.format(key,('|').join(key.split('/')),'404')
    return ''

async def parse_s3_response_validate(key,bucket, client, sem) -> None:
    """fetches response from s3 and parses it and adds it to result set
    """
    async with sem:
        response = await fetch_s3_response(key,bucket,client)
        status_url.add(response)
  
        
async def fetch_s3_response_validate(keys) -> None:
    """ Fetch the response from S3 bucket by passing the image keys and validate the response
    """
    sem = asyncio.Semaphore(LIMIT_CONCURRENT_REQUEST_S3)
    s3Session = aiobotocore.get_session()
    async with s3Session.create_client('s3', region_name='us-west-2') as client:
        s3tasks = []
        for key in keys:
            s3tasks.append(parse_s3_response_validate(key,S3_BUCKET_NAME, client, sem))
        await asyncio.gather(*s3tasks)

def main():
    """ Main script execution
    """
    assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
    # Configure logging on console
    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
        level=logging.INFO,
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )
    logging.getLogger("chardet.charsetprober").disabled = True
    keys = set()
    for key in open('../key-data'):
        keys.add(key.strip())   
    s3loop = asyncio.get_event_loop()   
    s3loop.run_until_complete(fetch_s3_response_validate(keys))
    logger.info('Total number of broken image urls : %s', len(status_url))
    with open(OUTPUT_FILE_PATH, 'w') as f:
        for item in status_url:
            f.write("%s\n" % item)
  
            
if __name__ == "__main__":
    main()