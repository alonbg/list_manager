import os
import hashlib
from distutils.util import strtobool
from urllib.parse import urlparse
import uuid
from requests_cache import CachedSession
from boto3 import Session
from botocore.client import Config
from botocore.exceptions import BotoCoreError
from .. import log, Utils


ACCESS_ID = os.environ.get("ACCESS_ID", "xxxx")
SECRET_KEY = os.environ.get("SECRET_KEY", "xxxx")
REGION = os.environ.get("REGION", "nyc3")
BUCKET = os.environ.get("BUCKET", "wireguard")
PATH_PREFIX = os.environ.get("PATH_PREFIX", "blocky")
ENDPOINT_URL = (
    os.environ.get("ENDPOINT_URL", f"https://{REGION}.digitaloceanspaces.com/")
    + PATH_PREFIX)

class Transfer:
    # http cache
    from datetime import timedelta
    session = CachedSession(
        Utils.with_root("http_cache"),
        backend="filesystem",
        expire_after=timedelta(days=1),
        serializer="yaml",
        indent=4,
    )

    s3_client = Session().client(
        "s3",
        config=Config(s3={"addressing_style": "virtual"}),
        region_name=REGION,
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_ID,
        aws_secret_access_key=SECRET_KEY,
    )
    
    # monkey patching mock
    if "mock_upload" not in globals():
         mock_upload = bool(strtobool(os.environ.get("MOCK", "False")))
    
    if mock_upload:
        def put_object(body:bytes, key:str, **_kwargs):
            with Utils.get_create_dir("mock_upload").joinpath(key.replace('/', '_')).open("w") as fp:
                fp.write(body.decode("utf-8"))

        s3_client.put_object = put_object

    @staticmethod
    def upload_list(body: bytes, url: str, **kwargs) -> bool:
        key = Transfer.with_url_path(url, **kwargs)
        try:
            Transfer.s3_client.put_object(
                Body=body, Bucket=BUCKET, Key=key, ACL="public-read"
            )
            return True
        except BotoCoreError as e:
            log.exception(f"upload to {key} failed: {e} {url}")
            return False

    @staticmethod
    def with_url_path(url, path_prefix="/"):
        return f"{path_prefix}{urlparse(url).path}"
    
    @classmethod
    def download_list(cls, url):
        try:
            response = cls.session.get(url, stream=False)
            response.raise_for_status()  # Check for any errors
            log.info(f"completed download of {url}")
            return response.content, response.from_cache
        except Exception() as e:
            log.exception(f"{e} {url}")

    @staticmethod
    def url_hash_uuid(url) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, url))

    @staticmethod
    def url_hash_blake2b(url) -> str:
        key_parts = list(urlparse(url))
        key = hashlib.blake2b(digest_size=8)
        for part in key_parts:
            key.update(str(part).encode("utf-8"))
        return key.hexdigest()
    
    @staticmethod
    def url_hash(url) -> str:
        return Transfer.url_hash_uuid(url)
    
    

# monkey patching for mock upload
if globals().get('mock_upload', bool(strtobool(os.environ.get("MOCK", "False")))):
    def upload_list(body: bytes, url: str, **kwargs) -> bool:
        key = Transfer.with_url_path(url, **kwargs)
        with Utils.get_create_dir("mock_upload").joinpath(key.replace('/', '_')).open("w") as fp:
                fp.write(body.decode("utf-8"))
        return True
        
    log.info("using mock upload")
    Transfer.upload_list = upload_list
    