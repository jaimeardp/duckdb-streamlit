import os
from dotenv import load_dotenv

load_dotenv()

_suffix = "_me"

config_vars = {
    "AWS_ACCESS_KEY_ID": os.environ[f'aws_access_key_id{_suffix}'],
    "AWS_SECRET_ACCESS_KEY": os.environ[f'aws_secret_access_key{_suffix}'],
    "AWS_DEFAULT_REGION": "us-east-1",
    "BUCKET_NAME": f'{os.environ[f"BUCKET_NAME{_suffix}"]}',
    "LAMBDA_FUNCTION_NAME": f'{os.environ["LAMBDA_FUNCTION_NAME"]}'
}