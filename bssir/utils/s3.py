import logging
import tomllib

import boto3
from mypy_boto3_s3.service_resource import Bucket
from botocore.exceptions import ClientError, NoCredentialsError

from ..metadata_reader import Mirror


def get_bucket(mirror: Mirror) -> Bucket:
    """Initializes and validates a connection to an S3-compatible bucket.

    This function reads credentials from a local 'tokens.toml' file, uses
    them to establish a session with the specified S3 endpoint, and then
    verifies that the bucket exists and is accessible before returning a
    resource object.

    Parameters
    ----------
    mirror : Mirror
        An object containing the configuration for the S3 storage, including
        its name, endpoint URL, and bucket name.

    Returns
    -------
    Bucket
        A boto3 Bucket resource object, ready for interaction.

    Raises
    ------
    FileNotFoundError
        If 'tokens.toml' does not exist.
    KeyError
        If the configuration for the specified mirror or required keys
        (e.g., 'access_key') are not found in 'tokens.toml'.
    NoCredentialsError
        If boto3 cannot find or process credentials.
    ClientError
        If there's an issue connecting to the S3 service, if the bucket
        does not exist, or if access is denied.

    """
    # 1. Read credentials from the TOML file.
    logging.info(f"Attempting to retrieve credentials for mirror '{mirror.name}'.")
    try:
        with open("tokens.toml", "rb") as file:
            token = tomllib.load(file)[mirror.name]
    except FileNotFoundError:
        logging.error(
            "Could not find 'tokens.toml'. Please ensure it is in the root directory."
        )
        raise
    except KeyError:
        logging.error(f"Token for mirror '{mirror.name}' not found in 'tokens.toml'.")
        raise

    # 2. Establish a connection and get the bucket resource.
    logging.info(
        f"Connecting to endpoint '{mirror.endpoint}' for bucket '{mirror.bucket_name}'."
    )
    try:
        s3_resource = boto3.resource(
            "s3",
            endpoint_url=mirror.endpoint,
            aws_access_key_id=token["access_key"],
            aws_secret_access_key=token["secret_key"],
        )
        # The Bucket object is a high-level abstraction. Its creation does not
        # trigger a network request or validate the bucket's existence.
        bucket = s3_resource.Bucket(mirror.bucket_name)  # type: ignore
    except NoCredentialsError:
        # This is unlikely if the token file is parsed correctly, but good to have.
        logging.error("Boto3 could not process credentials. Check token format.")
        raise
    except KeyError as e:
        logging.error(f"Missing '{e}' in token config for mirror '{mirror.name}'.")
        raise

    # 3. Validate bucket existence and accessibility.
    try:
        # The head_bucket() method is a low-cost way to check for existence
        # and permissions without listing the bucket's contents.
        bucket.meta.client.head_bucket(Bucket=mirror.bucket_name)
        logging.info(f"Successfully connected and verified bucket '{mirror.bucket_name}'.")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == "404":
            logging.error(
                f"Bucket '{mirror.bucket_name}' not found at endpoint '{mirror.endpoint}'."
            )
        elif error_code == "403":
            logging.error(
                f"Access denied to bucket '{mirror.bucket_name}'. Check credentials "
                "and permissions."
            )
        else:
            logging.error(f"Failed to connect to S3 bucket '{mirror.bucket_name}': {e}")
        raise

    return bucket

    # try:
    #     with open("tokens.toml", "rb") as file:
    #         token = tomllib.load(file)[mirror.name]
    # except FileNotFoundError:
    #     logging.error("Could not find 'tokens.toml'. Please ensure it exists.")
    #     raise
    # except KeyError:
    #     logging.error(f"Token for mirror '{mirror.name}' not found in 'tokens.toml'.")
    #     raise

    # try:
    #     s3_resource = boto3.resource(
    #         "s3",
    #         endpoint_url=mirror.endpoint,
    #         aws_access_key_id=token["access_key"],
    #         aws_secret_access_key=token["secret_key"],
    #     )
    #     bucket = s3_resource.Bucket(mirror.bucket_name) # type: ignore
    # except NoCredentialsError:
    #     logging.error("Boto3 could not find credentials. Check your token file.")
    #     raise
    # except ClientError as e:
    #     logging.error(f"Failed to connect to S3 bucket '{mirror.bucket_name}': {e}")
    #     raise

    # return bucket
