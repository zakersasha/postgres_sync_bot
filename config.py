"""Service configuration."""
import os
import sys

from dotenv import load_dotenv

load_dotenv()


class Config(object):
    """Project main config"""
    TOKEN = os.environ.get('TOKEN')
    RUNNER_PATH = getattr(sys.modules['__main__'], '__file__')
    ROOT_PATH = os.path.abspath(os.path.dirname(RUNNER_PATH))

    DB_NAME = os.environ.get('DB_NAME')
    DB_USERNAME = os.environ.get('DB_USERNAME')
    DB_PASSWORD = os.environ.get('DB_PASSWORD')
    DB_HOST = os.environ.get('DB_HOST')
    DB_PORT = os.environ.get('DB_PORT')
