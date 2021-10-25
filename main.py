import logging
import os
import sys
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

from twitter_bot_finder import TwitterBotFinder

project_dir = Path(__file__).resolve().parents[0]
data_dir = project_dir / "data"

required_files = {
    # "seeds": os.path.join(data_dir, 'seeds.json'),
}


def check_for_file(name: str, path: str):
    if not os.path.isfile(path):
        logging.error(f"required file {name} was not found in path {path}")
        raise FileNotFoundError(f"required file {name} was not found in path {path}")


def check_for_required_files(msg=''):
    def check_process():
        logging.info("checking for required files")
        for rf_name, rf_path in required_files.items():
            check_for_file(rf_name, rf_path)
        logging.info('checks for required files passed ✅')

    if not msg.isspace():
        while True:
            input(msg)
            try:
                check_process()
                break
            except FileNotFoundError:
                print('try again')
    else:
        check_process()


def find_bots():
    api_tokens = {
        "consumer_key": os.getenv("TWITTER_API_CONSUMER_KEY"),
        "consumer_secret": os.getenv("TWITTER_API_CONSUMER_SECRET"),
        "access_token": os.getenv("TWITTER_API_ACCESS_TOKEN"),
        "access_token_secret": os.getenv("TWITTER_API_ACCESS_TOKEN_SECRET"),
    }

    finder = TwitterBotFinder(
        data_dir=data_dir,
        api_tokens=api_tokens
    )

    finder.load_seed_users()
    finder.find_bots(override_seed_with_fringe=True)


def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_fmt)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)

    file_handler = logging.FileHandler((data_dir / 'logs.log').as_posix())
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)


if __name__ == '__main__':
    setup_logger()
    load_dotenv(find_dotenv())
    find_bots()
