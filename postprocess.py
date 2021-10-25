import collections
import logging
import os
import sys
import math
import pickle
from pathlib import Path
import tweepy

from dotenv import find_dotenv, load_dotenv

from twitter_bot_finder import TwitterBotFinder

project_dir = Path(__file__).resolve().parents[0]
data_dir = project_dir / "data"
results_dir = data_dir / "results"
experiments = ['1', '2', '3', '4', '5', '6']
all_bots_file = results_dir / 'all_bots.txt'


def merge_files():
    lines = []

    for expr in experiments:
        experiment_path = results_dir / expr
        bots_list_file = experiment_path / 'bots.txt'
        with open(bots_list_file.as_posix(), 'r') as f:
            lines.extend(f.readlines())

    lines = list(set(lines))

    with open(all_bots_file.as_posix(), 'w') as f:
        f.writelines(lines)


def add_extra_info_from_cache():
    all_bots = dict()

    for expr in experiments:
        experiment_path = results_dir / expr
        with open((experiment_path / 'visited_bots.cache').as_posix(), 'rb') as f:
            bots = pickle.load(f)
        all_bots.update(bots)

    lines = ['id,screen_name,bot_created_at,last_tweet_created_at,tweet_count,tweet_per_day\n']
    for bot in all_bots.values():
        if bot.status.lang != 'fa':
            continue
        lines.append(f'{bot.id},{bot.screen_name},{bot.created_at},{bot.status.created_at},{bot.statuses_count},{math.floor(bot.statuses_count / max(1, (bot.status.created_at - bot.created_at).days))}\n')
    with open((results_dir / 'all_bots_extended.txt').as_posix(), 'w') as f:
        f.writelines(lines)


def add_extra_info():
    api_tokens = {
        "consumer_key": os.getenv("TWITTER_API_CONSUMER_KEY"),
        "consumer_secret": os.getenv("TWITTER_API_CONSUMER_SECRET"),
        "access_token": os.getenv("TWITTER_API_ACCESS_TOKEN"),
        "access_token_secret": os.getenv("TWITTER_API_ACCESS_TOKEN_SECRET"),
    }
    auth = tweepy.OAuthHandler(api_tokens["consumer_key"], api_tokens["consumer_secret"])
    auth.set_access_token(api_tokens["access_token"], api_tokens["access_token_secret"])
    api = tweepy.API(auth, wait_on_rate_limit=True)

    all_bots = []

    bots_file = data_dir / "bots.txt"
    if not bots_file.is_file():
        print(f'{bots_file.as_posix()} file not found')
        return

    with open(bots_file.as_posix(), 'r') as f:
        bot_lines = f.readlines()

    for bl in bot_lines:
        bot_id, bot_user = bl.strip().split(sep=',')
        bot: tweepy.User = api.get_user(id=bot_id)
        if bot_user == bot.screen_name:
            all_bots.append(bot)
        else:
            print('name mismatch')

    lines = ['id,screen_name,bot_created_at,last_tweet_created_at,tweet_count,tweet_per_day\n']
    for bot in all_bots:
        if bot.status.lang != 'fa':
            continue
        lines.append(
            f'{bot.id},{bot.screen_name},{bot.created_at},{bot.status.created_at},{bot.statuses_count},{math.floor(bot.statuses_count / max(1, (bot.status.created_at - bot.created_at).days))}\n')
    with open((data_dir / 'bots_extended.txt').as_posix(), 'w') as f:
        f.writelines(lines)


if __name__ == '__main__':
    load_dotenv(find_dotenv())
    merge_files()
    add_extra_info_from_cache()
    add_extra_info()
