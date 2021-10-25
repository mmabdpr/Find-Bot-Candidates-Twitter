import collections
import logging
import os
import sys
import math
import pickle
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

from twitter_bot_finder import TwitterBotFinder

project_dir = Path(__file__).resolve().parents[0]
data_dir = project_dir / "data"
results_dir = data_dir / "results"
experiments = ['1', '2', '3', '4', '5', '6']


def merge_visited_bots():
    all_bots = dict()
    for expr in experiments:
        visited_bots_cache = results_dir / expr / 'visited_bots.cache'
        with open(visited_bots_cache.as_posix(), 'rb') as f:
            bots = pickle.load(f)
        all_bots.update(bots)

    with open((results_dir / 'visited_bots.cache').as_posix(), 'wb') as f:
        pickle.dump(all_bots, f)


def merge_visited_humans():
    all_humans = dict()
    for expr in experiments:
        visited_bots_cache = results_dir / expr / 'visited_humans.cache'
        with open(visited_bots_cache.as_posix(), 'rb') as f:
            humans = pickle.load(f)
        all_humans.update(humans)

    with open((results_dir / 'visited_humans.cache').as_posix(), 'wb') as f:
        pickle.dump(all_humans, f)


def visited_bots_to_seed():
    with open((results_dir / 'visited_bots.cache').as_posix(), 'rb') as f:
        all_bots = pickle.load(f)

    with open((results_dir / 'seeds.cache').as_posix(), 'wb') as f:
        pickle.dump(list(all_bots.values()), f)


def visited_bots_to_seed_per_experiment():
    for expr in experiments:
        with open((results_dir / expr / 'visited_bots.cache').as_posix(), 'rb') as f:
            all_bots = pickle.load(f)

        with open((results_dir / expr / 'new_seeds.cache').as_posix(), 'wb') as f:
            pickle.dump(list(all_bots.values()), f)


def visited_bots_to_multiple_seeds(sep_count=2):
    all_bots = dict()
    for expr in experiments:
        visited_bots_cache = results_dir / expr / 'visited_bots.cache'
        with open(visited_bots_cache.as_posix(), 'rb') as f:
            bots = pickle.load(f)
        all_bots.update(bots)

    seeds = [[] for _ in range(sep_count)]
    for i, u in enumerate(all_bots.values()):
        seeds[i % sep_count].append(u)

    for i in range(sep_count):
        with open((results_dir / f'seeds_{i}.cache').as_posix(), 'wb') as f:
            pickle.dump(seeds[i], f)


if __name__ == '__main__':
    merge_visited_bots()
    merge_visited_humans()
    visited_bots_to_seed()
    visited_bots_to_seed_per_experiment()
    visited_bots_to_multiple_seeds()
