import heapq
import logging
import pickle
import sys
from pathlib import Path
from typing import *
from collections import OrderedDict
import shutil

import pandas as pd
import tweepy


class TwitterNode:
    def __init__(self, user: tweepy.User):
        self.user = user

        self.delta = max(1, (user.status.created_at - user.created_at).days)
        self.tweet_per_day = user.statuses_count / self.delta

    def __lt__(self, other):
        return self.tweet_per_day > other.tweet_per_day

    def __hash__(self):
        return self.user.id

    def __eq__(self, other):
        return self.user.id == other.user.id

    def __repr__(self):
        return self.user.screen_name

    def __str__(self):
        return self.user.screen_name


class TwitterBotFinder:
    def __init__(self, data_dir: Path, api_tokens: Dict):
        self.data_dir = data_dir
        self.api = self._create_api(api_tokens)

        self.seed_users = []
        self.logger = logging.getLogger(__name__)

    def find_bots(self, bot_count=20_000, override_seed_with_fringe=False):
        fringe: Optional[Dict[int, TwitterNode]] = None
        if override_seed_with_fringe:
            fringe = self._load_cache('fringe.cache')
        if fringe is None:
            fringe = {u.id: TwitterNode(u) for u in self.seed_users if u.statuses_count > 0 and hasattr(u, 'status')}
        else:
            # fringe = list(set(fringe))
            fringe = {u.user.id: u for u in fringe if hasattr(u, 'status') and u.statuses_count > 0}

        visited_humans: Dict[int, tweepy.User] = self._load_cache('visited_humans.cache')
        if visited_humans is None:
            visited_humans = dict()
        visited_bots: Dict[int, tweepy.User] = self._load_cache('visited_bots.cache')
        if visited_bots is None:
            visited_bots = dict()

        c = self._count_bots(fringe, ['not_verified', 'tweet_per_day'])
        total_bots = c + len(visited_bots)
        self.logger.info(f'fringe bots: {c} -- visited bots: {len(visited_bots)} -- sum: {total_bots}')

        expanding, i, last_i = True, 1, 1
        while any(fringe):
            if expanding and i % 35 == 0 and last_i != i:
                last_i = i
                self._save_cache(visited_humans, 'visited_humans.cache')
                self._save_cache(visited_bots, 'visited_bots.cache')

                # fringe = list(set(fringe))
                # heapq.heapify(fringe)
                self._save_cache(list(fringe.values()), 'fringe.cache')

                c = self._count_bots(fringe, ['not_verified', 'tweet_per_day'])
                total_bots = c + len(visited_bots)
                self.logger.info(f'fringe bots: {c} -- visited bots: {len(visited_bots)} -- sum: {total_bots}')

            # node: TwitterNode = heapq.heappop(fringe)
            node = max(fringe.values(), key=lambda x: x.tweet_per_day)
            fringe.pop(node.user.id)
            user: tweepy.User = node.user

            self.logger.info(f'checking the node {user.id} {user.screen_name}')

            if user.id in visited_humans.keys() or user.id in visited_bots.keys():
                continue

            if not self._should_be_visited(user):
                continue

            if not self._is_bot(user, ['not_verified', 'tweet_per_day']):
                visited_humans[user.id] = user
                continue

            i += 1
            visited_bots[user.id] = user
            self.logger.info(f'found a bot: {user.screen_name} -- visited bots: {len(visited_bots)}')
            with open((self.data_dir / 'bots.txt').as_posix(), 'a') as f:
                f.write(f'{user.id},{user.screen_name}\n')

            if expanding and total_bots > bot_count:
                self.logger.info(f'stopped expansion. sweeping fringe')
                expanding = False

            if expanding:
                children = self._get_children(user, ['followers', 'friends', 'retweets'])
                for child in children:
                    if child.statuses_count > 0 and hasattr(child, 'status'):
                        fringe[child.id] = TwitterNode(child)
                        # heapq.heappush(fringe, TwitterNode(child))

        # save caches for the last time
        self._save_cache(visited_humans, 'visited_humans.cache')
        self._save_cache(visited_bots, 'visited_bots.cache')
        self._save_cache(list(fringe.values()), 'fringe.cache')

    def load_seed_users(self):
        seeds = []
        cache_file = self.data_dir / 'seeds.cache'
        if cache_file.is_file():
            seeds = self._load_cache(cache_file.as_posix())
        else:
            df = pd.read_json((self.data_dir / 'seeds.json').as_posix(), orient='records')
            for screen_name in df['screen_name'].values:
                try:
                    user = self.api.get_user(screen_name=screen_name)
                    if user.statuses_count > 0 and hasattr(user, 'status'):
                        seeds.append(user)
                except Exception as e:
                    self.logger.error(f'error getting user {screen_name} from seeds.json file\n--\n {e}')
            self._save_cache(seeds, cache_file.as_posix())
        self.seed_users = seeds

    @staticmethod
    def _create_api(api_tokens):
        auth = tweepy.OAuthHandler(api_tokens["consumer_key"], api_tokens["consumer_secret"])
        auth.set_access_token(api_tokens["access_token"], api_tokens["access_token_secret"])
        return tweepy.API(auth, wait_on_rate_limit=True)

    def _get_children(self, node: tweepy.User, expansion_strategies: List[str]) -> List[tweepy.User]:
        children = []

        if 'followers' in expansion_strategies:
            self.logger.info(f'getting the followers for user {node.screen_name}')
            try:
                for follower in tweepy.Cursor(self.api.followers, id=node.id).items(30):
                    children.append(follower)
            except Exception as e:
                self.logger.error(f'error getting followers of user {node.id}\n -- \n {e}')

        if 'friends' in expansion_strategies:
            self.logger.info(f'getting the friends for user {node.screen_name}')
            try:
                for friend in tweepy.Cursor(self.api.friends, id=node.id).items(30):
                    children.append(friend)
            except Exception as e:
                self.logger.error(f'error getting friends of user {node.id}\n -- \n {e}')

        if 'retweets' in expansion_strategies:
            self.logger.info(f'expanding using retweets for user {node.screen_name}')
            try:
                for tweet in tweepy.Cursor(self.api.user_timeline, id=node.id, include_rts=True, count=10).items(10):
                    if not hasattr(tweet, 'retweeted_status'):
                        continue

                    for retweeter_id in \
                            tweepy.Cursor(self.api.retweeters, id=tweet.retweeted_status.id, count=10).items(10):
                        retweeter = self.api.get_user(id=retweeter_id)
                        if not retweeter.protected:
                            children.append(retweeter)
            except Exception as e:
                self.logger.error(f'error getting retweets of user {node.id}\n -- \n {e}')

        return children

    def _is_bot(self, node: tweepy.User, filtering_strategies: List[str]):
        if 'not_verified' in filtering_strategies:
            if node.verified:
                return False

        if 'tweet_per_day' in filtering_strategies:
            try:
                total_days = max(1, (node.status.created_at - node.created_at).days)
                tweet_count = node.statuses_count
                if tweet_count / total_days < 10:
                    return False
            except Exception as e:
                self.logger.error(f'could not check tweet per day for {node.screen_name}\n -- \n {e}')
                return False

        return True

    def _save_cache(self, obj, filename: str):
        src_path = (self.data_dir / filename).as_posix()

        if not Path(src_path).is_file():
            self.logger.error(f'no file to backup does not exist')
        else:
            try:
                backup_path = (self.data_dir / f'{filename}.backup').as_posix()
                shutil.copy(src_path, backup_path)
            except Exception as e:
                self.logger.error(f'could not backup the cache {filename}\n -- \n {e}')
                sys.exit()

        try:
            self.logger.info(f'saving cache into {src_path}')
            with open(src_path, 'wb') as f:
                pickle.dump(obj, f)
            self.logger.info(f'saved cache into {src_path}')
        except Exception as e:
            self.logger.error(f'could not save the cache {filename}\n -- \n {e}')

    def _load_cache(self, filename: str):
        x = None

        try:
            path = (self.data_dir / filename).as_posix()
            self.logger.info(f'loading cache from {path}')
            with open(path, 'rb') as f:
                x = pickle.load(f)
            self.logger.info(f'loaded cache from {path}')
        except Exception as e:
            self.logger.info(f'could not load the cache {filename}\n -- \n {e}')

        return x

    def _should_be_visited(self, user: tweepy.User, silent=False):
        if user.status.lang != 'fa':
            if not silent:
                self.logger.info(f'lang of user {user.screen_name} is {user.status.lang} != fa')
            return False

        return True

    def _count_bots(self, fringe, filtering_strategies):
        return sum([1 for u in fringe.values()
                    if self._is_bot(u.user, filtering_strategies)
                    and self._should_be_visited(u.user, True)])
