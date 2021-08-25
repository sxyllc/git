# -*- coding: utf-8 -*-
# @Time    : 2020/9/17 15:39
# @Author  : 莫再提
import os
import pickle
import pandas as pd

"""
为程序的运行参数，提供本地化的持久缓存。
使得程序中断后，可以回滚当时的运行状态。
"""

# 获取当前目录
_get_abs_path = lambda path: os.path.normpath(os.path.join(os.getcwd(), path))


# 持久化为二进制文件
def save_cache(cache, cache_file_name):
    with open(cache_file_name, 'wb') as f:
        pickle.dump(cache, f)


# 加载二进制文件
def load_cache(cache_file_name):
    with open(cache_file_name, 'rb') as f:
        ds = pickle.load(f)
        return ds


class Cache:
    """
    持久化缓存类
    """
    def __init__(self, cache_file):
        """
        初始化
        """
        self.cache_file = cache_file

    def __setattr__(self, key, value):
        self.__dict__[key] = value
        if key != 'cache_file':
            save_cache(self, self.cache_file)

    def is_contain(self, item):
        if item in self.__dict__:
            return True
        return False

    def get(self, key, default):
        if self.is_contain(key):
            return self.__dict__[key]
        else:
            return default


def load_global_cache(p_id):
    """
    加载本地持久化的缓存，如果没有则新创建
    :param p_id: 程启启动标识
    :return: 缓存 和 是否新初始化的
    """
    cache_file = os.path.join(_get_abs_path(''), 'cache', '{}.pkl'.format(p_id))
    if os.path.isfile(cache_file):
        return load_cache(cache_file)
    else:
        return set()