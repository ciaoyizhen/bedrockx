# -*- encoding: utf-8 -*-
# @Time    :   2025/10/12 12:48:14
# @File    :   utils.py
# @Author  :   ciaoyizhen
# @Contact :   yizhen.ciao@gmail.com
# @Function:   通用工具
from functools import wraps
from threading import Lock

def singleton(cls):
    """线程安全的单例模式装饰器"""
    instances = {}
    lock = Lock()

    @wraps(cls)
    def get_instance(*args, **kwargs):
        # 双重检查锁，防止多线程竞争
        if cls not in instances:
            with lock:
                if cls not in instances:
                    instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance


