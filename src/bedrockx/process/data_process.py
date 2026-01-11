
from ..utils.log_manage import base_logger
from tqdm import tqdm

def filter_data(data:list[dict], filter_set:set, main_key_column:str)-> list[dict]:
    """将data中的main_key_columns字段根据filter_set的数据进行过滤

    Args:
        data (list): 待过滤的数据
        filter_set (_type_): 需要过滤的数据
        main_key_column (_type_): 待过滤数据的key

    Returns:
        list[dict]: 过滤后的数据
    """
    new_data = []
    for item in data:
        if main_key_column not in item:
            raise RuntimeError(f"data中没有字段{main_key_column=}")
        sub_item = item[main_key_column]
        if sub_item in filter_set:
            continue
        new_data.append(item)
    base_logger.info(f"原始数据大小:{len(data)}, 过滤后大小:{len(new_data)}")
    return new_data


def drop_duplicates(data: list[dict], main_key_column:str)-> list[dict]:
    """去除data中 main_key_columns字段重复的数据

    Args:
        data (list): 由dict存储的数据
        main_key_column (str): 需要去重的key

    Returns:
        list[dict]: 去重后的数据
    """
    temp_set = set()
    new_data = []
    for item in tqdm(data, desc="去重中"):
        if main_key_column not in item:
            base_logger.warning(f"不存在对应的key:{main_key_column=}\n{item=}\n已跳过")
            continue
        else:
            key = item[main_key_column]
            if key not in temp_set:
                new_data.append(item)
                temp_set.add(key)
    base_logger.info(f"原始数据大小:{len(data)}, 过滤后大小:{len(new_data)}")
    return new_data


def remove_columns(data: list[dict], key_list: list|str)-> list[dict]:
    """删除data中对应key_list对应的数据

    Args:
        data (list): 由dict存储的数据
        key_list (list|str): 需要删除的key

    Returns:
        list[dict]: 删除后的数据
    """
    
    if isinstance(key_list, str):
        key_list = [key_list]
    
    new_data = []
    for item in tqdm(data, desc="删除对应列中"):
        for k in key_list:
            if k in item:
                del item[k]
        
        new_data.append(item)
    return new_data