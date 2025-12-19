# -*- encoding: utf-8 -*-
# @Time    :   2025/10/11 19:57:41
# @File    :   utils.py
# @Author  :   ciaoyizhen
# @Contact :   yizhen.ciao@gmail.com
# @Function:   读取文件和保存文件
import json
import inspect
from pathlib import Path
from typing import List, Dict, Literal
from tqdm import tqdm
from functools import wraps
from ..utils.log_manage import base_logger

    
def read_file(file_name: str|Path, *, output_type="list", file_type=None, main_key_column=None, encoding="utf-8", disable_tqdm=False, **kwargs)-> List|Dict:
    """读取文件，根据传参来判断读取的方式
    最终返回完整的一个list

    Args:
        file_name (str|Path): 文件路径
        
        output_type (Literal["list", "dict", "set"]): 返回类型,当该值为dict的时候,需要指定output_type
        file_type (str): 文件类型,请使用`json`,`jsonl`,`xlsx`,`csv`,`txt`
        encoding (str): 文件编码方式
        key_columns (list): 需要取的列名
        main_key_column (str): 当返回为dict时,这个为key,value为其他的值,类型为dict
        output_type (Literal["list", "dict"]): 返回类型,当该值为dict的时候
        disable_tqdm (bool): 是否关闭进度条
        
        kwargs: 其他参数
            - sheet_name (str): 读取xlsx时，可以指定读取哪个sheet_name
        
    Returns:
        list|dict|set: 根据output_type返回List|Dict|set
    """
    
    if isinstance(file_name, str):
        file_name = Path(file_name)
        
    if file_type is None:
        file_type = file_name.suffix.lstrip(".")

    match output_type:
        case "list":
            return_data = []
        case "dict":
            return_data = {}
        case "set":
            return_data = set()
        case _:
            raise RuntimeError(f"output_type 传入了一个不可预知的参数:{output_type=}\n目前仅允许`list`, `dict`, `set`")
        
    
        
    match file_type:
        case "jsonl":
            with file_name.open("r", encoding=encoding) as f:
                for line in tqdm(f.readlines(), disable=disable_tqdm):
                    if line := line.strip():
                        line = json.loads(line)
                        if isinstance(return_data, list):
                            return_data.append(line)
                        elif isinstance(return_data, dict):
                            if main_key_column not in line:
                                raise RuntimeError(f"对象没有{main_key_column=}\n原始数据:{row}")

                            value = line[main_key_column]
                            return_data[value] = line
                        elif isinstance(return_data, set):
                            if main_key_column not in line:
                                raise RuntimeError(f"对象没有{main_key_column=}\n原始数据:{row}")
                            return_data.add(line[main_key_column])
                return return_data
            
        case "json":
            with file_name.open("r", encoding=encoding) as f:
                data = json.load(f)
                assert isinstance(data, list), "理论上，这里应该是list[dict]结构，但是不是,请报告 https://github.com/ciaoyizhen/caoyizhen_basetool 让我知道!!!"
                if isinstance(return_data, dict):
                    for row in tqdm(data, disable=disable_tqdm):
                        if main_key_column not in row:
                            raise RuntimeError(f"对象没有{main_key_column=}\n原始数据:{row}")
                        value = row[main_key_column]
                        return_data[value] = row     
                            
                    return return_data
                elif isinstance(return_data, set):
                    for row in tqdm(data, disable=disable_tqdm):
                        if main_key_column not in row:
                            raise RuntimeError(f"对象没有{main_key_column=}\n原始数据:{row}")
                        return_data.add(row[main_key_column])
                    return return_data
                        
                elif isinstance(return_data, list):
                    return data
                
        case "xlsx":
            # 这里导包, 可以让不用pandas时不安装包
            import pandas as pd
            data = pd.read_excel(file_name, **kwargs)
            
            if isinstance(return_data, list):
                for _, row in tqdm(data.iterrows(), total=data.shape[0], disable=disable_tqdm):
                    row = row.to_dict()
                    return_data.append(row)
            elif isinstance(return_data, dict):
                for _, row in tqdm(data.iterrows(), total=data.shape[0], disable=disable_tqdm):
                    row = row.to_dict()
                    if main_key_column not in row:
                        raise RuntimeError(f"对象没有{main_key_column=}\n原始数据:{row}")
                    value = row[main_key_column]
                    return_data[value] = row     
            elif isinstance(return_data, set):
                for _, row in tqdm(data.iterrows(), total=data.shape[0], disable=disable_tqdm):
                    row = row.to_dict()
                    if main_key_column not in row:
                        raise RuntimeError(f"对象没有{main_key_column=}\n原始数据:{row}")
                    return_data.add(row[main_key_column])
            return return_data
            
                
        case "csv":
            import pandas as pd
            if encoding == "utf-8":  # 解决读取csv的编码问题
                data = pd.read_csv(file_name, **kwargs)
            else:
                data = pd.read_csv(file_name, encoding=encoding, **kwargs)
            
            if isinstance(return_data, list):
                for _, row in tqdm(data.iterrows(), total=data.shape[0], disable=disable_tqdm):
                    row = row.to_dict()
                    return_data.append(row)
            elif isinstance(return_data, dict):
                for _, row in tqdm(data.iterrows(), total=data.shape[0], disable=disable_tqdm):
                    row = row.to_dict()
                    
                    if main_key_column not in row:
                        raise RuntimeError(f"对象没有{main_key_column=}\n原始数据:{row}")
                    value = row[main_key_column]
                    return_data[value] = row     
            elif isinstance(return_data, set):
                for _, row in tqdm(data.iterrows(), total=data.shape[0], disable=disable_tqdm):
                    row = row.to_dict()
                    if main_key_column not in row:
                        raise RuntimeError(f"对象没有{main_key_column=}\n原始数据:{row}")
                    return_data.add(row[main_key_column])
                return return_data
            return return_data
            
        case _:
            raise RuntimeError(f"无法识别后缀{file_type=}是什么格式的文件,请传入file_type来控制或修改后缀名")
        

def save_file(file_name: str|Path, data: list, file_type=None, *, encoding="utf-8", ensure_ascii=False, json_indent=4, pd_index=False,**kwargs):
    if isinstance(file_name, str):
        file_name = Path(file_name)
        
    file_name.parent.mkdir(exist_ok=True, parents=True)
    if file_type is None:
        file_type = file_name.suffix.lstrip(".")
        
    match file_type:
        case "jsonl":
            with file_name.open("w", encoding=encoding) as f:
                for item in data:
                    f.write(json.dumps(item, ensure_ascii=ensure_ascii) + "\n")
            base_logger.info(f"文件保存至 {file_name.resolve(strict=True)} ")
        case "json":
            with file_name.open("w", encoding=encoding) as f:
                json.dump(data, f, ensure_ascii=ensure_ascii, indent=json_indent)
            base_logger.info(f"文件保存至 {file_name.resolve(strict=True)} ")
        case "xlsx":
            import pandas as pd
            data = pd.DataFrame(data)
            data.to_excel(file_name, **kwargs, index=pd_index)
            base_logger.info(f"文件保存至 {file_name.resolve(strict=True)} ")
        case "csv":
            import pandas as pd
            data = pd.DataFrame(data)
            data.to_csv(file_name, **kwargs, index=pd_index)
            base_logger.info(f"文件保存至 {file_name.resolve(strict=True)} ")
        case _:
            raise RuntimeError(f"保存文件识别,无法识别{file_type=},该保存成什么格式")

def return_to_jsonl(file_path, encoding="utf-8", ensure_ascii=False):
    """
    兼容同步和异步函数的写入装饰器
    """
    def decorator(func):
        def write_to_file(result):
            if result is None: 
                return # 允许返回None时不写入
            error_msg = f"被装饰器的函数需要有返回，并且必须是str或dict"
            
            if isinstance(result, dict):
                content = json.dumps(result, ensure_ascii=ensure_ascii)
            elif isinstance(result, str):
                content = result
            else:
                raise RuntimeError(error_msg)
            
            # 确保父目录存在
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, "a", encoding=encoding) as f:
                f.write(content + "\n")

        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                single_result = await func(*args, **kwargs)
                write_to_file(single_result)
                return single_result
            return wrapper
        else:
            @wraps(func)
            def wrapper(*args, **kwargs):
                single_result = func(*args, **kwargs)
                write_to_file(single_result)
                return single_result
            return wrapper
    return decorator





def add_suffix_file(file_path: str|Path, suffix: str, *, sep="_")-> Path:
    """为文件添加真实后缀
    example:
    >>> file = "data.jsonl"
    >>> print(add_suffix_file(file, "response"))
    >>> Path("data_response.jsonl")

    Args:
        file_path (str|Path): _description_
        suffix (str): _description_
        sep (str): 分隔符

    Returns:
        Path: 路径
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)
    
    new_name = f"{file_path.stem}{sep}{suffix}{file_path.suffix}"
    
    return Path(new_name)