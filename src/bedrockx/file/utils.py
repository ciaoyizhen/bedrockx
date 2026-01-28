# -*- encoding: utf-8 -*-
# @Time    :   2025/10/11 19:57:41
# @File    :   utils.py
# @Author  :   ciaoyizhen
# @Contact :   yizhen.ciao@gmail.com
# @Function:   读取文件和保存文件
import json
import inspect
import ijson
from itertools import islice
from pathlib import Path
from typing import List, Dict, Literal, Union, Optional, Set, Callable, Any
from tqdm import tqdm
from functools import wraps
from ..utils.log_manage import base_logger
import pandas as pd

class ReadFileExampleCallBack:
    """
    预处理回调函数集合示例
    """

    @staticmethod
    def extract_content_str(item: Dict) -> str:
        """
        场景：提取嵌套的文本内容
        用途：output_type="set" 或 "list"
        """
        # 假设数据格式: {"message": [{"content": "..."}]}
        return item["messages"][0]["content"]
    
    @staticmethod
    def extract_content_dict(item: Dict) -> Dict:
        """
        场景：将嵌套结构扁平化
        用途：output_type="list"
        """
        content = item["messages"][0]["content"]
        label = item.get("label", "unknown")
        # 返回一个新的扁平字典
        return {"content": content, "label": label}

    @staticmethod
    def filter_positive_only(item: Dict) -> Optional[Dict]:
        """
        场景：过滤数据 (只保留 label 为 positive 的数据)
        用途：output_type="list"
        """
        if item.get("label") == "positive":
            # 也可以在这里顺便做清洗
            item["processed"] = True 
            return item
        # 返回 None 代表该条数据会被 read_file 丢弃
        return None

    @staticmethod
    def transform_for_id_mapping(item: Dict) -> Dict:
        """
        场景：配合 output_type="dict" 使用，必须保留 main_key_column
        用途：output_type="dict", main_key_column="id"
        """
        # 提取内容，但必须保留 'id' 字段，否则 read_file 会报错
        return {
            "id": item["id"], 
            "text_length": len(item["messages"][0]["content"]),
            "role": item["messages"][0]["role"]
        }

def _get_line_count(file_path: Path) -> int:
    """
    快速计算文件行数（不加载到内存）
    对于大文件，这比直接遍历快得多
    """
    lines = 0
    with file_path.open("rb") as f:
        # 使用 1MB 的 buffer 逐块读取来数换行符
        buf_size = 1024 * 1024
        read_f = f.raw.read
        buffer = read_f(buf_size)
        while buffer:
            lines += buffer.count(b'\n')
            buffer = read_f(buf_size)
    return lines

def read_file(
    file_name: Union[str, Path], 
    *, 
    output_type: Literal["list", "dict", "set"] = "list", 
    file_type: str = None, 
    main_key_column: str = None, 
    encoding: str = "utf-8", 
    disable_tqdm: bool = False, 
    data_length: int = None, 
    process_fn: Callable[[Any], Any] = None,
    **kwargs
) -> Union[List, Dict, Set]:
    """
    读取文件，根据传参来判断读取的方式。
    支持限制读取条数 (data_length)，针对 jsonl, csv, xlsx 进行了内存/IO优化。

    Args:
        file_name (str|Path): 文件路径
        output_type (Literal["list", "dict", "set"]): 返回容器类型
        file_type (str): 文件后缀，如 `json`, `jsonl`, `xlsx`, `csv`
        main_key_column (str): 当返回为dict/set时，作为主键的列名
        encoding (str): 文件编码方式
        disable_tqdm (bool): 是否关闭进度条
        data_length (int): 读取的数据条数（支持 jsonl/csv/xlsx 的局部读取优化）
        process_fn (Callable): 支持读取时直接做处理
        **kwargs: 
            - sheet_name (str): 读取xlsx时指定，默认为第一个。传 "all" 读取所有 sheet。
    """
    
    # 1. 路径与类型预处理
    if isinstance(file_name, str):
        file_name = Path(file_name)
        
    if file_type is None:
        file_type = file_name.suffix.lstrip(".").lower()

    # 2. 初始化容器
    match output_type:
        case "list":
            return_data = []
        case "dict":
            return_data = {}
        case "set":
            return_data = set()
        case _:
            raise RuntimeError(f"output_type 参数错误: {output_type}。仅允许 `list`, `dict`, `set`")

    # --- 内部核心逻辑：统一的数据添加器 ---
    # 定义这个闭包函数，避免在每个 case 里重复写 if list/elif dict/elif set
    def _add_item(item: Any):
        """处理 process_fn 并将数据添加到容器"""
        nonlocal return_data
        
        # 1. 如果有预处理函数，先执行转换
        if process_fn:
            item = process_fn(item)
            # 如果函数返回 None，则跳过该条数据（起到过滤作用）
            if item is None:
                return

        # 2. 添加到容器
        if isinstance(return_data, list):
            return_data.append(item)
            
        elif isinstance(return_data, dict):
            # Dict 必须有 main_key_column，且 item 必须是 dict 结构（或者是拥有该属性的对象）
            if not main_key_column:
                raise RuntimeError("output_type='dict' 时必须指定 main_key_column")
            if main_key_column not in item:
                raise RuntimeError(f"数据缺少 main_key_column='{main_key_column}'\n数据内容: {item}")
            return_data[item[main_key_column]] = item
            
        elif isinstance(return_data, set):
            # Set 逻辑升级：
            # 情况 A: 指定了 key，从 item (dict) 中提取 value 加入集合
            if main_key_column:
                if isinstance(item, dict) and main_key_column in item:
                    return_data.add(item[main_key_column])
                else:
                    raise RuntimeError(f"数据缺少 main_key_column='{main_key_column}'\n数据内容: {item}")
            # 情况 B: 没指定 key，直接把 item 加入集合 (适用于 process_fn 直接返回字符串/数字的情况)
            else:
                if isinstance(item, str) or isinstance(item, int):
                    return_data.add(item)
                else:
                    raise RuntimeError(f"返回set集合时，若需要使用process_fn，则需要返回str 或 int对象")

    # 3. 根据文件类型分发处理
    match file_type:
        # ---------------- JSONL (流式优化) ----------------
        case "jsonl":
            with file_name.open("r", encoding=encoding) as f:
                # 使用 islice 实现流式读取，不加载全文件，读够即停
                # 注意：如果 data_length 为 None，islice(f, None) 会读取全部
                iterator = islice(f, data_length)
                
                # 为了 tqdm 显示进度，如果知道 data_length，则传入 total
                total_count = data_length if data_length else _get_line_count(file_name)
                
                for line in tqdm(iterator, total=total_count, disable=disable_tqdm):
                    if line := line.strip():
                        item = json.loads(line)
                        _add_item(item)
                        
                        # 双重保险：对于 dict/set 去重后可能数量变少，
                        # 但 jsonl 流式读取通常控制的是“读取行数”而非“结果数量”。
                        # 这里以“读取行数”为准，islice 已经控制了，不需要额外 break。

        # ---------------- CSV (Pandas nrows 优化) ----------------
        case "csv":
            
            # 利用 pd.read_csv 的 nrows 参数只读取前 N 行
            df = pd.read_csv(file_name, encoding=encoding, nrows=data_length, **kwargs)
            
            # 转换由于 pandas iterrows 较慢，直接转 dict list 处理更快
            records = df.to_dict(orient="records")
            
            for row in tqdm(records, disable=disable_tqdm):
                _add_item(row)

        # ---------------- XLSX (Pandas nrows + Sheet 逻辑) ----------------
        case "xlsx" | "xls":
            
            # 处理 sheet_name 逻辑
            sheet_name_arg = kwargs.pop("sheet_name", 0) # 默认读第一个
            if sheet_name_arg == "all":
                sheet_name_arg = None # pandas 传 None 会读取所有 sheet 返回 dict
            
            # 读取数据 (利用 nrows 优化)
            dfs_result = pd.read_excel(file_name, sheet_name=sheet_name_arg, nrows=data_length, **kwargs)
            
            all_records = []
            
            # 如果读取了所有 sheet (返回的是 dict: {sheet_name: df})
            if isinstance(dfs_result, dict):
                for sheet_name, df in dfs_result.items():
                    # 注意：如果是 dict 模式且多个 sheet 有重复 key，后读取的会覆盖前面的
                    all_records.extend(df.to_dict(orient="records"))
                    # 如果总数已经够了（针对多 sheet 的情况，可能需要累加判断，这里简单处理）
                    if data_length and len(all_records) >= data_length:
                        all_records = all_records[:data_length]
                        break
            else:
                # 单个 sheet (返回的是 DataFrame)
                all_records = dfs_result.to_dict(orient="records")

            for row in tqdm(all_records, disable=disable_tqdm):
                _add_item(row)

        # ---------------- JSON (标准库限制) ----------------
        case "json":
            with file_name.open("r", encoding=encoding) as f:
                # 'item' 指示 ijson 解析根级数组中的每个元素
                # 这要求 JSON 文件的根必须是列表 [ ... ]
                try:
                    iterator = ijson.items(f, 'item')
                    
                    # 1. 如果指定了读取条数，使用 islice 进行切片 (读够即停)
                    if data_length:
                        iterator = islice(iterator, data_length)
                        total_count = data_length
                    else:
                        # 全量读取时，ijson 无法预知总条数，tqdm 只能显示处理速度
                        total_count = None 

                    # 2. 迭代处理
                    for row in tqdm(iterator, total=total_count, disable=disable_tqdm):
                        _add_item(row)
                        
                except ijson.common.IncompleteJSONError:
                    # 处理可能的 JSON 格式错误或文件截断
                    raise RuntimeError("JSON 文件格式错误或不完整")

        case _:
            raise RuntimeError(f"不支持的文件格式: {file_type}。请检查后缀或显式传入 file_type")

    return return_data

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
                    f.write(json.dumps(item, ensure_ascii=ensure_ascii, default=str) + "\n")
            base_logger.info(f"文件保存至 {file_name.resolve(strict=True)} ")
        case "json":
            with file_name.open("w", encoding=encoding) as f:
                json.dump(data, f, ensure_ascii=ensure_ascii, indent=json_indent, default=str)
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
                content = json.dumps(result, ensure_ascii=ensure_ascii, default=str)
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