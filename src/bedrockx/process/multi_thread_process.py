# -*- encoding: utf-8 -*-
# @Time    :   2025/10/11 22:16:26
# @File    :   MultiThreadProcess.py
# @Author  :   ciaoyizhen
# @Contact :   yizhen.ciao@gmail.com
# @Function:   多线程的消费者生产者进程处理
import json
from pathlib import Path
from concurrent.futures import as_completed, ThreadPoolExecutor
from ..utils import base_logger
from ..file import save_file, read_file
from tqdm import tqdm



class BaseMultiThreading():
    """
    基类, 实现多线程的消费者生产者的处理, 实现边处理边存储
    """
    def __init__(self, max_workers:int, save_path: str|Path=None, *, file_type:str|Path=None, **kwargs):
        """_summary_

        Args:
            max_workers (int): 并发数
            single_file_size (int): 临时存储时，单个文件的大小
            save_path (str|Path): 最终完整保存的文件
            file_type (str|Path): 文件存储类型
        """
        self.max_workers = max_workers
        self.save_path = Path(save_path)
        self.file_type = file_type
        
        if self.file_type is None:
            self.file_type = self.save_path.suffix.lstrip(".")
            
        if self.file_type not in {"json", "jsonl", "xlsx", "csv"}:
            raise RuntimeError(f"传入的file_type不符合要求或你的文件后缀不符合要求")

        self.post_init(**kwargs)
    
    def post_init(self, **kwargs):
        pass
    
    def single_data_process(self, item:dict)->dict:
        """
        这个函数实现单个数据怎么处理，输入是一个数据，进行处理，返回一个数据
        需要用户自定义实现
        """
        raise NotImplementedError(f"未实现函数 single_data_process, 该函数需要解决每个数据要怎么")
            
    
    def __call__(self, data:list):
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="线程处理数据") as exec, \
            tqdm(total=len(data), desc=f"{self.max_workers}并发处理中") as p_bar, \
            open(self.save_path, "w", encoding="utf-8") as f:
            try:
                futures_list = []
                for item in data:
                    future = exec.submit(self.single_data_process, item)
                    future.add_done_callback(lambda x: p_bar.update(1))
                    futures_list.append(future)
                
                for future in as_completed(futures_list):
                    result = future.result()
                    result = json.dumps(result, ensure_ascii=False)
                    f.write(result + "\n")
                    f.flush()
            except NotImplementedError:
                raise
            except KeyboardInterrupt:
                exec.shutdown(cancel_futures=True)
                exit()
            except Exception:
                import traceback
                base_logger.error(traceback.format_exc())
                