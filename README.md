# bedrockx

<div align="center">

[![PyPI version](https://badge.fury.io/py/bedrockx.svg)](https://pypi.org/project/bedrockx/)
[![Python Version](https://img.shields.io/pypi/pyversions/bedrockx.svg)](https://pypi.org/project/bedrockx/)
[![License](https://img.shields.io/github/license/ciaoyizhen/bedrockx.svg)](https://github.com/ciaoyizhen/bedrockx/blob/main/LICENSE)
[![Downloads](https://pepy.tech/badge/bedrockx)](https://pepy.tech/project/bedrockx)

**一个强大的 Python 工具库，让数据处理变得简单高效**

[快速开始](#快速开始) • [功能特性](#功能特性) • [文档](#详细文档) • [示例](#使用示例) • [贡献](#贡献)

</div>

---

## 📖 简介

bedrockx 是一个专为数据处理工作流设计的 Python 工具库，提供了文件操作、数据处理、多线程加速等常用功能。无论您是在处理大规模数据集，还是需要快速实现数据 ETL，bedrockx 都能显著提升您的工作效率。

### ✨ 为什么选择 bedrockx？

- 🚀 **简单易用**：统一的 API 设计，一行代码完成复杂操作
- 📁 **多格式支持**：支持 JSON、JSONL、CSV、Excel 等多种数据格式
- ⚡ **高性能**：内置多线程处理，轻松应对大规模数据
- 🔧 **灵活可扩展**：提供基类和装饰器，方便自定义扩展
- 📝 **完善的日志**：集成日志管理，方便调试和监控
- 🧪 **测试覆盖**：完整的单元测试，保证代码质量

---

## 🚀 快速开始

### 安装

```bash
pip install bedrockx
```

### 5 分钟上手

```python
from bedrockx import read_file, save_file, filter_data

# 1. 读取数据（自动识别格式）
data = read_file("data.jsonl")

# 2. 处理数据
filtered = filter_data(data, filter_set={1, 2}, main_key_column="id")

# 3. 保存结果
save_file("output.json", filtered)
```

就是这么简单！🎉

---

## 🎯 功能特性

### 📂 文件操作

#### 统一的文件读取接口

支持多种格式（JSON、JSONL、CSV、Excel）和多种输出类型（list、dict、set）：

```python
from bedrockx import read_file

# 读取为 list
data = read_file("data.jsonl")

# 读取为 dict（以 id 为键）
data_dict = read_file("data.json", output_type="dict", main_key_column="id")

# 读取为 set（只保留指定列的值）
id_set = read_file("data.csv", output_type="set", main_key_column="id")
```

#### 智能文件保存

自动创建目录，支持多种格式：

```python
from bedrockx import save_file

# 自动根据后缀名保存
save_file("output/result.jsonl", data)
save_file("output/result.xlsx", data)
save_file("output/result.csv", data)
```

#### 装饰器式文件追加

边处理边保存，无需缓存大量数据：

```python
from bedrockx import return_to_jsonl

@return_to_jsonl("results.jsonl")
def process_item(item):
    # 处理逻辑
    return {"id": item["id"], "result": item["value"] * 2}

for item in data:
    process_item(item)  # 自动追加到文件
```

### 🔄 数据处理

#### 数据过滤

```python
from bedrockx import filter_data

# 过滤掉已处理的数据
processed_ids = {1, 2, 3}
new_data = filter_data(data, processed_ids, main_key_column="id")
```

#### 数据去重

```python
from bedrockx import drop_duplicates

# 基于 id 字段去重
unique_data = drop_duplicates(data, main_key_column="id")
```

#### 列删除

```python
from bedrockx import remove_columns

# 删除敏感字段
clean_data = remove_columns(data, ["password", "token"])
```

### ⚡ 多线程处理

使用多线程加速数据处理，支持边处理边保存：

```python
from bedrockx import BaseMultiThreading

class MyProcessor(BaseMultiThreading):
    def single_data_process(self, item):
        # 定义单个数据的处理逻辑
        result = expensive_operation(item)
        return result

# 使用 4 个线程并发处理
processor = MyProcessor(max_workers=4, save_path="output.jsonl")
processor(data)  # 自动并发处理并保存
```

### 🛠️ 工具函数

#### 单例模式

```python
from bedrockx import singleton

@singleton
class Config:
    def __init__(self):
        self.settings = {}

# 全局唯一实例
config1 = Config()
config2 = Config()
assert config1 is config2  # True
```

#### 日志管理

```python
from bedrockx import LoggerManager

# 创建日志管理器
logger = LoggerManager("logs/app.log", level="INFO")

logger.info("程序启动")
logger.warning("警告信息")
logger.error("错误信息")
```

---

## 📚 详细文档

### API 参考

#### `read_file()`

读取各种格式的文件。

**参数：**

- `file_name` (str | Path): 文件路径
- `output_type` (str): 返回类型，可选 `"list"`, `"dict"`, `"set"`，默认 `"list"`
- `file_type` (str): 文件类型，可选 `"json"`, `"jsonl"`, `"csv"`, `"xlsx"`，默认自动识别
- `main_key_column` (str): 当 `output_type="dict"` 或 `"set"` 时，指定用作键的字段
- `encoding` (str): 文件编码，默认 `"utf-8"`
- `disable_tqdm` (bool): 是否禁用进度条，默认 `False`
- `**kwargs`: 传递给底层读取函数的其他参数

**返回：** list | dict | set

**示例：**

```python
# 基本用法
data = read_file("data.jsonl")

# 读取为字典
data_dict = read_file(
    "data.json",
    output_type="dict",
    main_key_column="id"
)

# 读取 Excel 特定 sheet
data = read_file(
    "data.xlsx",
    sheet_name="Sheet1"
)

# 读取 CSV 指定编码
data = read_file(
    "data.csv",
    encoding="gbk"
)
```

#### `save_file()`

保存数据到文件。

**参数：**

- `file_name` (str | Path): 保存路径
- `data` (list): 要保存的数据
- `file_type` (str): 文件类型，默认根据后缀自动识别
- `encoding` (str): 文件编码，默认 `"utf-8"`
- `ensure_ascii` (bool): JSON 格式是否转义非 ASCII 字符，默认 `False`
- `json_indent` (int): JSON 缩进，默认 `4`
- `pd_index` (bool): DataFrame 是否保存索引，默认 `False`
- `**kwargs`: 传递给底层保存函数的其他参数

**示例：**

```python
# 基本用法
save_file("output.jsonl", data)

# 保存为压缩格式的 JSON
save_file("output.json", data, json_indent=None)

# Excel 保存特定 sheet
save_file("output.xlsx", data, sheet_name="Results")
```

#### `filter_data()`

过滤数据。

**参数：**

- `data` (list[dict]): 待过滤的数据
- `filter_set` (set): 要过滤掉的值的集合
- `main_key_column` (str): 用于过滤的字段名

**返回：** list[dict]

**示例：**

```python
# 过滤已处理的 ID
processed_ids = {1, 2, 3, 4, 5}
new_data = filter_data(data, processed_ids, "id")
```

#### `drop_duplicates()`

去除重复数据。

**参数：**

- `data` (list[dict]): 待去重的数据
- `main_key_column` (str): 用于判断重复的字段名

**返回：** list[dict]

**示例：**

```python
# 基于 user_id 去重
unique_users = drop_duplicates(data, "user_id")
```

#### `remove_columns()`

删除指定列。

**参数：**

- `data` (list[dict]): 数据
- `key_list` (list | str): 要删除的字段名（列表或单个字符串）

**返回：** list[dict]

**示例：**

```python
# 删除敏感字段
clean_data = remove_columns(data, ["password", "email", "phone"])

# 删除单个字段
clean_data = remove_columns(data, "temp_field")
```

#### `BaseMultiThreading`

多线程处理基类。

**参数：**

- `max_workers` (int): 线程数
- `save_path` (str | Path): 结果保存路径
- `file_type` (str): 保存文件类型，默认根据后缀自动识别

**方法：**

- `single_data_process(item: dict) -> dict`: 需要子类实现，定义单个数据的处理逻辑

**示例：**

```python
from bedrockx import BaseMultiThreading
import time

class MyProcessor(BaseMultiThreading):
    def single_data_process(self, item):
        # 模拟耗时操作
        time.sleep(0.1)
        result = {
            "id": item["id"],
            "processed": True,
            "value": item["value"] * 2
        }
        return result

# 使用 10 个线程处理数据
processor = MyProcessor(max_workers=10, save_path="results.jsonl")
processor(large_dataset)  # 自动并发处理
```

#### `add_suffix_file()`

为文件名添加后缀。

**参数：**

- `file_path` (str | Path): 原始文件路径
- `suffix` (str): 要添加的后缀
- `sep` (str): 分隔符，默认 `"_"`

**返回：** Path

**示例：**

```python
from bedrockx import add_suffix_file

# 生成带后缀的文件名
output_path = add_suffix_file("data.jsonl", "processed")
# 结果: Path("data_processed.jsonl")

# 自定义分隔符
output_path = add_suffix_file("data.jsonl", "v2", sep="-")
# 结果: Path("data-v2.jsonl")
```

#### `@return_to_jsonl`

装饰器，自动将函数返回值追加到 JSONL 文件。

**参数：**

- `file_path` (str | Path): 保存路径
- `encoding` (str): 文件编码，默认 `"utf-8"`
- `ensure_ascii` (bool): 是否转义非 ASCII 字符，默认 `False`

**示例：**

```python
from bedrockx import return_to_jsonl

@return_to_jsonl("processed_data.jsonl")
def process_record(record):
    # 处理单条记录
    return {
        "id": record["id"],
        "result": some_computation(record)
    }

# 批量处理，自动保存
for record in records:
    process_record(record)  # 每次调用都追加到文件
```

#### `@singleton`

单例模式装饰器，确保类只有一个实例。

**示例：**

```python
from bedrockx import singleton

@singleton
class DatabaseConnection:
    def __init__(self):
        self.conn = create_connection()
    
    def query(self, sql):
        return self.conn.execute(sql)

# 无论创建多少次，都是同一个实例
db1 = DatabaseConnection()
db2 = DatabaseConnection()
assert db1 is db2  # True
```

#### `LoggerManager`

日志管理器，基于 loguru 的封装。

**参数：**

- `log_path` (str | None): 日志文件路径，`None` 表示不保存文件
- `level` (str): 日志级别，默认 `"INFO"`
- `rotation` (str): 日志轮转大小，默认 `"10 MB"`
- `retention` (str): 日志保留时间，默认 `"7 days"`
- `compression` (str): 压缩格式，默认 `"zip"`
- `enqueue` (bool): 是否启用异步写入，默认 `True`
- `console` (bool): 是否输出到控制台，默认 `True`

**方法：**

- `debug(msg)`: 调试信息
- `info(msg)`: 一般信息
- `warning(msg)`: 警告信息
- `error(msg)`: 错误信息
- `critical(msg)`: 严重错误
- `exception(msg)`: 异常信息（带堆栈）

**示例：**

```python
from bedrockx import LoggerManager

# 创建日志管理器
logger = LoggerManager(
    log_path="logs/app.log",
    level="DEBUG",
    rotation="50 MB",
    retention="30 days"
)

logger.debug("调试信息")
logger.info("程序启动成功")
logger.warning("配置文件未找到，使用默认配置")
logger.error("数据库连接失败")

# 记录异常
try:
    risky_operation()
except Exception as e:
    logger.exception("操作失败")
```

---

## 💡 使用示例

### 示例 1: 数据清洗流程

```python
from bedrockx import read_file, save_file, drop_duplicates, remove_columns

# 1. 读取原始数据
raw_data = read_file("raw_data.csv")
print(f"原始数据: {len(raw_data)} 条")

# 2. 去重
unique_data = drop_duplicates(raw_data, main_key_column="user_id")
print(f"去重后: {len(unique_data)} 条")

# 3. 删除不需要的字段
clean_data = remove_columns(unique_data, ["temp_col", "debug_info"])

# 4. 保存清洗后的数据
save_file("cleaned_data.jsonl", clean_data)
print("数据清洗完成！")
```

### 示例 2: 增量数据处理

```python
from bedrockx import read_file, save_file, filter_data

# 读取已处理的 ID
processed_ids = read_file(
    "processed.jsonl",
    output_type="set",
    main_key_column="id"
)

# 读取新数据
new_data = read_file("new_batch.jsonl")

# 过滤已处理的数据
to_process = filter_data(new_data, processed_ids, main_key_column="id")
print(f"需要处理: {len(to_process)} 条新数据")

# 处理并保存...
```

### 示例 3: 多线程 API 调用

```python
from bedrockx import BaseMultiThreading
import requests

class APIProcessor(BaseMultiThreading):
    def __init__(self, *args, api_key=None, **kwargs):
        self.api_key = api_key
        super().__init__(*args, **kwargs)
    
    def post_init(self, **kwargs):
        # 在这里可以初始化其他资源
        self.api_key = kwargs.get('api_key')
    
    def single_data_process(self, item):
        # 调用 API 处理单条数据
        response = requests.post(
            "https://api.example.com/process",
            json=item,
            headers={"Authorization": f"Bearer {self.api_key}"}
        )
        return response.json()

# 使用 20 个线程并发处理
processor = APIProcessor(
    max_workers=20,
    save_path="api_results.jsonl",
    api_key="your-api-key"
)

data = read_file("input_data.jsonl")
processor(data)  # 自动并发调用 API 并保存结果
```

### 示例 4: 实时数据处理

```python
from bedrockx import return_to_jsonl, LoggerManager

logger = LoggerManager("logs/processing.log")

@return_to_jsonl("processed_stream.jsonl")
def process_stream_data(data):
    try:
        # 数据转换
        result = {
            "timestamp": data["timestamp"],
            "value": data["raw_value"] * 1.5,
            "status": "processed"
        }
        logger.info(f"处理成功: {data['id']}")
        return result
    except Exception as e:
        logger.error(f"处理失败: {data.get('id')}, 错误: {e}")
        return None

# 模拟流式数据处理
while True:
    data = receive_data()  # 从某个数据源接收
    process_stream_data(data)  # 自动保存到文件
```

### 示例 5: 数据格式转换

```python
from bedrockx import read_file, save_file

# CSV 转 JSON
data = read_file("data.csv")
save_file("data.json", data)

# Excel 转 JSONL
data = read_file("data.xlsx", sheet_name="Sheet1")
save_file("data.jsonl", data)

# JSONL 转 Excel
data = read_file("data.jsonl")
save_file("data.xlsx", data)
```

### 示例 6: 配置管理

```python
from bedrockx import singleton
import json

@singleton
class Config:
    def __init__(self, config_path="config.json"):
        with open(config_path) as f:
            self.settings = json.load(f)
    
    def get(self, key, default=None):
        return self.settings.get(key, default)

# 在任何地方都可以获取同一个配置实例
config = Config()
api_key = config.get("api_key")
```

---

## 🔧 高级用法

### 自定义数据处理管道

```python
from bedrockx import (
    read_file, save_file, filter_data, 
    drop_duplicates, remove_columns, LoggerManager
)

class DataPipeline:
    def __init__(self, log_path="logs/pipeline.log"):
        self.logger = LoggerManager(log_path)
    
    def run(self, input_file, output_file, processed_ids_file=None):
        # 读取数据
        self.logger.info(f"读取数据: {input_file}")
        data = read_file(input_file)
        self.logger.info(f"读取完成: {len(data)} 条")
        
        # 去重
        data = drop_duplicates(data, "id")
        self.logger.info(f"去重后: {len(data)} 条")
        
        # 过滤已处理数据
        if processed_ids_file:
            processed = read_file(
                processed_ids_file,
                output_type="set",
                main_key_column="id"
            )
            data = filter_data(data, processed, "id")
            self.logger.info(f"过滤后: {len(data)} 条")
        
        # 删除临时字段
        data = remove_columns(data, ["_temp", "_debug"])
        
        # 保存结果
        save_file(output_file, data)
        self.logger.info(f"保存完成: {output_file}")
        
        return data

# 使用管道
pipeline = DataPipeline()
pipeline.run("input.jsonl", "output.jsonl", "processed.jsonl")
```

### 扩展 BaseMultiThreading

```python
from bedrockx import BaseMultiThreading, LoggerManager
import time

class RetryableProcessor(BaseMultiThreading):
    def post_init(self, max_retries=3, **kwargs):
        self.max_retries = max_retries
        self.logger = LoggerManager("logs/processor.log")
    
    def single_data_process(self, item):
        for attempt in range(self.max_retries):
            try:
                result = self.process_with_retry(item)
                return result
            except Exception as e:
                self.logger.warning(
                    f"处理失败 (尝试 {attempt + 1}/{self.max_retries}): {e}"
                )
                if attempt == self.max_retries - 1:
                    self.logger.error(f"最终失败: {item.get('id')}")
                    raise
                time.sleep(2 ** attempt)  # 指数退避
    
    def process_with_retry(self, item):
        # 实际的处理逻辑
        raise NotImplementedError

# 使用
class MyRetryProcessor(RetryableProcessor):
    def process_with_retry(self, item):
        # 可能失败的操作
        return call_unreliable_api(item)

processor = MyRetryProcessor(
    max_workers=5,
    save_path="output.jsonl",
    max_retries=5
)
```

---

## 🤝 贡献

欢迎贡献代码、报告问题或提出新功能建议！

### 开发环境设置

```bash
# 克隆仓库
git clone https://github.com/ciaoyizhen/bedrockx.git
cd bedrockx

# 安装开发依赖
pip install -e ".[dev]"

# 运行测试
pytest tests/

# 检查代码覆盖率
pytest tests/ --cov=bedrockx --cov-report=html
```

### 提交 PR 的步骤

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

### 代码规范

- 遵循 PEP 8 代码风格
- 添加类型注解
- 编写完整的文档字符串
- 为新功能添加测试

---

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

---

## 🙏 致谢

感谢所有贡献者和使用 bedrockx 的开发者！

特别感谢以下开源项目：

- [pandas](https://pandas.pydata.org/) - 数据处理
- [loguru](https://github.com/Delgan/loguru) - 日志管理
- [tqdm](https://github.com/tqdm/tqdm) - 进度条

---

## 📞 联系方式

- **作者**: ciaoyizhen
- **邮箱**: yizhen.ciao@gmail.com
- **GitHub**: [@ciaoyizhen](https://github.com/ciaoyizhen)
- **Issue Tracker**: [GitHub Issues](https://github.com/ciaoyizhen/bedrockx/issues)


<div align="center">

**[⬆ 返回顶部](#bedrockx)**

Made with ❤️ by ciaoyizhen

</div>