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
from bedrockx import read_file, save_file, filter_fn

# 1. 读取数据（自动识别格式）
data = read_file("data.jsonl")

# 2. 处理数据
filtered = filter_fn(data, filter_set={1, 2}, main_key_column="id")

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
from bedrockx import filter_fn

# 过滤掉已处理的数据
processed_ids = {1, 2, 3}
new_data = filter_fn(data, processed_ids, main_key_column="id")
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
processor(data)  # 自动并发处理并时时保存
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

#### 文件名后缀
```
pythonfrom bedrockx import add_suffix_file

# 生成带后缀的文件名
output_path = add_suffix_file("data.jsonl", "processed")
# 结果: Path("data_processed.jsonl")
```

---


---

## 🤝 贡献

欢迎贡献代码、报告问题或提出新功能建议！

### 开发环境设置

```bash
# 克隆仓库
git clone https://github.com/ciaoyizhen/bedrockx.git
cd bedrockx

# 安装开发依赖
uv sync

# 运行测试
pytest src/tests/

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