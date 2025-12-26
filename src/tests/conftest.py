import pytest
import json
import tempfile
from pathlib import Path
import sys

# 添加 src 到路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


@pytest.fixture
def temp_dir(tmp_path):
    """创建临时目录"""
    return tmp_path


@pytest.fixture
def sample_jsonl_file(temp_dir):
    """创建测试用的 jsonl 文件"""
    file_path = temp_dir / "test.jsonl"
    data = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},
        {"id": 3, "name": "Charlie", "age": 35}
    ]
    with open(file_path, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    return file_path


@pytest.fixture
def sample_json_file(temp_dir):
    """创建测试用的 json 文件"""
    file_path = temp_dir / "test.json"
    data = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},
        {"id": 3, "name": "Charlie", "age": 35}
    ]
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    return file_path


@pytest.fixture
def sample_csv_file(temp_dir):
    """创建测试用的 csv 文件"""
    file_path = temp_dir / "test.csv"
    content = "id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    return file_path


@pytest.fixture
def sample_data():
    """提供测试数据"""
    return [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},
        {"id": 3, "name": "Charlie", "age": 35}
    ]