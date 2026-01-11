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
    
@pytest.fixture
def sample_xlsx_file(temp_dir, sample_data):
    """
    (可选) 根据 sample_data 生成 xlsx 文件
    需要安装 pandas 和 openpyxl
    """
    try:
        import pandas as pd
    except ImportError:
        return None

    file_path = temp_dir / "test.xlsx"
    df = pd.DataFrame(sample_data)
    
    # 创建包含两个 sheet 的 Excel，用于测试 multi-sheet 读取
    with pd.ExcelWriter(file_path) as writer:
        df.to_excel(writer, sheet_name="Sheet1", index=False)
        # 复制一份数据到 Sheet2，ID 加 100 用于区分
        df2 = df.copy()
        df2["id"] = df2["id"] + 100
        df2.to_excel(writer, sheet_name="Sheet2", index=False)
        
    return file_path


@pytest.fixture
def complex_jsonl_file(temp_dir):
    """生成一个包含嵌套结构的复杂 JSONL 文件"""
    file_path = temp_dir / "complex.jsonl"
    data = [
        # ID 1: Positive
        {
            "id": 101, 
            "label": "positive", 
            "message": [{"role": "user", "content": "这是第一个好评"}]
        },
        # ID 2: Negative
        {
            "id": 102, 
            "label": "negative", 
            "message": [{"role": "user", "content": "这是差评"}]
        },
        # ID 3: Positive (重复内容，用于测试 Set)
        {
            "id": 103, 
            "label": "positive", 
            "message": [{"role": "user", "content": "这是第一个好评"}] 
        }
    ]
    
    with open(file_path, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    return file_path