import pytest
from bedrockx.process import filter_fn, drop_duplicates, remove_columns


class TestFilterData:
    """测试 filter_fn 函数"""
    
    def test_filter_basic(self):
        """测试基本过滤功能"""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"}
        ]
        filter_set = {2}
        
        result = filter_fn(data, filter_set, "id")
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 3
    
    def test_filter_empty_set(self):
        """测试空过滤集"""
        data = [{"id": 1}, {"id": 2}]
        filter_set = set()
        
        result = filter_fn(data, filter_set, "id")
        assert len(result) == 2
    
    def test_filter_all(self):
        """测试过滤所有数据"""
        data = [{"id": 1}, {"id": 2}]
        filter_set = {1, 2}
        
        result = filter_fn(data, filter_set, "id")
        assert len(result) == 0
    
    def test_filter_missing_key(self):
        """测试缺少关键字段"""
        data = [{"name": "Alice"}]
        filter_set = {1}
        
        with pytest.raises(RuntimeError, match="data中没有字段"):
            filter_fn(data, filter_set, "id")
    
    def test_filter_string_keys(self):
        """测试字符串类型的键"""
        data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Charlie", "age": 35}
        ]
        filter_set = {"Bob"}
        
        result = filter_fn(data, filter_set, "name")
        assert len(result) == 2
        assert result[0]["name"] == "Alice"


class TestDropDuplicates:
    """测试 drop_duplicates 函数"""
    
    def test_drop_duplicates_basic(self):
        """测试基本去重功能"""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 1, "name": "Alice Copy"}
        ]
        
        result = drop_duplicates(data, "id")
        assert len(result) == 2
        assert result[0]["name"] == "Alice"
        assert result[1]["name"] == "Bob"
    
    def test_drop_duplicates_no_duplicates(self):
        """测试没有重复的情况"""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
        
        result = drop_duplicates(data, "id")
        assert len(result) == 2
    
    def test_drop_duplicates_all_same(self):
        """测试所有数据都重复"""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 1, "name": "Alice2"},
            {"id": 1, "name": "Alice3"}
        ]
        
        result = drop_duplicates(data, "id")
        assert len(result) == 1
        assert result[0]["name"] == "Alice"
    
    def test_drop_duplicates_missing_key(self):
        """测试缺少关键字段（应该跳过）"""
        data = [
            {"id": 1, "name": "Alice"},
            {"name": "Bob"},  # 缺少 id
            {"id": 2, "name": "Charlie"}
        ]
        
        result = drop_duplicates(data, "id")
        assert len(result) == 2  # 跳过了缺少 id 的项
    
    def test_drop_duplicates_empty_list(self):
        """测试空列表"""
        result = drop_duplicates([], "id")
        assert result == []

    def test_filter_with_process_fn_transformation(self):
        """新特性：使用 process_fn 进行数据转换后过滤 (例如: 过滤偶数ID)"""
        data = [
            {"id": 1, "val": 10}, 
            {"id": 2, "val": 20}, 
            {"id": 3, "val": 30}
        ]
        # 逻辑：提取 id，并在 filter_set 中查找
        # 假设我们要过滤掉 id 为 2 的数据
        process = lambda x: x['id']
        filter_set = {2}
        
        result = filter_fn(data, filter_set, "", process_fn=process)
        assert len(result) == 2
        assert result[1]["id"] == 3

    def test_filter_with_process_fn_nested(self):
        """新特性：使用 process_fn 处理嵌套字典"""
        data = [
            {"meta": {"uuid": "u1"}, "content": "A"},
            {"meta": {"uuid": "u2"}, "content": "B"},
            {"meta": {"uuid": "u3"}, "content": "C"}
        ]
        filter_set = {"u2"}
        # 提取嵌套的 uuid 进行过滤
        process = lambda item: item["meta"]["uuid"]
        
        result = filter_fn(data, filter_set, "irrelevant_key", process_fn=process)
        assert len(result) == 2
        assert result[1]["content"] == "C"

    def test_filter_process_fn_priority(self):
        """逻辑：process_fn 优先级应高于 main_key_column"""
        data = [{"id": 1, "real_id": 99}]
        filter_set = {99} # 我们想根据 real_id 过滤
        
        # 即使 main_key_column 填了 "id" (值为1)，
        # 但 process_fn 返回了 item["real_id"] (值为99)，
        # 99 在 filter_set 中，所以应该被过滤掉。
        result = filter_fn(
            data, 
            filter_set, 
            "id", 
            process_fn=lambda x: x["real_id"]
        )
        assert len(result) == 0
        
    def test_drop_with_process_fn_case_insensitive(self):
        """新特性：使用 process_fn 进行忽略大小写的去重"""
        data = [
            {"name": "Alice"},
            {"name": "alice"}, # 视为重复
            {"name": "BOB"}
        ]
        
        # 将名字转为小写作为去重依据
        process = lambda item: item["name"].lower()
        
        result = drop_duplicates(data, "name", process_fn=process)
        assert len(result) == 2
        assert result[0]["name"] == "Alice"
        assert result[1]["name"] == "BOB"

    def test_drop_with_process_fn_composite_key(self):
        """新特性：使用 process_fn 进行复合键去重 (多字段组合)"""
        data = [
            {"x": 1, "y": 1, "val": "A"},
            {"x": 1, "y": 2, "val": "B"},
            {"x": 1, "y": 1, "val": "C (Duplicate of A based on x,y)"}
        ]
        
        # 根据 (x, y) 的元组进行去重
        process = lambda item: (item["x"], item["y"])
        
        result = drop_duplicates(data, "irrelevant", process_fn=process)
        assert len(result) == 2
        assert result[0]["val"] == "A"
        assert result[1]["val"] == "B"

    def test_drop_process_fn_priority(self):
        """逻辑：process_fn 存在时，不检查 main_key_column 是否存在"""
        data = [
            {"val": 10}, # 缺少 "id"
            {"val": 10}, 
            {"val": 20}
        ]
        
        # 虽然 data 中没有 "id"，但提供了 process_fn，
        # 所以代码不应进入 `elif main_key_column not in item` 分支，
        # 也不应该 warning/skip，而是正常去重。
        process = lambda x: x["val"]
        
        result = drop_duplicates(data, "id", process_fn=process)
        assert len(result) == 2
        assert result[0]["val"] == 10
        assert result[1]["val"] == 20
class TestRemoveColumns:
    """测试 remove_columns 函数"""
    
    def test_remove_single_column(self):
        """测试删除单个列"""
        data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30}
        ]
        
        result = remove_columns(data, "age")
        assert len(result) == 2
        assert "age" not in result[0]
        assert "age" not in result[1]
        assert "name" in result[0]
    
    def test_remove_multiple_columns(self):
        """测试删除多个列"""
        data = [
            {"id": 1, "name": "Alice", "age": 25, "city": "NYC"},
            {"id": 2, "name": "Bob", "age": 30, "city": "LA"}
        ]
        
        result = remove_columns(data, ["age", "city"])
        assert "age" not in result[0]
        assert "city" not in result[0]
        assert "name" in result[0]
    
    def test_remove_nonexistent_column(self):
        """测试删除不存在的列"""
        data = [{"id": 1, "name": "Alice"}]
        
        result = remove_columns(data, "age")
        assert len(result) == 1
        assert result[0] == {"id": 1, "name": "Alice"}
    
    def test_remove_from_empty_list(self):
        """测试从空列表删除"""
        result = remove_columns([], "id")
        assert result == []