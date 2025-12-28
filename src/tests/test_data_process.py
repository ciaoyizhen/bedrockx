import pytest
from bedrockx.process import filter_data, drop_duplicates, remove_columns


class TestFilterData:
    """测试 filter_data 函数"""
    
    def test_filter_basic(self):
        """测试基本过滤功能"""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"}
        ]
        filter_set = {2}
        
        result = filter_data(data, filter_set, "id")
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 3
    
    def test_filter_empty_set(self):
        """测试空过滤集"""
        data = [{"id": 1}, {"id": 2}]
        filter_set = set()
        
        result = filter_data(data, filter_set, "id")
        assert len(result) == 2
    
    def test_filter_all(self):
        """测试过滤所有数据"""
        data = [{"id": 1}, {"id": 2}]
        filter_set = {1, 2}
        
        result = filter_data(data, filter_set, "id")
        assert len(result) == 0
    
    def test_filter_missing_key(self):
        """测试缺少关键字段"""
        data = [{"name": "Alice"}]
        filter_set = {1}
        
        with pytest.raises(RuntimeError, match="data中没有字段"):
            filter_data(data, filter_set, "id")
    
    def test_filter_string_keys(self):
        """测试字符串类型的键"""
        data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Charlie", "age": 35}
        ]
        filter_set = {"Bob"}
        
        result = filter_data(data, filter_set, "name")
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