import pytest
from pathlib import Path
from bedrockx.file import read_file, save_file, add_suffix_file, return_to_jsonl


class TestReadFile:
    """测试 read_file 函数"""
    
    def test_read_jsonl_as_list(self, sample_jsonl_file):
        """测试读取 jsonl 文件为 list"""
        result = read_file(sample_jsonl_file, output_type="list")
        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0]["name"] == "Alice"
    
    def test_read_jsonl_as_dict(self, sample_jsonl_file):
        """测试读取 jsonl 文件为 dict"""
        result = read_file(sample_jsonl_file, output_type="dict", main_key_column="id")
        assert isinstance(result, dict)
        assert len(result) == 3
        assert result[1]["name"] == "Alice"
        assert result[2]["name"] == "Bob"
    
    def test_read_jsonl_as_set(self, sample_jsonl_file):
        """测试读取 jsonl 文件为 set"""
        result = read_file(sample_jsonl_file, output_type="set", main_key_column="id")
        assert isinstance(result, set)
        assert len(result) == 3
        assert 1 in result
        assert 2 in result
    
    def test_read_json_as_list(self, sample_json_file):
        """测试读取 json 文件为 list"""
        result = read_file(sample_json_file, output_type="list")
        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0]["name"] == "Alice"
    
    def test_read_json_as_dict(self, sample_json_file):
        """测试读取 json 文件为 dict"""
        result = read_file(sample_json_file, output_type="dict", main_key_column="id")
        assert isinstance(result, dict)
        assert result[1]["name"] == "Alice"
    
    def test_read_csv_as_list(self, sample_csv_file):
        """测试读取 csv 文件为 list"""
        result = read_file(sample_csv_file, output_type="list")
        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0]["name"] == "Alice"
    
    def test_read_with_path_object(self, sample_jsonl_file):
        """测试使用 Path 对象读取"""
        result = read_file(Path(sample_jsonl_file))
        assert isinstance(result, list)
        assert len(result) == 3
    
    def test_read_with_disable_tqdm(self, sample_jsonl_file):
        """测试禁用进度条"""
        result = read_file(sample_jsonl_file, disable_tqdm=True)
        assert len(result) == 3
    
    def test_read_missing_main_key_raises_error(self, temp_dir):
        """测试缺少 main_key_column 时抛出错误"""
        file_path = temp_dir / "test.jsonl"
        with open(file_path, "w") as f:
            f.write('{"name": "Alice"}\n')
        
        with pytest.raises(RuntimeError, match="对象没有main_key_column"):
            read_file(file_path, output_type="dict", main_key_column="id")
    
    def test_read_invalid_output_type(self, sample_jsonl_file):
        """测试无效的 output_type"""
        with pytest.raises(RuntimeError, match="output_type 传入了一个不可预知的参数"):
            read_file(sample_jsonl_file, output_type="invalid")
    
    def test_read_unknown_file_type(self, temp_dir):
        """测试未知的文件类型"""
        file_path = temp_dir / "test.unknown"
        file_path.touch()
        
        with pytest.raises(RuntimeError, match="无法识别后缀"):
            read_file(file_path)
    
    def test_read_empty_jsonl(self, temp_dir):
        """测试读取空的 jsonl 文件"""
        file_path = temp_dir / "empty.jsonl"
        file_path.write_text("")
        result = read_file(file_path)
        assert result == []


class TestSaveFile:
    """测试 save_file 函数"""
    
    def test_save_jsonl(self, temp_dir, sample_data):
        """测试保存为 jsonl 文件"""
        file_path = temp_dir / "output.jsonl"
        save_file(file_path, sample_data)
        
        assert file_path.exists()
        # 验证内容
        result = read_file(file_path)
        assert len(result) == 3
        assert result[0]["name"] == "Alice"
    
    def test_save_json(self, temp_dir, sample_data):
        """测试保存为 json 文件"""
        file_path = temp_dir / "output.json"
        save_file(file_path, sample_data)
        
        assert file_path.exists()
        result = read_file(file_path)
        assert len(result) == 3
    
    def test_save_csv(self, temp_dir, sample_data):
        """测试保存为 csv 文件"""
        file_path = temp_dir / "output.csv"
        save_file(file_path, sample_data)
        
        assert file_path.exists()
        result = read_file(file_path)
        assert len(result) == 3
    
    def test_save_xlsx(self, temp_dir, sample_data):
        """测试保存为 xlsx 文件"""
        file_path = temp_dir / "output.xlsx"
        save_file(file_path, sample_data)
        
        assert file_path.exists()
        result = read_file(file_path)
        assert len(result) == 3
    
    def test_save_creates_parent_dir(self, temp_dir, sample_data):
        """测试自动创建父目录"""
        file_path = temp_dir / "subdir" / "output.jsonl"
        save_file(file_path, sample_data)
        
        assert file_path.exists()
        assert file_path.parent.exists()
    
    def test_save_with_path_object(self, temp_dir, sample_data):
        """测试使用 Path 对象保存"""
        file_path = Path(temp_dir) / "output.jsonl"
        save_file(file_path, sample_data)
        
        assert file_path.exists()
    
    def test_save_with_encoding(self, temp_dir, sample_data):
        """测试指定编码保存"""
        file_path = temp_dir / "output.jsonl"
        save_file(file_path, sample_data, encoding="utf-8")
        
        assert file_path.exists()
    
    def test_save_invalid_file_type(self, temp_dir, sample_data):
        """测试无效的文件类型"""
        file_path = temp_dir / "output.unknown"
        
        with pytest.raises(RuntimeError, match="保存文件识别"):
            save_file(file_path, sample_data)
    
    def test_save_empty_data(self, temp_dir):
        """测试保存空数据"""
        file_path = temp_dir / "empty.jsonl"
        save_file(file_path, [])
        
        assert file_path.exists()
        result = read_file(file_path)
        assert result == []


class TestAddSuffixFile:
    """测试 add_suffix_file 函数"""
    
    def test_add_suffix_basic(self):
        """测试基本添加后缀功能"""
        result = add_suffix_file("data.jsonl", "response")
        assert result == Path("data_response.jsonl")
    
    def test_add_suffix_with_path_object(self):
        """测试使用 Path 对象"""
        result = add_suffix_file(Path("data.jsonl"), "response")
        assert result == Path("data_response.jsonl")
    
    def test_add_suffix_custom_separator(self):
        """测试自定义分隔符"""
        result = add_suffix_file("data.jsonl", "response", sep="-")
        assert result == Path("data-response.jsonl")
    
    def test_add_suffix_with_path(self):
        """测试带路径的文件"""
        result = add_suffix_file("dir/data.jsonl", "response")
        assert result == Path("data_response.jsonl")
    
    def test_add_suffix_multiple_dots(self):
        """测试文件名包含多个点"""
        result = add_suffix_file("data.backup.jsonl", "response")
        assert result == Path("data.backup_response.jsonl")


class TestReturnToJsonl:
    """测试 return_to_jsonl 装饰器"""
    
    def test_decorator_with_dict(self, temp_dir):
        """测试装饰器处理字典返回值"""
        file_path = temp_dir / "output.jsonl"
        
        @return_to_jsonl(file_path)
        def process_data():
            return {"id": 1, "name": "Alice"}
        
        result = process_data()
        assert result == {"id": 1, "name": "Alice"}
        assert file_path.exists()
        
        # 验证文件内容
        content = read_file(file_path)
        assert len(content) == 1
        assert content[0]["name"] == "Alice"
    
    def test_decorator_with_string(self, temp_dir):
        """测试装饰器处理字符串返回值"""
        file_path = temp_dir / "output.jsonl"
        
        @return_to_jsonl(file_path)
        def process_data():
            return "test string"
        
        result = process_data()
        assert result == "test string"
        assert file_path.exists()
    
    def test_decorator_with_none(self, temp_dir):
        """测试装饰器处理 None 返回值"""
        file_path = temp_dir / "output.jsonl"
        
        @return_to_jsonl(file_path)
        def process_data():
            return None
        
        result = process_data()
        assert result is None
        # None 不应该写入文件
    
    def test_decorator_invalid_return_type(self, temp_dir):
        """测试装饰器处理无效返回类型"""
        file_path = temp_dir / "output.jsonl"
        
        @return_to_jsonl(file_path)
        def process_data():
            return [1, 2, 3]  # list 不支持
        
        with pytest.raises(RuntimeError, match="被装饰器的函数需要有返回"):
            process_data()
    
    def test_decorator_creates_parent_dir(self, temp_dir):
        """测试装饰器自动创建父目录"""
        file_path = temp_dir / "subdir" / "output.jsonl"
        
        @return_to_jsonl(file_path)
        def process_data():
            return {"test": "data"}
        
        process_data()
        assert file_path.exists()
        assert file_path.parent.exists()