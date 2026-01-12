import pytest
from pathlib import Path
from bedrockx.file import read_file, save_file, add_suffix_file, return_to_jsonl, ReadFileExampleCallBack


class TestReadFile:
    """测试 read_file 函数"""

    # ========================== JSONL 测试 ==========================
    def test_read_jsonl_as_list(self, sample_jsonl_file):
        """测试读取 jsonl 为 list，以及 data_length 功能"""
        # 1. 全量读取
        result = read_file(sample_jsonl_file, output_type="list")
        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0]["name"] == "Alice"

        # 2. 限制读取前 2 条
        result_limit = read_file(sample_jsonl_file, output_type="list", data_length=2)
        assert len(result_limit) == 2
        assert result_limit[-1]["name"] == "Bob"  # 第2条是 Bob

    def test_read_jsonl_as_dict(self, sample_jsonl_file):
        """测试读取 jsonl 为 dict"""
        result = read_file(sample_jsonl_file, output_type="dict", main_key_column="id")
        assert isinstance(result, dict)
        assert len(result) == 3
        assert result[1]["name"] == "Alice"
        assert result[2]["name"] == "Bob"

    def test_read_jsonl_as_set(self, sample_jsonl_file):
        """测试读取 jsonl 为 set"""
        result = read_file(sample_jsonl_file, output_type="set", main_key_column="id")
        assert isinstance(result, set)
        assert len(result) == 3
        assert {1, 2, 3} == result

    # ========================== JSON 测试 (ijson/Standard) ==========================
    def test_read_json_as_list(self, sample_json_file):
        """测试读取 json 为 list"""
        result = read_file(sample_json_file, output_type="list")
        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0]["name"] == "Alice"

        # 测试 limit (如果安装了 ijson，这里会走流式读取逻辑)
        result_limit = read_file(sample_json_file, output_type="list", data_length=1)
        assert len(result_limit) == 1
        assert result_limit[0]["name"] == "Alice"

    # ========================== CSV 测试 ==========================
    def test_read_csv_as_list(self, sample_csv_file):
        """测试读取 csv 为 list"""
        result = read_file(sample_csv_file, output_type="list")
        assert len(result) == 3
        # CSV 读取出来通常数字会变成字符串，或者取决于 pandas 的解析
        # 这里为了稳健，转换类型对比，或者直接对比 pandas 默认行为
        assert result[0]["name"] == "Alice"
        
        # 测试 limit (Pandas nrows)
        result_limit = read_file(sample_csv_file, output_type="list", data_length=2)
        assert len(result_limit) == 2

    # ========================== Excel 多 Sheet 测试 ==========================
    def test_read_xlsx_features(self, sample_xlsx_file):
        """测试 Excel 读取：单 Sheet 与 多 Sheet 合并"""
        if sample_xlsx_file is None:
            pytest.skip("未安装 pandas 或 openpyxl，跳过 Excel 测试")

        # 1. 默认读取 (只读第一个 Sheet1) -> 3条数据
        result_default = read_file(sample_xlsx_file, output_type="list")
        assert len(result_default) == 3
        assert result_default[0]["id"] == 1

        # 2. 指定 Sheet 读取 (读取 Sheet2) -> 3条数据 (ID 是 101, 102...)
        result_sheet2 = read_file(sample_xlsx_file, output_type="list", sheet_name="Sheet2")
        assert len(result_sheet2) == 3
        assert result_sheet2[0]["id"] == 101

        # 3. 读取所有 Sheet (Sheet1 + Sheet2) -> 应该有 6 条数据
        result_all = read_file(sample_xlsx_file, output_type="list", sheet_name="all")
        assert len(result_all) == 6
        
        # 验证是否包含了两个 sheet 的数据
        all_ids = {item["id"] for item in result_all}
        assert 1 in all_ids   # 来自 Sheet1
        assert 101 in all_ids # 来自 Sheet2

        # 4. 读取所有 Sheet + Limit (测试合并后的截断)
        # 比如两个 sheet 加起来6条，我只要前4条
        result_all_limit = read_file(sample_xlsx_file, output_type="list", sheet_name="all", data_length=4)
        assert len(result_all_limit) == 4

    # ========================== 异常与边界测试 ==========================
    def test_read_with_path_object(self, sample_jsonl_file):
        """测试使用 Path 对象读取"""
        result = read_file(Path(sample_jsonl_file))
        assert len(result) == 3

    def test_read_with_disable_tqdm(self, sample_jsonl_file):
        """测试禁用进度条参数"""
        result = read_file(sample_jsonl_file, disable_tqdm=True)
        assert len(result) == 3

    def test_error_handling(self, temp_dir):
        """测试各种错误情况 (匹配最新的错误文案)"""
        
        # 1. 缺少 main_key_column
        bad_jsonl = temp_dir / "bad.jsonl"
        bad_jsonl.write_text('{"name": "NoID"}\n', encoding="utf-8")
        
        with pytest.raises(RuntimeError, match="数据缺少 main_key_column"):
            read_file(bad_jsonl, output_type="dict", main_key_column="id")

        # 2. 无效的 output_type
        with pytest.raises(RuntimeError, match="output_type 参数错误"):
            read_file(bad_jsonl, output_type="invalid_type")

        # 3. 不支持的文件后缀
        unknown_file = temp_dir / "test.xyz"
        unknown_file.touch()
        with pytest.raises(RuntimeError, match="不支持的文件格式"):
            read_file(unknown_file)

    def test_read_empty_file(self, temp_dir):
        """测试空文件"""
        empty = temp_dir / "empty.jsonl"
        empty.touch()
        assert read_file(empty) == []


class TestReadFileCallbacks:
    """测试 read_file 的 process_fn 回调功能"""

    def test_callback_extract_str_to_set(self, complex_jsonl_file):
        """
        测试：extract_content_str
        目标：读取 nested content 并转为 Set (自动去重)
        """
        result = read_file(
            complex_jsonl_file, 
            output_type="set",
            process_fn=ReadFileExampleCallBack.extract_content_str
            # 注意：output_type="set" 且 process_fn 返回非 dict 时，不需要 main_key_column
        )
        
        assert isinstance(result, set)
        # 原始数据有3条，但只有2种不一样的 content
        assert len(result) == 2 
        assert "这是第一个好评" in result
        assert "这是差评" in result

    def test_callback_extract_dict_to_list(self, complex_jsonl_file):
        """
        测试：extract_content_dict
        目标：将深层嵌套结构扁平化为 List[Dict]
        """
        result = read_file(
            complex_jsonl_file, 
            output_type="list",
            process_fn=ReadFileExampleCallBack.extract_content_dict
        )
        
        assert isinstance(result, list)
        assert len(result) == 3
        # 验证结构是否已扁平化
        assert result[0] == {"content": "这是第一个好评", "label": "positive"}
        assert "messages" not in result[0] # 原始字段不应该存在

    def test_callback_filter_logic(self, complex_jsonl_file):
        """
        测试：filter_positive_only
        目标：process_fn 返回 None 时，数据应被过滤
        """
        result = read_file(
            complex_jsonl_file,
            output_type="list",
            process_fn=ReadFileExampleCallBack.filter_positive_only
        )
        
        assert len(result) == 2 # 只有两条 positive 数据
        for item in result:
            assert item["label"] == "positive"
            assert item["processed"] is True # 验证回调函数里的修改生效了

    def test_callback_transform_for_dict_output(self, complex_jsonl_file):
        """
        测试：transform_for_id_mapping
        目标：output_type="dict" 时，回调函数必须保留 key_column
        """
        result = read_file(
            complex_jsonl_file,
            output_type="dict",
            main_key_column="id", # 必须指定
            process_fn=ReadFileExampleCallBack.transform_for_id_mapping
        )
        
        assert isinstance(result, dict)
        assert len(result) == 3
        
        # 验证 key 映射
        item_101 = result[101]
        assert item_101["text_length"] > 0
        assert item_101["role"] == "user"
        assert "messages" not in item_101 # 确保是转换后的数据

    def test_callback_lambda(self, complex_jsonl_file):
        """测试使用 lambda 匿名函数"""
        # 简单的提取 ID 列表
        result = read_file(
            complex_jsonl_file,
            output_type="list",
            process_fn=lambda x: x["id"]
        )
        assert result == [101, 102, 103]

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