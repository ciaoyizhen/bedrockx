import pytest
import time
from pathlib import Path
from bedrockx.process import BaseMultiThreading
from bedrockx.file import read_file


class TestMultiThreading:
    """测试 BaseMultiThreading 类"""
    
    def test_basic_processing(self, temp_dir):
        """测试基本多线程处理"""
        
        class TestProcessor(BaseMultiThreading):
            def single_data_process(self, item):
                # 简单的数据处理
                return {**item, "processed": True}
        
        save_path = temp_dir / "output.jsonl"
        processor = TestProcessor(max_workers=2, save_path=save_path)
        
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
        
        processor(data)
        
        assert save_path.exists()
        result = read_file(save_path)
        assert len(result) == 2
        assert all(item["processed"] for item in result)
    
    def test_with_custom_file_type(self, temp_dir):
        """测试指定文件类型"""
        
        class TestProcessor(BaseMultiThreading):
            def single_data_process(self, item):
                return item
        
        save_path = temp_dir / "output.jsonl"
        processor = TestProcessor(max_workers=2, save_path=save_path, file_type="jsonl")
        
        data = [{"id": 1}]
        processor(data)
        
        assert save_path.exists()
    
    def test_invalid_file_type(self, temp_dir):
        """测试无效的文件类型"""
        save_path = temp_dir / "output.txt"
        
        with pytest.raises(RuntimeError, match="传入的file_type不符合要求"):
            TestProcessor = type("TestProcessor", (BaseMultiThreading,), {
                "single_data_process": lambda self, item: item
            })
            TestProcessor(max_workers=2, save_path=save_path)
    
    def test_not_implemented_error(self, temp_dir):
        """测试未实现 single_data_process"""
        save_path = temp_dir / "output.jsonl"
        processor = BaseMultiThreading(max_workers=2, save_path=save_path)
        
        with pytest.raises(NotImplementedError):
            processor([{"id": 1}])
    
    def test_processing_with_delay(self, temp_dir):
        """测试带延迟的处理"""
        
        class SlowProcessor(BaseMultiThreading):
            def single_data_process(self, item):
                time.sleep(0.01)  # 模拟耗时操作
                return {**item, "processed": True}
        
        save_path = temp_dir / "output.jsonl"
        processor = SlowProcessor(max_workers=4, save_path=save_path)
        
        data = [{"id": i} for i in range(10)]
        
        start = time.time()
        processor(data)
        duration = time.time() - start
        
        # 多线程应该比单线程快
        assert duration < 0.1  # 10个任务，单线程需要0.1秒，多线程应该快得多