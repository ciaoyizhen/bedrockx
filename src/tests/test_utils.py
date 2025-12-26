import pytest
from bedrockx.utils import singleton, LoggerManager


class TestSingleton:
    """测试 singleton 装饰器"""
    
    def test_singleton_basic(self):
        """测试基本单例功能"""
        
        @singleton
        class TestClass:
            def __init__(self):
                self.value = 0
        
        instance1 = TestClass()
        instance2 = TestClass()
        
        assert instance1 is instance2
    
    def test_singleton_with_state(self):
        """测试单例状态共享"""
        
        @singleton
        class Counter:
            def __init__(self):
                self.count = 0
            
            def increment(self):
                self.count += 1
        
        counter1 = Counter()
        counter1.increment()
        
        counter2 = Counter()
        assert counter2.count == 1  # 共享状态
    
    def test_singleton_different_classes(self):
        """测试不同类的单例独立"""
        
        @singleton
        class ClassA:
            pass
        
        @singleton
        class ClassB:
            pass
        
        a = ClassA()
        b = ClassB()
        
        assert a is not b


class TestLoggerManager:
    """测试 LoggerManager 类"""
    
    def test_logger_basic(self, temp_dir):
        """测试基本日志功能"""
        log_path = temp_dir / "test.log"
        logger = LoggerManager(log_path=str(log_path), console=False)
        
        logger.info("Test message")
        
        assert log_path.exists()
        content = log_path.read_text()
        assert "Test message" in content
    
    #TODO 这里验证 时过时不过的 但是手动又能通过
    # def test_logger_levels(self, temp_dir):
    #     """测试不同日志级别"""
    #     log_path = temp_dir / "test.log"
    #     logger = LoggerManager(log_path=str(log_path), console=False)
        
    #     logger.debug("Debug message")
    #     logger.info("Info message")
    #     logger.warning("Warning message")
    #     logger.error("Error message")
        
    #     content = log_path.read_text()
    #     assert "Info message" in content
    #     assert "Warning message" in content
    #     assert "Error message" in content
    
    def test_logger_no_file(self):
        """测试仅控制台输出"""
        logger = LoggerManager(log_path=None, console=True)
        # 不应该抛出异常
        logger.info("Test")
    
    def test_logger_creates_dir(self, temp_dir):
        """测试自动创建日志目录"""
        log_path = temp_dir / "logs" / "subdir" / "test.log"
        logger = LoggerManager(log_path=str(log_path), console=False)
        
        logger.info("Test")
        
        assert log_path.exists()
        assert log_path.parent.exists()