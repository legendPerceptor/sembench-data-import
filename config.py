"""
数据库配置文件
"""
import os
from dotenv import load_dotenv

load_dotenv()

# 数据库配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'sembench'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'password'),
}

# 数据集路径
DATA_PATH = './data/movie'

# 批量插入大小
BATCH_SIZE = 1000
