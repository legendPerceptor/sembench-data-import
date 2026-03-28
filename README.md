# SemBench Movies 数据导入工具

将 SemBench Movies 数据集（烂番茄电影数据）导入 PostgreSQL 数据库。

## 功能特性

- ✅ 自动创建数据库表结构和索引
- ✅ 批量导入电影信息和评论数据
- ✅ 数据去重和冲突处理
- ✅ 进度条显示
- ✅ 外键约束和级联删除
- ✅ 导入统计信息

## 数据集结构

### movies 表
| 字段 | 类型 | 说明 |
|------|------|------|
| id | VARCHAR(255) | 电影ID (主键) |
| title | VARCHAR(1000) | 电影标题 |
| audience_score | INTEGER | 观众评分 (0-100) |
| tomato_meter | INTEGER | 烂番茄评分 (0-100) |
| rating | VARCHAR(50) | 分级 (PG-13, R等) |
| release_date_theaters | DATE | 院线发行日期 |
| runtime_minutes | INTEGER | 时长(分钟) |
| genre | TEXT | 电影类型 |
| director | VARCHAR(500) | 导演 |
| writer | TEXT | 编剧 |

### movie_reviews 表
| 字段 | 类型 | 说明 |
|------|------|------|
| id | SERIAL | 自增主键 |
| movie_id | VARCHAR(255) | 关联电影ID (外键) |
| review_id | VARCHAR(255) | 评论ID (唯一) |
| critic_name | VARCHAR(500) | 评论家姓名 |
| is_top_critic | BOOLEAN | 是否为顶级评论家 |
| review_text | TEXT | 评论内容 |
| score_sentiment | VARCHAR(50) | 评分情感 |

## 安装

### 使用 uv（推荐）

```bash
# 安装 uv（如果尚未安装）
curl -LsSf https://astral.sh/uv/install.sh | sh

# 安装依赖
uv sync

# 运行导入
uv run importer.py --init-db
```

### 使用 pip

```bash
pip install -r requirements.txt
python importer.py --init-db
```

## 配置数据库

1. 复制配置文件示例：
```bash
cp .env.example .env
```

2. 编辑 `.env` 文件，配置数据库连接信息：
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=sembench
DB_USER=postgres
DB_PASSWORD=your_password
```

## 创建数据库

首先在 PostgreSQL 中创建数据库：

```bash
# 使用 psql
createdb sembench

# 或执行 SQL
psql -U postgres -c "CREATE DATABASE sembench;"
```

## 使用方法

### 1. 初始化数据库表结构并导入数据

```bash
uv run importer.py --init-db
```

### 2. 仅导入数据（表已存在）

```bash
uv run importer.py
```

### 3. 指定自定义数据文件路径

```bash
uv run importer.py --init-db \
  --movies /path/to/movies.csv \
  --reviews /path/to/reviews.csv
```

### 4. 命令行指定数据库连接

```bash
uv run importer.py --init-db \
  --host localhost \
  --port 5432 \
  --database sembench \
  --user postgres \
  --password your_password
```

## 命令行参数

| 参数 | 说明 |
|------|------|
| --init-db | 初始化数据库表结构 |
| --movies | 电影数据CSV文件路径 |
| --reviews | 评论数据CSV文件路径 |
| --host | 数据库主机 |
| --port | 数据库端口 |
| --database | 数据库名称 |
| --user | 数据库用户 |
| --password | 数据库密码 |

## 项目结构

```
.
├── pyproject.toml        # 项目配置和依赖
├── uv.lock              # UV 锁文件
├── importer.py          # 导入主程序
├── config.py            # 配置文件
├── schema.sql           # 数据库表结构
├── .env.example         # 配置文件示例
└── README.md            # 使用说明
```

## 查询示例

```sql
-- 查询评分最高的电影
SELECT title, tomato_meter, audience_score
FROM movies
WHERE tomato_meter IS NOT NULL
ORDER BY tomato_meter DESC
LIMIT 10;

-- 查询某部电影的评论
SELECT m.title, r.critic_name, r.review_text, r.score_sentiment
FROM movies m
JOIN movie_reviews r ON m.id = r.movie_id
WHERE m.title = 'The Godfather'
ORDER BY r.creation_date DESC;

-- 统计各类型电影数量
SELECT
    UNNEST(REGEXP_SPLIT_TO_ARRAY(genre, ',')) as genre,
    COUNT(*) as count
FROM movies
WHERE genre IS NOT NULL
GROUP BY genre
ORDER BY count DESC;

-- 查询顶级评论家的评论
SELECT m.title, r.critic_name, r.review_text
FROM movies m
JOIN movie_reviews r ON m.id = r.movie_id
WHERE r.is_top_critic = true
ORDER BY r.creation_date DESC
LIMIT 20;
```

## 注意事项

1. **数据量大**: 评论数据文件约400MB，导入需要一定时间
2. **内存占用**: 建议至少4GB可用内存
3. **批量大小**: 默认批量插入1000条，可在 `config.py` 中调整 `BATCH_SIZE`
4. **数据清理**: 程序会自动清理空值和无效数据
5. **外键约束**: 导入评论前必须先导入电影数据

## 故障排除

### 连接失败
- 检查数据库是否运行: `pg_isready`
- 检查防火墙设置
- 验证 `.env` 配置是否正确

### 内存不足
- 减小 `config.py` 中的 `BATCH_SIZE` 值
- 关闭其他程序释放内存

### 权限错误
- 确保数据库用户有 CREATE TABLE 权限
- 检查数据库和表的权限设置

## 许可证

MIT License
