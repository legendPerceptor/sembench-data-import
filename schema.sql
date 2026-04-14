-- SemBench Movies 数据库表结构

-- 删除已存在的表（谨慎使用）
-- DROP TABLE IF EXISTS movie_reviews CASCADE;
-- DROP TABLE IF EXISTS movies CASCADE;

-- 电影信息表
CREATE TABLE IF NOT EXISTS movies (
    id VARCHAR(255) PRIMARY KEY,
    title VARCHAR(1000),
    audience_score INTEGER,
    tomato_meter INTEGER,
    rating VARCHAR(50),
    rating_contents TEXT,
    release_date_theaters DATE,
    release_date_streaming DATE,
    runtime_minutes INTEGER,
    genre TEXT,
    original_language VARCHAR(100),
    director VARCHAR(500),
    writer TEXT,
    box_office VARCHAR(100),
    distributor VARCHAR(500),
    sound_mix VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_movies_title ON movies(title);
CREATE INDEX IF NOT EXISTS idx_movies_genre ON movies(genre);
CREATE INDEX IF NOT EXISTS idx_movies_director ON movies(director);
CREATE INDEX IF NOT EXISTS idx_movies_release_date_theaters ON movies(release_date_theaters);

-- 电影评论表
CREATE TABLE IF NOT EXISTS movie_reviews (
    id SERIAL PRIMARY KEY,
    movie_id VARCHAR(255) NOT NULL,
    review_id VARCHAR(255) UNIQUE NOT NULL,
    creation_date DATE,
    critic_name VARCHAR(500),
    is_top_critic BOOLEAN,
    original_score VARCHAR(50),
    review_state VARCHAR(50),
    publication_name VARCHAR(500),
    review_text TEXT,
    score_sentiment VARCHAR(50),
    review_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_reviews_movie_id ON movie_reviews(movie_id);
CREATE INDEX IF NOT EXISTS idx_reviews_critic_name ON movie_reviews(critic_name);
CREATE INDEX IF NOT EXISTS idx_reviews_score_sentiment ON movie_reviews(score_sentiment);
CREATE INDEX IF NOT EXISTS idx_reviews_creation_date ON movie_reviews(creation_date);

-- 创建更新时间触发器
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_movies_updated_at BEFORE UPDATE ON movies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 添加注释
COMMENT ON TABLE movies IS '烂番茄电影信息表';
COMMENT ON TABLE movie_reviews IS '烂番茄电影评论表';
COMMENT ON COLUMN movies.audience_score IS '观众评分 (0-100)';
COMMENT ON COLUMN movies.tomato_meter IS '烂番茄评分 (0-100)';
COMMENT ON COLUMN movie_reviews.is_top_critic IS '是否为顶级评论家';
