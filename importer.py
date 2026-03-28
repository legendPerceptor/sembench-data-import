#!/usr/bin/env python3
"""
SemBench Movies 数据集导入工具
支持将烂番茄电影数据导入PostgreSQL数据库
"""
import os
import csv
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import sql
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from config import DB_CONFIG, DATA_PATH, BATCH_SIZE


class MovieDataImporter:
    """电影数据导入器"""

    def __init__(self, db_config=None):
        """初始化数据库连接"""
        self.db_config = db_config or DB_CONFIG
        self.conn = None
        self.cursor = None

    def connect(self):
        """连接数据库"""
        try:
            print(f"正在连接数据库: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("✓ 数据库连接成功")
            return True
        except Exception as e:
            print(f"✗ 数据库连接失败: {e}")
            return False

    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("✓ 数据库连接已关闭")

    def execute_sql_file(self, sql_file):
        """执行SQL文件"""
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
                self.cursor.execute(sql_content)
                self.conn.commit()
                print(f"✓ 成功执行SQL文件: {sql_file}")
                return True
        except Exception as e:
            print(f"✗ 执行SQL文件失败: {e}")
            self.conn.rollback()
            return False

    def clean_value(self, value):
        """清理数据值"""
        if pd.isna(value) or value == '' or value is None:
            return None
        return value

    def import_movies(self, movies_file):
        """导入电影数据"""
        try:
            print(f"\n开始导入电影数据: {movies_file}")

            # 读取CSV文件
            df = pd.read_csv(movies_file)
            total_rows = len(df)
            print(f"共 {total_rows} 条电影记录")

            # 准备插入数据
            insert_query = """
                INSERT INTO movies (
                    id, title, audience_score, tomato_meter, rating, rating_contents,
                    release_date_theaters, release_date_streaming, runtime_minutes,
                    genre, original_language, director, writer, box_office,
                    distributor, sound_mix
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    audience_score = EXCLUDED.audience_score,
                    tomato_meter = EXCLUDED.tomato_meter,
                    rating = EXCLUDED.rating,
                    rating_contents = EXCLUDED.rating_contents,
                    release_date_theaters = EXCLUDED.release_date_theaters,
                    release_date_streaming = EXCLUDED.release_date_streaming,
                    runtime_minutes = EXCLUDED.runtime_minutes,
                    genre = EXCLUDED.genre,
                    original_language = EXCLUDED.original_language,
                    director = EXCLUDED.director,
                    writer = EXCLUDED.writer,
                    box_office = EXCLUDED.box_office,
                    distributor = EXCLUDED.distributor,
                    sound_mix = EXCLUDED.sound_mix,
                    updated_at = CURRENT_TIMESTAMP
            """

            # 批量插入
            batch_data = []
            inserted_count = 0
            skipped_count = 0

            with tqdm(total=total_rows, desc="导入电影", unit="条") as pbar:
                for _, row in df.iterrows():
                    # 清理数据
                    data = (
                        self.clean_value(row.get('id')),
                        self.clean_value(row.get('title')),
                        self.clean_value(row.get('audienceScore')),
                        self.clean_value(row.get('tomatoMeter')),
                        self.clean_value(row.get('rating')),
                        self.clean_value(row.get('ratingContents')),
                        self.clean_value(row.get('releaseDateTheaters')),
                        self.clean_value(row.get('releaseDateStreaming')),
                        self.clean_value(row.get('runtimeMinutes')),
                        self.clean_value(row.get('genre')),
                        self.clean_value(row.get('originalLanguage')),
                        self.clean_value(row.get('director')),
                        self.clean_value(row.get('writer')),
                        self.clean_value(row.get('boxOffice')),
                        self.clean_value(row.get('distributor')),
                        self.clean_value(row.get('soundMix'))
                    )

                    if data[0] is None:  # id不能为空
                        skipped_count += 1
                        pbar.update(1)
                        continue

                    batch_data.append(data)

                    # 批量插入
                    if len(batch_data) >= BATCH_SIZE:
                        execute_batch(self.cursor, insert_query, batch_data)
                        self.conn.commit()
                        inserted_count += len(batch_data)
                        batch_data = []
                        pbar.update(BATCH_SIZE)

                # 插入剩余数据
                if batch_data:
                    execute_batch(self.cursor, insert_query, batch_data)
                    self.conn.commit()
                    inserted_count += len(batch_data)
                    pbar.update(len(batch_data))

            print(f"✓ 电影数据导入完成: 插入 {inserted_count} 条, 跳过 {skipped_count} 条")
            return True

        except Exception as e:
            print(f"✗ 电影数据导入失败: {e}")
            self.conn.rollback()
            return False

    def import_reviews(self, reviews_file):
        """导入评论数据"""
        try:
            print(f"\n开始导入评论数据: {reviews_file}")

            # 读取CSV文件
            df = pd.read_csv(reviews_file)
            total_rows = len(df)
            print(f"共 {total_rows} 条评论记录")

            # 准备插入数据
            insert_query = """
                INSERT INTO movie_reviews (
                    movie_id, review_id, creation_date, critic_name, is_top_critic,
                    original_score, review_state, publication_name, review_text,
                    score_sentiment, review_url
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (review_id) DO UPDATE SET
                    movie_id = EXCLUDED.movie_id,
                    creation_date = EXCLUDED.creation_date,
                    critic_name = EXCLUDED.critic_name,
                    is_top_critic = EXCLUDED.is_top_critic,
                    original_score = EXCLUDED.original_score,
                    review_state = EXCLUDED.review_state,
                    publication_name = EXCLUDED.publication_name,
                    review_text = EXCLUDED.review_text,
                    score_sentiment = EXCLUDED.score_sentiment,
                    review_url = EXCLUDED.review_url
            """

            # 批量插入
            batch_data = []
            inserted_count = 0
            skipped_count = 0
            invalid_movie_count = 0

            with tqdm(total=total_rows, desc="导入评论", unit="条") as pbar:
                for _, row in df.iterrows():
                    movie_id = self.clean_value(row.get('id'))

                    # 检查电影是否存在
                    if movie_id:
                        self.cursor.execute("SELECT 1 FROM movies WHERE id = %s", (movie_id,))
                        if not self.cursor.fetchone():
                            invalid_movie_count += 1
                            pbar.update(1)
                            continue

                    # 清理布尔值
                    is_top_critic = self.clean_value(row.get('isTopCritic'))
                    if isinstance(is_top_critic, str):
                        is_top_critic = is_top_critic.lower() in ('true', '1', 'yes')

                    # 清理数据
                    data = (
                        movie_id,
                        self.clean_value(row.get('reviewId')),
                        self.clean_value(row.get('creationDate')),
                        self.clean_value(row.get('criticName')),
                        is_top_critic,
                        self.clean_value(row.get('originalScore')),
                        self.clean_value(row.get('reviewState')),
                        self.clean_value(row.get('publicatioName')),
                        self.clean_value(row.get('reviewText')),
                        self.clean_value(row.get('scoreSentiment')),
                        self.clean_value(row.get('reviewUrl'))
                    )

                    if data[1] is None:  # review_id不能为空
                        skipped_count += 1
                        pbar.update(1)
                        continue

                    batch_data.append(data)

                    # 批量插入
                    if len(batch_data) >= BATCH_SIZE:
                        execute_batch(self.cursor, insert_query, batch_data)
                        self.conn.commit()
                        inserted_count += len(batch_data)
                        batch_data = []
                        pbar.update(BATCH_SIZE)

                # 插入剩余数据
                if batch_data:
                    execute_batch(self.cursor, insert_query, batch_data)
                    self.conn.commit()
                    inserted_count += len(batch_data)
                    pbar.update(len(batch_data))

            print(f"✓ 评论数据导入完成: 插入 {inserted_count} 条, 跳过 {skipped_count} 条, 无效电影ID: {invalid_movie_count} 条")
            return True

        except Exception as e:
            print(f"✗ 评论数据导入失败: {e}")
            self.conn.rollback()
            return False

    def get_statistics(self):
        """获取导入统计信息"""
        try:
            stats = {}

            # 电影统计
            self.cursor.execute("SELECT COUNT(*) FROM movies")
            stats['total_movies'] = self.cursor.fetchone()[0]

            self.cursor.execute("SELECT COUNT(*) FROM movies WHERE tomato_meter IS NOT NULL")
            stats['movies_with_rating'] = self.cursor.fetchone()[0]

            # 评论统计
            self.cursor.execute("SELECT COUNT(*) FROM movie_reviews")
            stats['total_reviews'] = self.cursor.fetchone()[0]

            self.cursor.execute("SELECT COUNT(*) FROM movie_reviews WHERE is_top_critic = true")
            stats['top_critic_reviews'] = self.cursor.fetchone()[0]

            # 按类型统计电影数量
            self.cursor.execute("""
                SELECT UNNEST(REGEXP_SPLIT_TO_ARRAY(genre, ',')) as genre, COUNT(*) as count
                FROM movies
                WHERE genre IS NOT NULL
                GROUP BY genre
                ORDER BY count DESC
                LIMIT 10
            """)
            stats['top_genres'] = self.cursor.fetchall()

            return stats

        except Exception as e:
            print(f"✗ 获取统计信息失败: {e}")
            return None

    def print_statistics(self, stats):
        """打印统计信息"""
        if not stats:
            return

        print("\n" + "="*60)
        print("数据导入统计")
        print("="*60)
        print(f"电影总数: {stats['total_movies']:,}")
        print(f"有评分的电影: {stats['movies_with_rating']:,}")
        print(f"评论总数: {stats['total_reviews']:,}")
        print(f"顶级评论家评论: {stats['top_critic_reviews']:,}")

        if stats.get('top_genres'):
            print("\n前10大电影类型:")
            for genre, count in stats['top_genres']:
                print(f"  {genre.strip()}: {count:,}")
        print("="*60)

    def run(self, movies_file=None, reviews_file=None, init_db=False):
        """运行导入流程"""
        if not self.connect():
            return False

        try:
            # 初始化数据库表
            if init_db:
                schema_file = os.path.join(os.path.dirname(__file__), 'schema.sql')
                if not self.execute_sql_file(schema_file):
                    return False

            # 导入电影数据
            movies_file = movies_file or os.path.join(DATA_PATH, 'rotten_tomatoes_movies.csv')
            if not os.path.exists(movies_file):
                print(f"✗ 电影数据文件不存在: {movies_file}")
                return False

            if not self.import_movies(movies_file):
                return False

            # 导入评论数据
            reviews_file = reviews_file or os.path.join(DATA_PATH, 'rotten_tomatoes_movie_reviews.csv')
            if not os.path.exists(reviews_file):
                print(f"✗ 评论数据文件不存在: {reviews_file}")
                return False

            if not self.import_reviews(reviews_file):
                return False

            # 显示统计信息
            stats = self.get_statistics()
            self.print_statistics(stats)

            print("\n✓ 所有数据导入完成!")
            return True

        finally:
            self.close()


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='SemBench Movies 数据集导入工具')
    parser.add_argument('--init-db', action='store_true', help='初始化数据库表结构')
    parser.add_argument('--movies', type=str, help='电影数据CSV文件路径')
    parser.add_argument('--reviews', type=str, help='评论数据CSV文件路径')
    parser.add_argument('--host', type=str, help='数据库主机')
    parser.add_argument('--port', type=str, help='数据库端口')
    parser.add_argument('--database', type=str, help='数据库名称')
    parser.add_argument('--user', type=str, help='数据库用户')
    parser.add_argument('--password', type=str, help='数据库密码')

    args = parser.parse_args()

    # 更新数据库配置
    db_config = DB_CONFIG.copy()
    if args.host:
        db_config['host'] = args.host
    if args.port:
        db_config['port'] = args.port
    if args.database:
        db_config['database'] = args.database
    if args.user:
        db_config['user'] = args.user
    if args.password:
        db_config['password'] = args.password

    # 运行导入
    importer = MovieDataImporter(db_config)
    importer.run(
        movies_file=args.movies,
        reviews_file=args.reviews,
        init_db=args.init_db
    )


if __name__ == '__main__':
    main()
