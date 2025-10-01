
from setuptools import setup, find_packages

setup(
    name="snowflake_utils",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "snowflake-connector-python",
        "pandas",
        "toml"
    ],
    author="denso",
    description="Snowflake接続とデータ取得を簡単にするユーティリティ",
)
