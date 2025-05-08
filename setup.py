from setuptools import setup, find_packages

setup(
    name="tuningfork",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "psutil",
        "psycopg2-binary",  # PostgreSQL connector
        "mysql-connector-python",  # MySQL connector
        "pyodbc",  # MSSQL connector
    ],
    extras_require={
        "dev": [
            "pytest",
            "pytest-cov",
        ],
    },
    python_requires=">=3.6",
    description="Database Performance Optimization Tool",
    author="TuningFork Team",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)