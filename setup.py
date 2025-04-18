"""
Setup configuration for the E-commerce Analytics System
"""
from setuptools import setup, find_packages

# Read requirements
with open('requirements.txt') as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

# Read long description
with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='ecommerce-analytics',
    version='1.0.0',
    description='Real-time e-commerce analytics system using Kafka, Spark, and PostgreSQL',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Your Name',
    author_email='your.email@example.com',
    url='https://github.com/yourusername/ecommerce-analytics',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    python_requires='>=3.8.10',
    install_requires=requirements,
    extras_require={
        'dev': [
            'black==22.3.0',
            'flake8==6.0.0',
            'mypy==1.3.0',
            'isort==5.12.0',
        ],
        'test': [
            'pytest==7.1.3',
            'pytest-cov==4.0.0',
            'pytest-mock==3.10.0',
            'pytest-env==0.8.1',
            'coverage==7.2.3',
        ],
    },
    entry_points={
        'console_scripts': [
            'ecommerce-analytics=main:main',
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Distributed Computing',
        'Topic :: Scientific/Engineering :: Information Analysis',
    ],
    keywords='kafka spark postgresql analytics streaming realtime',
    project_urls={
        'Bug Reports': 'https://github.com/yourusername/ecommerce-analytics/issues',
        'Source': 'https://github.com/yourusername/ecommerce-analytics',
    },
    include_package_data=True,
    zip_safe=False,
)
