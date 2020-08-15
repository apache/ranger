import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="apache-ranger",
    version="0.0.1",
    author="Apache Ranger",
    author_email="dev@ranger.apache.org",
    description="Apache Ranger Python client",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/apache/ranger/tree/master/intg/src/main/python",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
