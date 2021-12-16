from setuptools import setup, find_packages

setup(
    name="podcast-demo",
    version="0.0.1",
    description="Chalice application that parses podcasts from YouTube channels",
    author="Florin",
    author_email="haja.fgabriel@gmail.com",
    url="https://github.com/haja-fgabriel/chalice-podcast-demo",
    package_dir={"": "src/podcast-demo/"},
    packages=find_packages("src/podcast-demo/"),
)
