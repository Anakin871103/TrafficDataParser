import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='TrafficDataParser',
    version='0.0.9',
    author='Hsuan-Chih Wang',
    author_email='transport.hcwang@gmail.com',
    description='Traffic Data Parser Package',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/HsuanChih-Wang/TrafficDataParser',
    project_urls={
        "Bug Tracker": "https://github.com/HsuanChih-Wang/TrafficDataParser/issues"
    },
    license='MIT',
    packages=['trafficdataparser'],
    install_requires=['pandas'],
)