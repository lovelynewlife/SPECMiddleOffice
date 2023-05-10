from setuptools import setup, find_packages

setup(
    name='SPECrawling',
    version='0.1',
    packages=find_packages(),
    url='https://github.com/lovelynewlife/SPECMiddleOffice',
    license='',
    author='AhoyZhang',
    python_requires=">=3.7",
    author_email='',
    description='It\'s a crawler tool for spec.org.',
    data_files=[],
    package_data={
        '': ['*.cfg', 'static/*', "static/helper/*"]
    },
    install_requires=['scrapy', "tqdm", "lxml", "ipython"],
    entry_points={"console_scripts": ["specrawling = SPECrawling.cmdline:main"]}
)
