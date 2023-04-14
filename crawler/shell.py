# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from scrapy import cmdline
import os


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    os.chdir(f"{os.path.dirname(os.path.abspath(__file__))}/fetch_catalog")
    cmdline.execute("scrapy crawl OSGBenchmarks -s DATA_ROOT_PATH=/home/uw2/data/SPEC".split())
    print(os.path.dirname(os.path.abspath(__file__)))

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
