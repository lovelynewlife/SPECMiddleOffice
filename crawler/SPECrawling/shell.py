import code
import signal

import os


def start_ipython_console(banner):
    try:
        from IPython.terminal.embed import InteractiveShellEmbed
        from IPython.terminal.ipapp import load_default_config
    except ImportError:
        from IPython.frontend.terminal.embed import InteractiveShellEmbed
        from IPython.frontend.terminal.ipapp import load_default_config

    from SPECrawling import SPEC, FileType, GroupType
    InteractiveShellEmbed.clear_instance()
    shell = InteractiveShellEmbed.instance(
        banner1=banner, user_ns=locals(),
    )
    shell()


def run_shell():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
    banner_file_path = os.path.join(static_dir, "banner.txt")

    with open(banner_file_path) as file:
        banner = file.read()

    start_ipython_console(banner)
