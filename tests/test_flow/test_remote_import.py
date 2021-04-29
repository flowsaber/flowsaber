import httpimport

from flowsaber.api import *


def test_remote_import():
    url = "https://gist.githubusercontent.com/zhqu1148980644/" \
          "2eafbe8d25883919ecf11a729f1fdb9a/raw/23f8e087ceba040492c78565f81c764bce9b65c5"
    with httpimport.remote_repo(['testtask'], url):
        from testtask import task1, task2

    @flow
    def myflow(name):
        return [name | task1, name] | task2 | view

    names = Channel.values("qwe", 'asd', 'zxcx', 'hhh')

    workflow = myflow(names)
    run(workflow)


if __name__ == "__main__":
    test_remote_import()
