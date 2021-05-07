from flowsaber.api import *


def test_event_channel():
    def check_if_passed(interval):
        import time
        last = cur = time.time()
        check = 4
        while check > 0:
            if cur - last >= interval:
                yield True
                check -= 1
                last = cur
            else:
                yield False
            cur = time.time()

    @flow
    def myflow():
        event = Channel.event(check_if_passed(4), check_if_passed(8), interval=2)
        event | subscribe(lambda x: print(time.time()))

    run(myflow())


if __name__ == "__main__":
    test_event_channel()
