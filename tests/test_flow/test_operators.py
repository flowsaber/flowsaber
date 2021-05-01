from flowsaber.api import *


def test_GetItem_task():
    @task
    def gen_dict(num: int):
        return {
            'name': str(num),
            'age': num * num,
            'height': num + num,
            'money': num / 1000
        }

    @flow
    def f():
        num_ch = Channel.values(1, 2, 3)
        dict_ch = gen_dict(num_ch)
        name_ch, age_ch = dict_ch >> [GetItem("name"), getitem('age')]
        height_ch = dict_ch['height']

        [name_ch, age_ch, height_ch, dict_ch.getitem(key="money")] | view

    run(f())


if __name__ == "__main__":
    test_GetItem_task()
