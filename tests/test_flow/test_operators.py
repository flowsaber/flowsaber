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
        not_exist1_ch = dict_ch | Get("not_exist_1", "not_exist_1")
        not_exist2_ch = dict_ch | Select("not_exist_2", "not_exist_2")

        [name_ch, age_ch, height_ch, dict_ch.getitem(key="money"), not_exist1_ch, not_exist2_ch] | view

    run(f())


def test_Merge_task():
    @task
    def assert_tuple(merged: tuple, *args):
        assert args == merged and isinstance(merged, tuple)

    @flow
    def f():
        num_ch = Channel.values(1, 2, 3)
        chs = [num_ch] * 3
        m1 = Merge()(*chs)
        m2 = merge(*chs)
        m3 = num_ch.merge(*chs[1:])

        assert_tuple(m1, *chs)
        assert_tuple(m2, *chs)
        assert_tuple(m3, *chs)

    run(f())


if __name__ == "__main__":
    test_GetItem_task()
    test_Merge_task()
