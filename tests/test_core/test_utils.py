from flowsaber.core.utils import check_cycle


def test_check_cycle():
    cycle_samples = [

    ]

    non_cycle_samples = [

    ]

    assert all(check_cycle(sample) for sample in cycle_samples)
    assert all(not check_cycle(sample) for sample in non_cycle_samples)
