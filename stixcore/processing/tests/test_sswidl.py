from stixcore.io.fits.processors import FitsL2Processor
from stixcore.processing.sswidl import SSWIDLProcessor, SSWIDLTask


class T1(SSWIDLTask):

    def __init__(self):
        super().__init__(script="T1", work_dir=".", params={})


class T2(SSWIDLTask):

    def __init__(self):
        super().__init__(script="T2", work_dir=".", params={})


def test_processor():
    pros = SSWIDLProcessor(FitsL2Processor("."))

    t2 = T2()
    pros[T2] = t2

    pros[T1].params["test"] = 1
    t1 = pros[T1]
    assert t1.params["test"] == 1

    assert len(pros) == 2

    pros.process()

    for t in pros.values():
        print(t.results)

    assert len(pros) == 2
