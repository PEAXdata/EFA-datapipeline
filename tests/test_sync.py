from efa_30mhz.sync import Source, Target, Sync


def test_generic_source_target():
    class GenericSource(Source):
        rows = ["abc", "def", "ghi"]

        def __init__(self, **kwargs):
            super().__init__(**kwargs)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

        def read_all(self):
            return GenericSource.rows

    class GenericTarget(Target):
        rows = []

        def __init__(self, **kwargs):
            super().__init__(**kwargs)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

        def write(self, row):
            GenericTarget.rows.append(row)

    with Sync(GenericSource, {}, GenericTarget, {}) as sync:
        sync.start()
    assert GenericTarget.rows == GenericSource.rows
