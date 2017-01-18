class _BaseException(Exception):
    def __init__(self, *a, **kw):
        handle = kw.pop('handle', None)
        super(Exception, self).__init__(*a, **kw)
        self.handle = handle


class WorkFailed(_BaseException):
    pass


class WorkException(_BaseException):
    pass
