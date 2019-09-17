from datetime import datetime


class DbObject(object):
    """docstring for DbObject"""
    def __init__(self, _type: type, _time: datetime, lineno: int, name: str,
                 value):
        super(DbObject, self).__init__()
        self.type = _type
        self.time = _time
        self.lineno = lineno
        self.name = name
        self.value = value

    def __repr__(self):
        return "DbObject(name: {}, type: {}, lineno: {})".format(
            self.name, self.type.__name__, self.lineno)

    def __str__(self):
        return repr(self)

    @property
    def __class__(self):
        return self.type