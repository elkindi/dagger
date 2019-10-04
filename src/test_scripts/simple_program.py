class Rectangle(object):
    """docstring for Rectangle"""
    def __init__(self, x, y, dx, dy):
        super(Rectangle, self).__init__()
        self.x = x
        self.y = y
        self.dx = dx
        self.dy = dy

    def setX(self, newX):
        self.x = x

    def __str__(self):
        return '[{},{},{},{}]'.format(self.x, self.y, self.dx, self.dy)


rect = Rectangle(0, 0, 12, 12)
i, x = 3, 1
m = "ok"
if i > 2:
    x = 4
    rect.setX(x)
else:
    m = "not ok"
print(x)
print(m)