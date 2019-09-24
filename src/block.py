import itertools
import typing


class Block:
    """
    Block class

    Specifies essentially a range of integers
    """
    def __init__(self, start: int, end: int):
        super(Block, self).__init__()
        try:
            start = int(start)
            end = int(end)
        except Exception as e:
            raise e
        else:
            if start < end:
                self.start = start
                self.end = end
            else:
                raise ValueError("Start value must be smaller than end value")

    def __iter__(self):
        self.i = self.start
        return self

    def __next__(self):
        if self.i <= self.end:
            res, self.i = self.i, self.i + 1
            return res
        else:
            raise StopIteration

    def __repr__(self):
        return 'Block({}, {})'.format(self.start, self.end)

    def __str__(self):
        return '[{}, {}]'.format(self.start, self.end)


class BlockList:
    """
    BlockList class

    Specifies essentially a union of integer ranges
    Implements the 'in' operator to check if an integer 
    is in either of the contained blocks
    """
    def __init__(self, *blocks):
        super(BlockList, self).__init__()
        self.set = set()
        self.add_blocks(blocks)

    # Implementation of the 'in' operator
    # If the list contains no blocks,
    # return true by default
    # (this feature can be changed depending on the need)
    def __contains__(self, item):
        if len(self.set) == 0:
            return True
        else:
            return item in self.set

    def add_block(self, block):
        self.set.update(block)

    def add_blocks(self, blocks):
        for block in blocks:
            self.set.update(block)

    # Return a list of disjoint blocks,
    # so the returned blocks are not always
    # exactly the same as the ones that were added
    def get_blocks(self):
        blocks = []
        for a, b in itertools.groupby(enumerate(self.set),
                                      lambda x: x[1] - x[0]):
            b = list(b)
            blocks.append(Block(b[0][1], b[-1][1]))
        return tuple(blocks)

    def __repr__(self):
        return 'BlockList{}'.format(self.get_blocks())

    # Change this if you change the behaviour of the 'in' operator
    def __str__(self):
        if len(self.set) == 0:
            return '[-inf, +inf]'
        return str(self.get_blocks())