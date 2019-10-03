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
            if start < 0:
                raise ValueError("Start value must be positive")
            if start > end:
                raise ValueError("Start value must be smaller than or equal to end value")
            self.start = start
            self.end = end

    def __iter__(self):
        self.i = self.start
        return self

    def __next__(self):
        if self.i <= self.end:
            res, self.i = self.i, self.i + 1
            return res
        else:
            raise StopIteration

    # Implementation of the 'in' operator
    def __contains__(self, item):
        return item >= self.start and item <= self.end

    # checks if two blocks overlap
    def overlap(self, other):
        return self.start <= other.end and self.end >= other.start

    def __repr__(self):
        return 'Block({}, {})'.format(self.start, self.end)

    def __str__(self):
        return '[{}, {}]'.format(self.start, self.end)


class BlockList:
    """
    BlockList class

    Specifies essentially a union of integer ranges
    Implements the 'in' operator to check if an integer 
    is in any of the contained blocks
    """
    def __init__(self, *blocks):
        super(BlockList, self).__init__()
        self.blocks = []
        self.add_blocks(blocks)

    # Implementation of the 'in' operator
    def __contains__(self, item):
        for block in self.blocks:
            if item in block:
                return True
        return False

    def add_block(self, new_block):
        if not isinstance(new_block, Block):
            raise TypeError("Argument must be of type Block (found: ",
                            type(new_block), ")")
        for block in self.blocks:
            if block.overlap(new_block):
                raise ValueError("New block must not overlap with previously defined blocks")
        self.blocks.append(new_block)

    def add_blocks(self, blocks):
        for block in blocks:
            self.add_block(block)

    # Return the list of blocks as a tuple
    def get_blocks(self):
        return tuple(self.blocks)

    def __repr__(self):
        return 'BlockList{}'.format(self.get_blocks())

    def __str__(self):
        return str(self.get_blocks())