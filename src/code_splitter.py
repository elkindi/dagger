import ast
import copy
from block import Block, BlockList


class CodeSplitter():
    """
    CodeSplitter class

    Splits input code given as an ast.Module into different code blocks

    Only top-level expressions are looked at, so for example the following function:

    22:
    23: def f(a):
    24:     for i in range(2):
    25:         a += i
    26:     return a
    27:

    cannot be split into multiple blocks and 
    will be in the block that contains the line number 23
    """
    def __init__(self, blocklist: BlockList = None):
        super(CodeSplitter, self).__init__()
        self.blocklist = blocklist
        if blocklist is None:
            self.blocklist = BlockList()

    # Returns the code of block between start_lineno and end_lineno
    def basic_split(self, tree: ast.Module, start_lineno: int,
                    end_lineno: int):
        node = copy.deepcopy(tree)
        new_body = []
        if isinstance(node, ast.Module):
            for elem in node.body:
                if elem.lineno >= start_lineno and elem.lineno <= end_lineno:
                    new_body.append(elem)
            node.body = new_body
        else:
            raise TypeError("Argument must be a Module node")
        return node

    # Returns a list of code blocks and
    # a flag list specifying whether each code block corresponds
    # to a block from the list or to code in between two blocks
    # Ex: if the blocklist was: ([12,23], [41,56]) and the code was 60 lines long,
    # the function would return code blocks with line numbers:
    # ([1,11], [12,23], [24,40], [41,56], [57,60]) and list of flags: [0,1,0,1,0]
    def split(self, tree: ast.Module, blocklist: BlockList = None):
        if blocklist is None:
            blocklist = self.blocklist
        if not isinstance(blocklist, BlockList):
            try:
                blocklist = BlockList(blocklist)
            except TypeError as e:
                raise TypeError(
                    "second argument must be of type BlockList or convertable to it"
                )
        blocks = blocklist.get_blocks()
        tree_splits = []
        block_flags = []
        lineno = 0
        max_lineno = self.get_max_lineno(tree)
        for block in blocks:
            if lineno < block.start:
                tree_splits.append(
                    self.basic_split(tree, lineno, block.start - 1))
                block_flags.append(0)
            tree_splits.append(self.basic_split(tree, block.start, block.end))
            block_flags.append(1)
            lineno = block.end + 1
        if lineno <= max_lineno:
            tree_splits.append(self.basic_split(tree, lineno, max_lineno))
            block_flags.append(0)
        return tree_splits, block_flags

    def add_blocks(self, *blocks: Block):
        self.blocklist.add_blocks(blocks)

    def get_blocks(self):
        return self.blocklist.get_blocks()

    # Get the maximum line number of the given code
    def get_max_lineno(self, tree: ast.AST):
        class LineCounter(ast.NodeVisitor):
            """docstring for LineCounter"""
            def __init__(self):
                super(LineCounter, self).__init__()
                self.max_lineno = -1

            def visit(self, node):
                try:
                    self.max_lineno = max(self.max_lineno, node.lineno)
                except Exception as e:
                    pass
                self.generic_visit(node)

            def count(self, tree):
                self.visit(tree)
                return self.max_lineno

        return LineCounter().count(tree)
