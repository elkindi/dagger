import ast

class Analyzer(ast.NodeVisitor):
    """docstring for Analyzer"""
    def __init__(self, tree):
        super(Analyzer, self).__init__()
        self.tree = tree
        