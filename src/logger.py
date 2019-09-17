import ast
import itertools
from typing import List
from block import Block, BlockList

NodeList = List[ast.AST]


class Logger(ast.NodeTransformer):
    """docstring for Logger"""
    def __init__(self, log_function, *log_function_args):
        super(Logger, self).__init__()
        if callable(log_function):
            self.log_function = log_function.__name__
        elif isinstance(log_function, str):
            self.log_function = log_function
        self.log_function_args = log_function_args
        self.modifier_functions = set(['append'])
        self.blockList = BlockList()

    def add_blocks(self, blocks):
        self.blockList.add_blocks(blocks)

    def get_blocks(self):
        return self.blockList

    def add_modifier_attr_fcts(self, fct_names):
        self.modifier_functions.update(fct_names)

    def get_modifier_attr_fcts(self):
        return self.modifier_functions.copy()

    def get_log_function_args(self, node):
        if not isinstance(node, ast.Name):
            raise ValueError("Node must be of type ast.Name")
        args = []
        for arg in self.log_function_args:
            if arg == 'val':
                args.append(ast.Name(id=node.id, ctx=ast.Load()))
            elif arg == 'name':
                args.append(ast.Str(s=node.id))
            elif arg == 'lineno':
                args.append(ast.Num(node.lineno))
            elif arg == 'db':
                args.append(ast.Name(id='db', ctx=ast.Load()))
        return args

    def visit_ClassDef(self, node):
        return node

    def visit_For(self, node):
        return node

    def visit_Expr(self, node):
        (is_log_expr, modified_node) = self.is_log_expr(node)
        if is_log_expr:
            return modified_node
        elif node.lineno in self.blockList:
            log_nodes = self.log_expr(node)
            return [node, *log_nodes]
        else:
            return node

    def visit_Assign(self, node):
        if node.lineno in self.blockList:
            log_nodes = self.log_assign(node)
            return [node, *log_nodes]
        else:
            return node

    def is_log_expr(self, node):
        if isinstance(node.value, ast.Call) and \
                isinstance(node.value.func, ast.Name) and \
                node.value.func.id == self.log_function and \
                len(node.value.args) > 0 and \
                isinstance(node.value.args[0], ast.Name):
            return (True, *self.log_name(node.value.args[0]))
        else:
            return (False, None)

    def log_name(self, node) -> NodeList:
        log_node = ast.Expr(
            value=ast.Call(func=ast.Name(id=self.log_function, ctx=ast.Load()),
                           args=self.get_log_function_args(node),
                           keywords=[]))
        log_node = ast.copy_location(log_node, node)
        ast.fix_missing_locations(log_node)
        return [log_node]

    def log_assign(self, node) -> NodeList:
        log_nodes = []
        for target in node.targets:
            log_nodes.extend(self.log_assign_target(target))
        log_nodes.extend(self.log_assign_value(node.value))
        return log_nodes

    def log_assign_target(self, node) -> NodeList:
        if isinstance(node, ast.Name):
            return self.log_name(node)
        elif isinstance(node, ast.Tuple) or isinstance(node, ast.List):
            log_nodes = []
            for elt in node.elts:
                log_nodes.extend(self.log_assign_target(elt))
            return log_nodes
        elif isinstance(node, ast.Subscript) or isinstance(
                node, ast.Starred) or isinstance(node, ast.Attribute):
            return self.log_assign_target(node.value)
        else:
            return []

    def log_assign_value(self, node) -> NodeList:
        if isinstance(node, ast.Call):
            return self.log_call_func(node.func)
        else:
            return []

    def log_expr(self, node) -> NodeList:
        if isinstance(node.value, ast.Call):
            return self.log_call(node.value)
        else:
            return []

    def log_call(self, node) -> NodeList:
        if isinstance(node.func, ast.Attribute):
            return self.log_call_attribute(node.func)
        else:
            return []

    def log_call_attribute(self, node) -> NodeList:
        if node.attr in self.modifier_functions and isinstance(
                node.value, ast.Name):
            return self.log_name(node.value)
        else:
            return []
