import ast
import itertools
from typing import List

NodeList = List[ast.AST]


class Logger(ast.NodeTransformer):
    """
    Logger class

    Implements visit functions of the NodeTransformer 
    to add logging functions at the needed places
    """
    def __init__(self, log_function, *log_function_args):
        super(Logger, self).__init__()
        if callable(log_function):
            self.log_function = log_function.__name__
        elif isinstance(log_function, str):
            self.log_function = log_function
        self.log_function_args = log_function_args
        self.modifier_functions = set(['append', 'pop', 'sort'])

    def add_modifier_attr_fcts(self, fct_names):
        self.modifier_functions.update(fct_names)

    def get_modifier_attr_fcts(self):
        return self.modifier_functions.copy()

    # This is kinda hardcoded, but just change this
    # depending on the arguments of the logging function used
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
            elif arg == 'log':
                args.append(ast.Name(id='log', ctx=ast.Load()))
        return args

    # We don't want to handle user-defined classes yet,
    # so no generic_visit call (do not visit its children)
    def visit_ClassDef(self, node):
        return node

    # # We don't want to handle for-loops yet,
    # # so no generic_visit call (do not visit its children)
    # def visit_For(self, node):
    #     return node

    # # We don't want to handle for-loops yet,
    # # so no generic_visit call (do not visit its children)
    # def visit_While(self, node):
    #     return node

    # If the node is an expression,
    # test if it is a call to the logging function
    # If Yes, add the needed attributes to the call
    # If No, check if it is a call to one of the mafs
    def visit_Expr(self, node):
        (is_log_expr, modified_node) = self.is_log_expr(node)
        if is_log_expr:
            return modified_node
        else:
            log_nodes = self.log_expr(node)
            return [node, *log_nodes]

    # Log the assigned variable
    def visit_Assign(self, node):
        log_nodes = self.log_assign(node)
        return [node, *log_nodes]

    # Checks if the expression is a call to the logging function
    # and returns a new node with all the needed arguments if so
    def is_log_expr(self, node):
        if isinstance(node.value, ast.Call) and \
                isinstance(node.value.func, ast.Name) and \
                node.value.func.id == self.log_function and \
                len(node.value.args) > 0 and \
                isinstance(node.value.args[0], ast.Name):
            return (True, *self.log_name(node.value.args[0]))
        else:
            return (False, None)

    # Creates a node logging the variable defined by the name node
    def log_name(self, node) -> NodeList:
        log_node = ast.Expr(
            value=ast.Call(func=ast.Name(id=self.log_function, ctx=ast.Load()),
                           args=self.get_log_function_args(node),
                           keywords=[]))
        log_node = ast.copy_location(log_node, node)
        ast.fix_missing_locations(log_node)
        return [log_node]

    # Log the assignement targets
    # and checks if the assignemnt value has a call to a maf
    def log_assign(self, node) -> NodeList:
        log_nodes = []
        for target in node.targets:
            log_nodes.extend(self.log_assign_target(target))
        log_nodes.extend(self.log_assign_value(node.value))
        return log_nodes

    # Logs the assignement target
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

    # Logs the assignement value if it is a call to a maf
    def log_assign_value(self, node) -> NodeList:
        if isinstance(node, ast.Call):
            return self.log_call(node)
        else:
            return []

    # Logs the variable in the expression if it is a call to a maf
    def log_expr(self, node) -> NodeList:
        if isinstance(node.value, ast.Call):
            return self.log_call(node.value)
        else:
            return []

    # Logs the variable in the call if it is a call to a maf
    def log_call(self, node) -> NodeList:
        if isinstance(node.func, ast.Attribute):
            return self.log_call_attribute(node.func)
        else:
            return []

    # Logs the variable in the attribute call if it is a call to a maf
    def log_call_attribute(self, node) -> NodeList:
        if node.attr in self.modifier_functions and isinstance(
                node.value, ast.Name):
            return self.log_name(node.value)
        else:
            return []
