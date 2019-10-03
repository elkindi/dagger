import re
import ast
import operator as op
from copy import deepcopy
from pandas import DataFrame


class SplitCommandParser(object):
    """docstring for SplitCommandParser"""

    pattern = re.compile(
        r"SPLIT (?P<df_name>[\w]+) WHERE (?P<col_name>[\w]+) " +
        r"(?P<comp_op><|<=|=|!=|>=|>) (?P<comp_val>[\S]+) " +
        r"ON BLOCK (?P<block>[\d]+)", re.IGNORECASE)

    def __init__(self):
        super(SplitCommandParser, self).__init__()

    @classmethod
    def parse(cls, command):
        def convert(key, val):
            if key in ['df_name', 'col_name']:
                return val
            elif key == 'comp_op':
                return {
                    '<': op.lt,
                    '<=': op.le,
                    '=': op.eq,
                    '!=': op.ne,
                    '>=': op.ge,
                    '>': op.gt
                }.get(val)
            elif key in ['comp_val', 'block']:
                return ast.literal_eval(val)

        match = cls.pattern.match(command)
        if match is None:
            raise ValueError("Incorrect command")
        params = match.groupdict()
        return {k: convert(k, v) for k, v in params.items()}


class DfPartitioner(object):
    """docstring for DfPartitioner"""
    def __init__(self, params):
        super(DfPartitioner, self).__init__()
        self.params = params

    def partition(self, df: DataFrame):
        col_name = self.params['col_name']
        if col_name not in df.columns:
            print("Dataframe has no column named ", col_name)
        else:
            comp_op = self.params['comp_op']
            comp_val = self.params['comp_val']
            col_type = df.dtypes[col_name]
            mask = comp_op(df[col_name], comp_val)
            df_comp_true = df.loc[mask]
            df_comp_false = df.loc[~mask]
            return [df_comp_true, df_comp_false]
        return [df]

    def deepcopy_globs(self, globs):
        new_globs = {}
        for k, v in globs.items():
            if k == 'log':
                new_globs[k] = v
                continue
            try:
                v = deepcopy(v)
            except Exception as e:
                pass
            finally:
                new_globs[k] = v
        return new_globs

    def partition_from_globs(self, globs: dict):
        df_name = self.params['df_name']
        if df_name not in globs.keys():
            raise ValueError("Parameter ", df_name, " is not defined")
        df_val = globs.get(df_name)
        if not isinstance(df_val, DataFrame):
            raise TypeError("Parameter ", df_name, " is not a dataframe")
        df_partitions = self.partition(df_val)
        globs_partitions = []
        for i, df_partition in enumerate(df_partitions):
            globs_copy = self.deepcopy_globs(globs)
            globs_copy[df_name] = df_partition
            globs_copy['split_id'] = i
            globs_partitions.append(globs_copy)
        return globs_partitions