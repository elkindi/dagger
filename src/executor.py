from datetime import datetime
from dbObject import DbObject
from dbInterface import DbResource
from split_primitive_classes import DfPartitioner, SplitCommandParser


class Executor(object):
    """
    Executor class

    Gets all the code blocks to execute
    and the parameters for the splitting

    Executes everything and saves a log of the last execution
    """
    def __init__(self,
                 code_list=None,
                 block_flag_list=None,
                 delta_logging=False,
                 split_command=None):
        super(Executor, self).__init__()
        self.code_list = code_list
        self.block_flag_list = block_flag_list
        self.num_blocks = len(block_flag_list)
        self.num_logged_blocks = sum(block_flag_list)
        self.set_split_command(split_command)
        self.delta_logging = delta_logging
        self.log = None

    # Get the log of the last execution
    def get_log(self):
        return self.log

    # Set the code block list
    def set_code_list(self, code_list):
        self.code_list = code_list

    # Set the block flag list
    def set_block_flag_list(self, block_flag_list):
        self.block_flag_list = block_flag_list

    # Save the parameters of the split command
    def set_split_command(self, split_command):
        if split_command is None:
            self.split_params = None
        else:
            self.split_params = SplitCommandParser.parse(split_command)
            if self.split_params['block'] > self.num_logged_blocks:
                raise ValueError("Block ", self.split_params['block'],
                                 " is out of the defined block range (",
                                 self.num_logged_blocks, ")")

    # Set the delta logging parameter
    def set_delta_logging(self, delta_logging):
        self.delta_logging = delta_logging

    # Run the given code blocks using the given logging function
    def run(self, log_func):
        if self.code_list is None:
            raise ValueError("Code list is not defined")
        if self.block_flag_list is None:
            raise ValueError("Block flag list is not defined")
        db_resource = DbResource(self.delta_logging)
        with db_resource as db:
            lb_count = 0  # logged block count
            self.log = []
            if self.split_params is not None:
                globs_list = [{
                    'log_variable': log_func,
                    'log': self.log,
                    'db': db
                }]
                for i, code in enumerate(self.code_list):
                    if self.block_flag_list[i] == 1:
                        lb_count += 1
                        db.set_block_id(lb_count)
                        if lb_count == self.split_params['block']:
                            df_partitioner = DfPartitioner(self.split_params)
                            globs_list = df_partitioner.partition_from_globs(
                                *globs_list)
                    else:
                        db.reset_block_id()
                    for j, globs in enumerate(globs_list):
                        if len(globs_list) > 1:
                            db.set_split_id(j)
                        else:
                            db.reset_split_id()
                        exec(code, globs)
            else:
                globs = {'log_variable': log_func, 'log': self.log, 'db': db}
                for i, code in enumerate(self.code_list):
                    if self.block_flag_list[i] == 1:
                        lb_count += 1
                        db.set_block_id(lb_count)
                    else:
                        db.reset_block_id()
                    exec(code, globs)